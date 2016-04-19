module Fbls

import Base: AbstractIOBuffer, ==, convert, delete!, done, empty!, eof, get, 
gethash, index, isempty, haskey, length, next, position, push!, seekend, 
setindex!, start
import Base.Dates: DateTime, datetime2unix, now, unix2datetime
import Base.Random: UUID, uuid4

typealias IOBuf AbstractIOBuffer
typealias Offs Int64
typealias RecId UUID
typealias Revision Int64
typealias Str AbstractString
typealias Vec{T} Array{T, 1}

abstract Err <: Exception
type RecNotFound <: Err end

TempBuf() = IOBuffer()

typealias EvtSub Function
typealias EvtSubs Vec{EvtSub}

immutable Evt{ArgsT}
    id::UUID
    subs::EvtSubs

    Evt() = new(uuid4(), EvtSubs())
end

sub!(evt::Evt, sub::EvtSub) = push!(evt.subs, sub)

unsub!(evt::Evt, sub::EvtSub) = delete!(evt.subs, sub)

push!{ArgsT}(evt::Evt{ArgsT}, args::ArgsT) =
    for sub in evt.subs sub(args...) end

abstract AnyCol

typealias FldId UUID

immutable Fld
    id::FldId

    Fld() = new(uuid4())
end

typealias Rec Dict{Fld, Any}

abstract Col{ValT} <: AnyCol

RecOf(vals::Pair...) = begin
    r = Rec()
    for (c, v) in vals r[c] = v end
    return r
end

delete!{ValT}(r::Rec, c::Col{ValT}) = delete!(r, Fld(c))

haskey{ValT}(r::Rec, c::Col{ValT}) = haskey(r, Fld(c))

getindex{ValT}(r::Rec, c::Col{ValT}) = getindex(r, Fld(c))

get{ValT}(r::Rec, c::Col{ValT}, default) = get(r, Fld(c), default)

setindex!{ValT}(r::Rec, v::ValT, c::Col{ValT}) = setindex!(r, v, Fld(c))

immutable BasicCol{ValT} <: Col{ValT}
    name::Symbol
    fld::Fld

    BasicCol(n::Symbol, f::Fld) = new(n, f)
end

Col(ValT, n::Symbol, f::Fld) = BasicCol{ValT}(n, f)
Col(ValT, n::Symbol) = Col(ValT, n, Fld())

alias{ValT}(col::Col{ValT}, n::Symbol) = Col(ValT, n, Fld(col))

convert{ValT}(::Type{Fld}, col::Col{ValT}) = BasicCol{ValT}(col).fld

defname{ValT}(col::Col{ValT}) = BasicCol{ValT}(col).name

immutable TempCol{ValT} <: Col{ValT}
    wrapped::Col{ValT}

    TempCol(col::Col{ValT}) = new(col)
end

Temp{ValT}(col::Col{ValT}) = TempCol{ValT}(col)

convert{ValT}(::Type{BasicCol{ValT}}, col::TempCol{ValT}) = 
    BasicCol{ValT}(col.wrapped)

istemp(::AnyCol) = false
istemp{ValT}(::TempCol{ValT}) = true

RecId() = uuid4()

idCol = Col(RecId, :fbls_id)
isdelCol = Col(Bool, :fbls_isdel)

recid(r::Rec) = r[idCol]

==(l::Rec, r::Rec) = begin
    lid = get(l, idCol, Void)
    rid = get(r, idCol, Void)

    return lid != Void && rid != Void && lid == rid
end

hash(r::Rec) = hash(recid(r))

pushdep!(def, dep) = begin
    sub!(ondelete(def), (id) -> delete!(dep, id))
    sub!(onload(def), (rec) -> load!(dep, rec))
    sub!(onupsert(def), (rec) -> upsert!(dep, rec))
end

abstract Revix{ValT}

typealias RevixRecs{ValT} Dict{RecId, ValT}

immutable BasicRevix{ValT} <: Revix{ValT}
    name::Symbol
    col::Col{ValT}
    recs::RevixRecs{ValT}
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicRevix(n::Symbol, c::Col{ValT}) = new(n, c, 
                                              RevixRecs{ValT}(), 
                                              Evt{Tuple{RecId}}(),
                                              Evt{Tuple{Rec}}(),
                                              Evt{Tuple{Rec}}())
end

Revix{ValT}(n::Symbol, c::Col{ValT}) = BasicRevix{ValT}(n, c)

defname(rx::Revix) = BasicRevix(rx).name

empty!(rx::Revix) = empty!(BasicRevix(rx).recs)

get{ValT}(rx::Revix{ValT}, id::RecId) = BasicRevix{ValT}(rx).recs[id]

haskey(rx::Revix, id::RecId) = haskey(BasicRevix(rx).recs, id)

delete!(rx::Revix, id::RecId) = begin
    brx = BasicRevix(rx)
    if !haskey(rx, id) throw(RecNotFound()) end
    push!(brx.ondelete, (id,))
    delete!(brx.recs, id)
end

upsert!(rx::Revix, rec::Rec) = begin
    brx = BasicRevix(rx)
    push!(brx.onupsert, (rec,))
    brx.recs[recid(rec)] = rec[brx.col]
    return rec
end

load!(rx::Revix, rec::Rec) = begin
    brx = BasicRevix(rx)
    push!(brx.onload, (rec,))
    brx.recs[recid(rec)] = rec[brx.col]
    return rec
end

ondelete(rx::Revix) = BasicRevix(rx).ondelete
onupsert(rx::Revix) = BasicRevix(rx).onupsert 
onload(rx::Revix) = BasicRevix(rx).onload

done(rx::Revix, i) = done(BasicRevix(rx).recs, i)
isempty(rx::Revix) = isempty(BasicRevix(rx).recs)
length(rx::Revix) = length(BasicRevix(rx).recs)
next(rx::Revix, i) = next(BasicRevix(rx).recs, i)
start(rx::Revix) = start(BasicRevix(rx).recs)

dump(rx::Revix, out::IOBuf) = begin
    brx = BasicRevix(rx)

    for (id, val) in brx.recs
        write(out, id.value)
        writeval(brx.col, val, out)
    end
end

load!(rx::Revix, in::IOBuf) = begin
    brx = BasicRevix(rx)

    while !eof(in)
        id = readval(UUID, -1, in)

        s = readsize(in)

        if s == -1
            delete!(brx.recs, id)
        else
            brx.recs[id] = readval(brx.col, s, in)
        end
    end
end

immutable IORevix{ValT} <: Revix{ValT}
    wrapped::Revix{ValT}
    buf::IOBuf
    
    IORevix(rx::Revix{ValT}, buf::IOBuf) = new(rx, buf)
end

IO{ValT}(rx::Revix{ValT}, buf::IOBuf) =
    IORevix{ValT}(rx, buf)

convert(::Type{BasicRevix}, rx::IORevix) = BasicRevix(rx.wrapped)

convert{ValT}(::Type{BasicRevix{ValT}}, rx::IORevix{ValT}) = 
    BasicRevix{ValT}(rx.wrapped)

delete!(rx::IORevix, id::RecId) = begin
    delete!(rx.wrapped, id)
    seekend(rx.buf)
    write(rx.buf, id.value)
    write(rx.buf, ValSize(-1)) 
end

upsert!(rx::IORevix, rec::Rec) = begin
    upsert!(rx.wrapped, rec)
    seekend(rx.buf)
    write(rx.buf, recid(rec).value)
    col = BasicRevix(rx).col
    writeval(col, rec[col], rx.buf)
    return rec
end

typealias TblCols Dict{Symbol, AnyCol}
typealias TblRecs Dict{RecId, Rec} 

abstract Tbl

immutable BasicTbl <: Tbl
    name::Symbol
    cols::TblCols
    recs::TblRecs
    upsertedatCol::Col{DateTime}
    revisionCol::Col{Int64}
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicTbl(n::Symbol) = begin
        t = new(n, 
                TblCols(),
                TblRecs(), 
                Col(DateTime, symbol("($n)_upsertedat")), 
                Col(Revision, symbol("$(n)_revision")),
                Evt{Tuple{RecId}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}())
        
        pushcol!(t, idCol, t.upsertedatCol, t.revisionCol)
        return t
    end
end

Tbl(n::Symbol) = BasicTbl(n)

cols(tbl::Tbl) = values(BasicTbl(tbl).cols)

defname(tbl::Tbl) = BasicTbl(tbl).name

empty!(tbl::Tbl) = empty!(BasicTbl(tbl).recs)

findcol(tbl::Tbl, n::Symbol) = begin
    bt = BasicTbl(tbl)
    return if haskey(bt.cols, n) 
        Nullable{AnyCol}(bt.cols[n]) 
    else 
        Nullable{AnyCol}() 
    end
end

haskey(tbl::Tbl, id::RecId) = haskey(BasicTbl(tbl).recs, id)

upsertedat(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).upsertedatCol]

revision(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).revisionCol]

delete!(tbl::Tbl, id::RecId) = begin
    bt = BasicTbl(tbl)
    if !haskey(tbl, id) throw(RecNotFound()) end
    push!(bt.ondelete, (id,))
    delete!(bt.recs, id)
end

get(tbl::Tbl, id::RecId) = BasicTbl(tbl).recs[id]

initrec!(rec) = begin
    if !haskey(rec, idCol)
        rec[idCol] = RecId()
    end

    return rec
end

initrec!(tbl::Tbl, rec::Rec) = begin
    initrec!(rec)
    bt = BasicTbl(tbl)

    if !haskey(rec, bt.revisionCol)
        rec[bt.revisionCol] = 1
    end

    return rec
end

upsert!(tbl::Tbl, rec::Rec) = begin
    bt = BasicTbl(tbl)
    id = get(rec, idCol, Void)
    rec[bt.upsertedatCol] = now()

    prev = if id != Void && haskey(bt.recs, id) 
        rec[bt.revisionCol] += 1
        push!(bt.onupsert, (rec,))
        bt.recs[id]
    else 
        initrec!(bt, rec)
        id = recid(rec)
        push!(bt.onupsert, (rec,))
        bt.recs[id] = Rec() 
    end

    for c in values(bt.cols)
        if haskey(rec, c)
            f = Fld(c)
            prev[f] = rec[f]
        end
    end
    
    return rec
end

isdirty(tbl::Tbl, rec::Rec, cols::AnyCol...) = begin
    bt = BasicTbl(tbl)
    if !haskey(rec, idCol) return true end
    rid = recid(rec)
    if !haskey(bt.recs, rid) return true end
    trec = bt.recs[rid]
    if isempty(cols) cols = bt.cols end
    return any(map((c) -> rec[c] != trec[c], cols))  
end

load!(tbl::Tbl, rec::Rec) = begin
    bt = BasicTbl(tbl)

    if haskey(rec, isdelCol)
        id = recid(rec)
        push!(bt.ondelete, (id,))
        delete!(bt.recs, id)
    else
        push!(bt.onload, (rec,))
        bt.recs[recid(rec)] = rec
    end

    return rec
end

ondelete(tbl::Tbl) = BasicTbl(tbl).ondelete 
onupsert(tbl::Tbl) = BasicTbl(tbl).onupsert 
onload(tbl::Tbl) = BasicTbl(tbl).onload

pushcol!(tbl::Tbl, cols::AnyCol...) = begin
    bt = BasicTbl(tbl)
    for c in cols bt.cols[defname(c)] = c end
end

done(tbl::Tbl, i) = done(values(BasicTbl(tbl).recs), i)
isempty(tbl::Tbl) = isempty(values(BasicTbl(tbl).recs))
length(tbl::Tbl) = length(values(BasicTbl(tbl).recs))
next(tbl::Tbl, i) = next(values(BasicTbl(tbl).recs), i)
start(tbl::Tbl) = start(values(BasicTbl(tbl).recs))

immutable RecCol <: Col{Rec}
    wrapped::Col{Rec}
    tbl::Tbl

    RecCol(col::Col{Rec}, tbl::Tbl) = new(col, tbl)
end

RecCol(n::Symbol, tbl::Tbl) = RecCol(Col(Rec, n), tbl)

convert(::Type{BasicCol{Rec}}, col::RecCol) = BasicCol{Rec}(col.wrapped)

immutable RefCol <: Col{RecId}
    wrapped::Col{RecId}
    tbl::Tbl

    RefCol(col::Col{RecId}, tbl::Tbl) = new(col, tbl)
end

RefCol(n::Symbol, tbl::Tbl) = RefCol(Col(RecId, n), tbl)

convert(::Type{BasicCol{RecId}}, col::RefCol) = BasicCol{RecId}(col.wrapped)

getref(col::RefCol, rec::Rec) = get(col.tbl, rec[col])

typealias RecSize Int16
typealias ColSize Int8
typealias ValSize Int64

readsize(in::IOBuf) = read(in, ValSize)

writesize{ValT}(v::ValT, out::IOBuf) = write(out, ValSize(sizeof(v)))

readstr{LenT}(::Type{LenT}, in::IOBuf) = begin
    pos = position(in)
    len = read(in, LenT)

    return utf8(read(in, UInt8, len))
end

readval{ValT}(::Type{ValT}, s::ValSize, in::IOBuf) = read(in, ValT) 

readval(t::Type{DateTime}, s::ValSize, in::IOBuf) =
    unix2datetime(read(in, Float64))

readval(t::Type{Rec}, s::ValSize, in::IOBuf) = readval(RecId, s, in)

readval(t::Type{Str}, s::ValSize, in::IOBuf) = utf8(read(in, UInt8, s))

readval(t::Type{UUID}, s::ValSize, in::IOBuf) = RecId(read(in, UInt128))

readval{ValT}(col::Col{ValT}, s::ValSize, in::IOBuf) = readval(ValT, s, in)

readval(col::RecCol, s::ValSize, in::IOBuf) = get(col.tbl, readval(Rec, s, in))

readrec(tbl::Tbl, in::IOBuf) = begin
    len = read(in, RecSize)
    rec = Rec()
    
    for i = 1:len
        n = readstr(ColSize, in)
        s = readsize(in)
        c = findcol(tbl, symbol(n))
        
        if isnull(c)
            skip(in, s)
        else
            c = get(c)
            rec[Fld(c)] = readval(c, s, in)
        end
    end
    
    return rec
end

writestr{LenT}(::Type{LenT}, val::Str, out::IOBuf) = begin
    bs = bytestring(val)
    len = length(bs)
    write(out, LenT(len))
    write(out, bytestring(val))
end

writeval(val::DateTime, out::IOBuf) = writeval(datetime2unix(val), out)

writeval(val::Rec, out::IOBuf) = writeval(recid(val), out)

writeval(val::Str, out::IOBuf) = begin
    writesize(val, out)
    write(out, bytestring(val))
end

writeval(val::UUID, out::IOBuf) = writeval(val.value, out)

writeval(val, out::IOBuf) = begin
    writesize(val, out)
    write(out, val)
end

writeval{ValT}(col::Col{ValT}, val::ValT, out::IOBuf) = writeval(val, out)

recsize(tbl::Tbl, rec::Rec) = 
    RecSize(count((c) -> !istemp(c) && haskey(rec, Fld(c)), cols(tbl)))


writerec(tbl::Tbl, rec::Rec, out::IOBuf) = begin
    write(out, recsize(tbl, rec))

    for c in cols(tbl)
        f = Fld(c)

        if !istemp(c) && haskey(rec, f)
            writestr(ColSize, string(defname(c)), out)
            writeval(c, rec[f], out)
        end
    end
end

immutable IOTbl <: Tbl
    wrapped::Tbl
    buf::IOBuf
    offsCol::Col{Offs}
    prevoffsCol::Col{Offs}
    
    IOTbl(tbl::Tbl, buf::IOBuf, offsCol::Col{Offs}) = begin
        t = new(tbl, buf, offsCol, 
                Col(Offs, symbol("$(defname(tbl))_prevoffs")))

        pushcol!(tbl, isdelCol, t.offsCol, t.prevoffsCol)

        return t
    end
end

IO(tbl::Tbl, buf::IOBuf; offsCol = Col(Offs, symbol("$(defname(tbl))_offs"))) = 
    IOTbl(tbl, buf, offsCol)

cols(tbl::IOTbl) = cols(tbl.wrapped)

convert(::Type{BasicTbl}, tbl::IOTbl) = BasicTbl(tbl.wrapped)

writerec(tbl::Tbl, rec::Rec) = begin
    iot = IOTbl(tbl)
    seekend(iot.buf)
    writerec(iot, rec, iot.buf)
end

delete!(tbl::IOTbl, id::RecId) = begin
    delete!(tbl.wrapped, id)
    writerec(tbl, RecOf(idCol => id, isdelCol => true))
end

get(tbl::IOTbl, idx::Revix{Offs}, id::RecId) = begin
    if haskey(tbl.wrapped, id)
        return get(tbl.wrapped, id)
    elseif haskey(idx, id)
        seek(tbl.buf, get(idx, id))
        return load!(tbl, readrec(tbl, tbl.buf))
    end

    throw(RecNotFound())
end

upsert!(tbl::IOTbl, rec::Rec) = begin
    if !haskey(rec, tbl.offsCol)
        rec[tbl.offsCol] = -1
    end

    rec[tbl.prevoffsCol] = rec[tbl.offsCol]
    rec[tbl.offsCol] = position(tbl.buf)
    res = upsert!(tbl.wrapped, rec)
    writerec(tbl, rec)

    return res
end

offs(rec::Rec, tbl::Tbl) = rec[IOTbl(tbl).offsCol]
prevoffs(rec::Rec, tbl::Tbl) = rec[IOTbl(tbl).prevoffsCol]

dump(tbl::Tbl, out::IOBuf) = begin
    for r in tbl
        writerec(tbl, r, out)
    end
end

load!(tbl::Tbl, in::IOBuf) = begin
    while !eof(in)
        load!(tbl, readrec(tbl, in))
    end
end

testTblBasics() = begin
    t = Tbl(:foos)
    @assert isempty(t)
    @assert length(t) == 0

    r = upsert!(t, Rec())
    @assert !isempty(t)
    @assert length(t) == 1

    rid = recid(r)
    @assert revision(r, t) == 1
    rupsertedat = upsertedat(r, t)

    upsert!(t, r)
    @assert !isempty(t)
    @assert length(t) == 1
    @assert recid(r) == rid
    @assert upsertedat(r, t) >= rupsertedat
    @assert revision(r, t) == 2
end

testGet() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    gr = get(t, recid(r))

    @assert gr == r
end

testIOTblBasics() = begin
    t = IO(Tbl(:foos), TempBuf())
    r = upsert!(t, Rec())
    @assert revision(r, t) == 1
    
    roffs = offs(r, t)
    @assert roffs > -1
    @assert prevoffs(r, t) == -1

    upsert!(t, r)
    @assert offs(r, t) > roffs
    @assert prevoffs(r, t) == roffs
end

testRecBasics() = begin
    r = Rec()
    c = Col(Str, :foo)
    @assert !haskey(r, c)
    
    r[c] = "abc"
    r[c] = "def"
    @assert r[c] == "def"
    @assert length(r) == 1
    
    delete!(r, c)
    @assert length(r) == 0
end

testTempCol() = begin
    t = Tbl(:foo)
    c = Col(Str, :bar)
    tc = Temp(c)
    pushcol!(t, tc)

    r = Rec()
    r[tc] = "abc"
    @assert r[c] == "abc"

    buf = TempBuf()
    writerec(t, r, buf)
    seekstart(buf)
    rr = readrec(t, buf)
    @assert !haskey(rr, tc)
end

testRecCol() = begin
    t = Tbl(:foos)
    c = RecCol(:foo, t)
    foo = upsert!(t, Rec())
    
    r = Rec()
    r[c] = foo
    @assert r[c] == foo
end

testRefCol() = begin
    t = Tbl(:foos)
    c = RefCol(:foo, t)
    foo = upsert!(t, Rec())
    
    r = Rec()
    r[c] = recid(foo)
    @assert getref(c, r) == foo
end

testReadWriteRec() = begin
    t = Tbl(:foos)
    c = Col(Str, :bar)
    pushcol!(t, c)

    r = Rec()
    r[c] = "abc"
    upsert!(t, r)
    
    buf = TempBuf()
    writerec(t, r, buf)
    seekstart(buf)
    rr = readrec(t, buf)
    @assert rr[c] == r[c]
end

testReadWriteRecCol() = begin
    foos = Tbl(:foos)
    bars = Tbl(:bars)
    barFoo = RecCol(:foo, foos)
    pushcol!(bars, barFoo)

    foo = upsert!(foos, Rec())
    
    bar = Rec()
    bar[barFoo] = foo
    
    buf = TempBuf()
    writerec(bars, bar, buf)
    seekstart(buf)
    rbar = readrec(bars, buf)
    @assert rbar[barFoo] == foo
end

testAliasCol() = begin
    foo = Col(Str, :foo)
    bar = alias(foo, :bar)
    
    r = Rec()
    r[foo] = "abc"
    @assert r[bar] == "abc"
end

testEmptyTbl() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    empty!(t)

    @assert !haskey(t, recid(r))
end

testDumpLoad() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    buf = TempBuf()
    dump(t, buf)
    empty!(t)
    seekstart(buf)
    load!(t, buf)

    @assert get(t, recid(r)) == r
end

testDelete() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    id = recid(r)
    delete!(t, id)

    @assert !haskey(t, id)
end

testIODelete() = begin
    buf = TempBuf()
    t = IO(Tbl(:foos), buf)
    r = upsert!(t, Rec())
    id = recid(r)
    delete!(t, id)
    empty!(t)
    seekstart(buf)
    load!(t, buf)

    @assert !haskey(t, id)
end

testIsdirty() = begin
    t = Tbl(:foobars)
    foo = Col(Str, :foo)
    bar = Col(Str, :bar)
    pushcol!(t, foo, bar)

    r = RecOf(foo => "abc", bar => "def")
    @assert isdirty(t, r)
    @assert isdirty(t, r, foo, bar)

    upsert!(t, r)
    @assert !isdirty(t, r, foo, bar)

    r[foo] = "ghi"
    @assert !isdirty(t, r, bar)
    @assert isdirty(t, r, foo)

    upsert!(t, r)
    @assert !isdirty(t, r, foo, bar)    
end

testOnupsert() = begin
    t = Tbl(:foos)
    rec = Rec()
    wascalled = false
    sub!(onupsert(t), (r) -> (@assert r == rec; wascalled = true))
    upsert!(t, rec)
    @assert wascalled
end

testRevix() = begin
    buf = TempBuf()
    tbl = IO(Tbl(:foo), buf)
    rx = Revix(:foo_offs, tbl.offsCol) 
    @assert isempty(rx)
    @assert length(rx) == 0

    pushdep!(tbl, rx)
    rec = upsert!(tbl, Rec())
    id = recid(rec)
    @assert haskey(rx, id)
    @assert !isempty(rx)
    @assert length(rx) == 1
    @assert get(rx, id) == offs(rec, tbl)
    
    empty!(tbl)
    @assert get(tbl, rx, id) == rec

    delete!(tbl, id)
    @assert isempty(rx)
    @assert length(rx) == 0
    @assert !haskey(rx, id)
end

testDumpLoadRevix() = begin
    c = Col(Str, :foo)
    rx = Revix(:foos, c)
    r = upsert!(rx, initrec!(RecOf(c => "abc")))
    
    buf = TempBuf()
    dump(rx, buf)
    empty!(rx)
    seekstart(buf)
    load!(rx, buf)

    @assert get(rx, recid(r)) == "abc"
end

testIORevix() = begin
    c = Col(Str, :foo)
    buf = TempBuf()
    rx = IO(Revix(:foos, c), buf)
    rec = upsert!(rx, initrec!(RecOf(c => "abc")))
    id = recid(rec)

    empty!(rx)
    seekstart(buf)
    load!(rx, buf)    
    @assert get(rx, id) == "abc"

    delete!(rx, id)
    seekstart(buf)
    load!(rx, buf)
    @assert !haskey(rx, id)
end

testAll() = begin
    testTblBasics()
    testRecBasics()
    testIOTblBasics()
    testGet()
    testTempCol()
    testRecCol()
    testRefCol()
    testReadWriteRec()
    testReadWriteRecCol()
    testAliasCol()
    testEmptyTbl()
    testDumpLoad()
    testDelete()
    testIODelete()
    testIsdirty()
    testOnupsert()
    testRevix()
    testDumpLoadRevix()
    testIORevix()
end

end
