module Fbls

import Base: AbstractIOBuffer, ==, convert, delete!, done, empty!, eof, get, getindex, isempty, haskey, length, next, position, seekend, setindex!, start
import Base.Dates: DateTime, datetime2unix, now, unix2datetime
import Base.Random: UUID, uuid4

typealias Offs Int64
typealias RecId UUID
typealias Str AbstractString
typealias IOBuf AbstractIOBuffer
typealias Vec{T} Array{T, 1}

abstract Err <: Exception
type RecNotFound <: Err end

TempBuf() = IOBuffer()

abstract Cx

abstract AnyEvt

immutable Evt{ArgsT} <: AnyEvt
    id::UUID

    Evt() = new(uuid4())
end

typealias EvtSub Function
typealias EvtSubs Dict{AnyEvt, Vec{EvtSub}}
typealias EvtQueue Vec{Vec{Any}}
typealias EvtQueues Dict{AnyEvt, EvtQueue}

immutable BasicCx <: Cx
    evtsubs::EvtSubs
    evtqueues::EvtQueues

    BasicCx() = new(EvtSubs(), EvtQueues())
end

Cx() = BasicCx()

sub!(evt::AnyEvt, sub::EvtSub, cx::Cx) = begin
    bcx = BasicCx(cx)

    subs = if haskey(bcx.evtsubs, evt)
        bcx.evtsubs[evt]
    else
        bcx.evtsubs[evt] = Vec{EvtSub}()
    end

    push!(subs, sub)
end

unsub!(evt::AnyEvt, sub::EvtSub, cx::Cx) = 
    delete!(BasicCx(cx).subs[evt], sub)

pushevt!{ArgsT}(evt::Evt{ArgsT}, args::ArgsT, cx::Cx) = begin
    bcx = BasicCx(cx)
    
    if haskey(bcx.evtsubs, evt) && !isempty(bcx.evtsubs[evt])
        q = if haskey(bcx.evtqueues, evt)
            bcx.evtqueues[evt]
        else
            bcx.evtqueues[evt] = EvtQueue()
        end
        
        push!(q, [Any(a) for a=args])
    end
end

doevts!(cx::Cx) = begin
    bcx = BasicCx(cx)
    res = 0

    for (evt, q) in bcx.evtqueues
        if !isempty(q)
            subs = bcx.evtsubs[evt]

            for args in q, sub in subs
                sub(args...)
                res += 1
            end

            empty!(q)
        end
    end

    return res
end

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

getindex{ValT}(r::Rec, c::Col{ValT}) = if haskey(r, c) getindex(r, Fld(c)) else Void end

setindex!{ValT}(r::Rec, v::ValT, c::Col{ValT}) = setindex!(r, v, Fld(c))

immutable BasicCol{ValT} <: Col{ValT}
    name::Symbol
    fld::Fld

    BasicCol(n::Symbol, f::Fld) = new(n, f)
    BasicCol(n::Symbol) = new(n, Fld())
end

alias{ValT}(col::Col{ValT}, n::Symbol) = BasicCol{ValT}(n, Fld(col))

convert{ValT}(::Type{Fld}, col::BasicCol{ValT}) = col.fld

defname{ValT}(col::Col{ValT}) = BasicCol{ValT}(col).name

immutable TempCol{ValT} <: Col{ValT}
    wrapped::Col{ValT}

    TempCol(col::Col{ValT}) = new(col)
end

Temp{ValT}(col::Col{ValT}) = TempCol{ValT}(col)

convert{ValT}(::Type{BasicCol{ValT}}, col::TempCol{ValT}) = 
    BasicCol{ValT}(col.wrapped)

convert{ValT}(::Type{Fld}, col::TempCol{ValT}) = Fld(col.wrapped)

istemp(::AnyCol) = false
istemp{ValT}(::TempCol{ValT}) = true

RecId() = uuid4()

idCol = BasicCol{RecId}(:fbls_id)
isdelCol = BasicCol{Bool}(:fbls_isdel)

recid(r::Rec) = r[idCol]

==(l::Rec, r::Rec) = begin
    lid = recid(l)
    rid = recid(r)

    return lid != Void && rid != Void && lid == rid
end

pushdep!(def, dep, cx::Cx) = begin
    ondelete!(def, (rec) -> delete!(dep, rec, cx), cx)
    oninsert!(def, (rec) -> insert!(dep, rec, cx), cx)
    onload!(def, (rec) -> load!(dep, rec, cx), cx)
end

abstract Revix{ValT}

typealias RevixRecs{ValT} Dict{RecId, ValT}

immutable BasicRevix{ValT} <: Revix{ValT}
    name::Symbol
    col::Col{ValT}
    recs::RevixRecs{ValT}
    ondelete::Evt{Tuple{Rec}} 
    oninsert::Evt{Tuple{Rec}} 
    onload::Evt{Tuple{Rec}} 

    BasicRevix(n::Symbol, c::Col{ValT}) = new(n, c, 
                                              RevixRecs{ValT}(), 
                                              Evt{Tuple{Rec}}(),
                                              Evt{Tuple{Rec}}(),
                                              Evt{Tuple{Rec}}())
end

Revix{ValT}(n::Symbol, c::Col{ValT}) = BasicRevix{ValT}(n, c)

defname(rx::Revix) = BasicRevix(rx).name

empty!(rx::Revix) = empty!(BasicRevix(rx).recs)

get{ValT}(rx::Revix{ValT}, id::RecId, cx::Cx) = BasicRevix{ValT}(rx).recs[id]

haskey(rx::Revix, id::RecId, cx::Cx) = haskey(BasicRevix(rx).recs, id)

delete!(rx::Revix, rec::Rec, cx::Cx) = begin
    brx = BasicRevix(rx)
    delete!(brx.recs, recid(rec))
    pushevt!(brx.ondelete, (rec,), cx)
    return rec
end

insert!(rx::Revix, rec::Rec, cx::Cx) = begin
    brx = BasicRevix(rx)
    brx.recs[recid(rec)] = rec[brx.col]
    pushevt!(brx.oninsert, (rec,), cx)
    return rec
end

load!(rx::Revix, rec::Rec, cx::Cx) = begin
    brx = BasicRevix(rx)
    brx.recs[recid(rec)] = rec[brx.col]
    pushevt!(brx.onload, (rec,), cx)
    return rec
end

ondelete!(rx::Revix, sub::EvtSub, cx::Cx) = 
    sub!(BasicRevix(rx).ondelete, sub, cx)
oninsert!(rx::Revix, sub::EvtSub, cx::Cx) = 
    sub!(BasicRevix(rx).oninsert, sub, cx) 
onload!(rx::Revix, sub::EvtSub, cx::Cx) = 
    sub!(BasicRevix(rx).onload, sub, cx)

done(rx::Revix, i) = done(BasicRevix(rx).recs, i)
isempty(rx::Revix) = isempty(BasicRevix(rx).recs)
length(rx::Revix) = length(BasicRevix(rx).recs)
next(rx::Revix, i) = next(BasicRevix(rx).recs, i)
start(rx::Revix) = start(BasicRevix(rx).recs)

dump(rx::Revix, out::IOBuf) = begin
    brx = BasicRevix(rx)

    for (id, val) in brx.recs
        writeval(id, out)
        write(out, ValSize(sizeofval(val)))
        writeval(brx.col, val, out)
    end
end

load!(rx::Revix, in::IOBuf, cx::Cx) = begin
    brx = BasicRevix(rx)

    while !eof(in)
        id = readval(UUID, -1, in)
        s = read(in, ValSize)

        if s == -1
            delete!(brx.recs, id)
        else
            brx.recs[id] = readval(brx.col, s, in, cx)
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

delete!(rx::IORevix, rec::Rec, cx::Cx) = begin
    delete!(rx.wrapped, rec, cx)
    seekend(rx.buf)
    writeval(recid(rec), rx.buf)
    write(rx.buf, ValSize(-1)) 
    return rec
end

insert!(rx::IORevix, rec::Rec, cx::Cx) = begin
    insert!(rx.wrapped, rec, cx)
    seekend(rx.buf)
    writeval(recid(rec), rx.buf)
    col = BasicRevix(rx).col
    v = rec[col]
    s = ValSize(sizeofval(v))
    write(rx.buf, s)
    writeval(col, v, rx.buf)
    return rec
end

typealias TblCols Dict{Symbol, AnyCol}
typealias TblRecs Dict{RecId, Rec} 

abstract Tbl

immutable BasicTbl <: Tbl
    name::Symbol
    cols::TblCols
    recs::TblRecs
    insertedatCol::Col{DateTime}
    revCol::Col{Int64}
    ondelete::Evt{Tuple{Rec}} 
    oninsert::Evt{Tuple{Rec}} 
    onload::Evt{Tuple{Rec}} 
    onuprec::Evt{Tuple{Rec, Rec}} 

    BasicTbl(n::Symbol) = begin
        t = new(n, 
                TblCols(),
                TblRecs(), 
                BasicCol{DateTime}(symbol("($n)_insertedat")), 
                BasicCol{Int64}(symbol("$(n)_revision")),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec, Rec}}())
        
        pushcol!(t, idCol, t.insertedatCol, t.revCol)
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

haskey(tbl::Tbl, id::RecId, cx::Cx) = haskey(BasicTbl(tbl).recs, id)

insertedat(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).insertedatCol]

revision(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).revCol]

delete!(tbl::Tbl, id::RecId, cx::Cx) = begin
    bt = BasicTbl(tbl)
    pushevt!(bt.ondelete, (bt.recs[id],), cx)
    delete!(bt.recs, id) 
end

delete!(tbl::Tbl, rec::Rec, cx::Cx) = delete!(tbl, recid(rec), cx)

get(tbl::Tbl, id::RecId, cx::Cx) = BasicTbl(tbl).recs[id]

initrec!(rec) = begin
    if !haskey(rec, idCol)
        rec[idCol] = RecId()
    end

    return rec
end

initrec!(tbl::Tbl, rec::Rec) = begin
    initrec!(rec)
    bt = BasicTbl(tbl)

    if !haskey(rec, bt.revCol)
        rec[bt.revCol] = 1
    end

    return rec
end

insert!(tbl::Tbl, rec::Rec, cx::Cx) = begin
    bt = BasicTbl(tbl)
    id = recid(rec)
    rec[bt.insertedatCol] = now()
    pushevt!(bt.oninsert, (rec,), cx)

    prev = if id != Void && haskey(bt.recs, id) 
        rec[bt.revCol] += 1
        pushevt!(bt.onuprec, (copy(bt.recs[id]), rec), cx)
        bt.recs[id]
    else 
        initrec!(bt, rec)
        id = recid(rec)
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
    rid = recid(rec)
    if !haskey(bt.recs, rid) return true end
    trec = bt.recs[rid]
    if isempty(cols) cols = bt.cols end
    return any(map((c) -> rec[c] != trec[c], cols))  
end

load!(tbl::Tbl, rec::Rec, cx::Cx) = begin
    bt = BasicTbl(tbl)

    if haskey(rec, isdelCol)
        id = recid(rec)
        pushevt!(bt.ondelete, (bt.recs[id],), cx)
        delete!(bt.recs, id)
    else
        bt.recs[recid(rec)] = rec
        pushevt!(bt.onload, (rec,), cx)
    end

    return rec
end

ondelete!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    sub!(BasicTbl(tbl).ondelete, sub, cx) 
oninsert!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    sub!(BasicTbl(tbl).oninsert, sub, cx) 
onload!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    sub!(BasicTbl(tbl).onload, sub, cx)
onuprec!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    sub!(BasicTbl(tbl).onuprec, sub, cx) 

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
    name::Symbol
    fld::Fld
    tbl::Tbl

    RecCol(n::Symbol, tbl::Tbl) = new(n, Fld(), tbl)
end

convert(::Type{Fld}, col::RecCol) = col.fld

defname(col::RecCol) = col.name

immutable RefCol <: Col{RecId}
    name::Symbol
    fld::Fld
    tbl::Tbl

    RefCol(n::Symbol, tbl::Tbl) = new(n, Fld(), tbl)
end

convert(::Type{Fld}, col::RefCol) = col.fld

defname(col::RefCol) = col.name

getref(col::RefCol, rec::Rec, cx::Cx) = get(col.tbl, rec[col], cx)

typealias RecSize Int16
typealias ColSize Int8
typealias ValSize Int64

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

readval(t::Type{UUID}, s::ValSize, in::IOBuf) =
    RecId(read(in, UInt128))

readval{ValT}(col::Col{ValT}, s::ValSize, in::IOBuf, cx::Cx) = 
    readval(ValT, s, in)

readval(col::RecCol, s::ValSize, in::IOBuf, cx::Cx) = 
    get(col.tbl, readval(Rec, s, in), cx)

readrec(tbl::Tbl, in::IOBuf, cx::Cx) = begin
    len = read(in, RecSize)
    rec = Rec()
    
    for i = 1:len
        n = readstr(ColSize, in)
        s = read(in, ValSize)
        c = findcol(tbl, symbol(n))
        
        if isnull(c)
            skip(in, s)
        else
            c = get(c)
            rec[Fld(c)] = readval(c, s, in, cx)
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

writeval(val::DateTime, out::IOBuf) = write(out, datetime2unix(val))

writeval(val::Rec, out::IOBuf) = writeval(recid(val), out)

writeval(val::Str, out::IOBuf) = write(out, bytestring(val))

writeval(val::UUID, out::IOBuf) = write(out, val.value)

writeval(val, out::IOBuf) = write(out, val)

writeval{ValT}(col::Col{ValT}, val::ValT, out::IOBuf) = writeval(val, out)

sizeofval(val::DateTime) = sizeof(UInt128)
sizeofval(val::Str) = sizeof(val)
sizeofval(val::UUID) = sizeof(val.value)
sizeofval(val::Rec) = sizeof(RecId)
sizeofval(val) = sizeof(val)

recsize(tbl::Tbl, rec::Rec) = 
    RecSize(count((c) -> !istemp(c) && haskey(rec, Fld(c)), cols(tbl)))

writerec(tbl::Tbl, rec::Rec, out::IOBuf) = begin
    write(out, recsize(tbl, rec))

    for c in cols(tbl)
        f = Fld(c)

        if !istemp(c) && haskey(rec, f)
            writestr(ColSize, string(defname(c)), out)
            v = rec[f]
            write(out, ValSize(sizeofval(v)))
            writeval(c, v, out)
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
                BasicCol{Offs}(symbol("$(defname(tbl))_prevoffs")))

        pushcol!(tbl, isdelCol, t.offsCol, t.prevoffsCol)

        return t
    end
end

IO(tbl::Tbl, buf::IOBuf; 
   offsCol = BasicCol{Offs}(symbol("$(defname(tbl))_offs"))) = 
    IOTbl(tbl, buf, offsCol)

cols(tbl::IOTbl) = cols(tbl.wrapped)

convert(::Type{BasicTbl}, tbl::IOTbl) = BasicTbl(tbl.wrapped)

writerec(tbl::Tbl, rec::Rec) = begin
    iot = IOTbl(tbl)
    seekend(iot.buf)
    writerec(iot, rec, iot.buf)
end

delete!(tbl::IOTbl, id::RecId, cx::Cx) = begin
    delete!(tbl.wrapped, id, cx)
    writerec(tbl, RecOf(idCol => id, isdelCol => true))
end

get(tbl::IOTbl, idx::Revix{Offs}, id::RecId, cx::Cx) = begin
    if haskey(tbl.wrapped, id, cx)
        return get(tbl.wrapped, id, cx)
    elseif haskey(idx, id, cx)
        seek(tbl.buf, get(idx, id, cx))
        return load!(tbl, readrec(tbl, tbl.buf, cx), cx)
    end

    throw(RecNotFound())
end

insert!(tbl::IOTbl, rec::Rec, cx::Cx) = begin
    if !haskey(rec, tbl.offsCol)
        rec[tbl.offsCol] = -1
    end

    rec[tbl.prevoffsCol] = rec[tbl.offsCol]
    rec[tbl.offsCol] = position(tbl.buf)
    res = insert!(tbl.wrapped, rec, cx)
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

load!(tbl::Tbl, in::IOBuf, cx::Cx) = begin
    while !eof(in)
        load!(tbl, readrec(tbl, in, cx), cx)
    end
end

testTblBasics() = begin
    cx = Cx()
    t = Tbl(:foos)
    @assert isempty(t)
    @assert length(t) == 0

    r = insert!(t, Rec(), cx)
    @assert !isempty(t)
    @assert length(t) == 1

    rid = recid(r)
    @assert rid != Void
    @assert revision(r, t) == 1
    rinsertedat = insertedat(r, t)
    @assert rinsertedat != Void

    insert!(t, r, cx)
    @assert !isempty(t)
    @assert length(t) == 1
    @assert recid(r) == rid
    @assert insertedat(r, t) >= rinsertedat
    @assert revision(r, t) == 2
end

testGet() = begin
    cx = Cx()
    t = Tbl(:foos)
    r = insert!(t, Rec(), cx)
    gr = get(t, recid(r), cx)
    @assert gr == r
end

testIOTblBasics() = begin
    cx = Cx()
    t = IO(Tbl(:foos), TempBuf())
    r = insert!(t, Rec(), cx)
    @assert recid(r) != Void
    @assert revision(r, t) == 1
    @assert insertedat(r, t) != Void
    roffs = offs(r, t)
    @assert roffs > -1
    @assert prevoffs(r, t) == -1

    insert!(t, r, cx)
    @assert offs(r, t) > roffs
    @assert prevoffs(r, t) == roffs
end

testRecBasics() = begin
    r = Rec()
    c = BasicCol{Str}(:foo)
    @assert r[c] == Void
    r[c] = "abc"
    r[c] = "def"
    @assert r[c] == "def"
    @assert length(r) == 1
    delete!(r, c)
    @assert length(r) == 0
end

testTempCol() = begin
    cx = Cx()
    t = Tbl(:foo)
    c = BasicCol{Str}(:bar)
    tc = Temp(c)
    pushcol!(t, tc)

    r = Rec()
    r[tc] = "abc"
    @assert r[c] == "abc"

    buf = TempBuf()
    writerec(t, r, buf)
    seekstart(buf)
    rr = readrec(t, buf, cx)
    @assert !haskey(rr, tc)
end

testRecCol() = begin
    cx = Cx()
    t = Tbl(:foos)
    c = RecCol(:foo, t)
    foo = insert!(t, Rec(), cx)
    
    r = Rec()
    r[c] = foo

    @assert r[c] == foo
end

testRefCol() = begin
    cx = Cx()
    t = Tbl(:foos)
    c = RefCol(:foo, t)
    foo = insert!(t, Rec(), cx)
    
    r = Rec()
    r[c] = recid(foo)

    @assert getref(c, r, cx) == foo
end

testReadWriteRec() = begin
    cx = Cx()
    t = Tbl(:foos)
    c = BasicCol{Str}(:bar)
    pushcol!(t, c)
    r = Rec()
    r[c] = "abc"
    insert!(t, r, cx)
    
    buf = TempBuf()
    writerec(t, r, buf)
    seekstart(buf)
    rr = readrec(t, buf, cx)
    @assert rr[c] == r[c]
end

testReadWriteRecCol() = begin
    cx = Cx()
    foos = Tbl(:foos)
    bars = Tbl(:bars)
    barFoo = RecCol(:foo, foos)
    pushcol!(bars, barFoo)
    foo = insert!(foos, Rec(), cx)
    
    bar = Rec()
    bar[barFoo] = foo
    
    buf = TempBuf()
    writerec(bars, bar, buf)
    seekstart(buf)
    rbar = readrec(bars, buf, cx)
    @assert rbar[barFoo] == foo
end

testAliasCol() = begin
    foo = BasicCol{Str}(:foo)
    bar = alias(foo, :bar)
    
    r = Rec()
    r[foo] = "abc"
    @assert r[bar] == "abc"
end

testEmptyTbl() = begin
    cx = Cx()
    t = Tbl(:foos)
    r = insert!(t, Rec(), cx)
    empty!(t)
    @assert !haskey(t, recid(r), cx)
end

testDumpLoad() = begin
    cx = Cx()
    t = Tbl(:foos)
    r = insert!(t, Rec(), cx)
    buf = TempBuf()
    dump(t, buf)
    empty!(t)
    seekstart(buf)
    load!(t, buf, cx)
    @assert get(t, recid(r), cx) == r
end

testDelete() = begin
    cx = Cx()
    t = Tbl(:foos)
    r = insert!(t, Rec(), cx)
    id = recid(r)
    delete!(t, id, cx)
    @assert !haskey(t, id, cx)
end

testIODelete() = begin
    cx = Cx()
    buf = TempBuf()
    t = IO(Tbl(:foos), buf)
    r = insert!(t, Rec(), cx)
    id = recid(r)
    delete!(t, id, cx)
    empty!(t)
    seekstart(buf)
    load!(t, buf, cx)
    @assert !haskey(t, id, cx)
end

testIsdirty() = begin
    cx = Cx()
    t = Tbl(:foobars)
    foo = BasicCol{Str}(:foo)
    bar = BasicCol{Str}(:bar)
    pushcol!(t, foo, bar)

    r = RecOf(foo => "abc", bar => "def")

    @assert isdirty(t, r)
    @assert isdirty(t, r, foo, bar)

    insert!(t, r, cx)
    @assert !isdirty(t, r, foo, bar)

    r[foo] = "ghi"
    @assert !isdirty(t, r, bar)
    @assert isdirty(t, r, foo)

    insert!(t, r, cx)
    @assert !isdirty(t, r, foo, bar)    
end

testOninsert() = begin
    cx = Cx()
    t = Tbl(:foos)
    rec = Rec()
    wascalled = false
    oninsert!(t, (r) -> (@assert r == rec; wascalled = true), cx)
    insert!(t, rec, cx)
    @assert !wascalled
    @assert doevts!(cx) == 1
    @assert wascalled
end

testRevix() = begin
    cx = Cx()
    buf = TempBuf()
    tbl = IO(Tbl(:foo), buf)
    rx = Revix(:foo_offs, tbl.offsCol) 
    @assert isempty(rx)
    @assert length(rx) == 0
    pushdep!(tbl, rx, cx)

    rec = insert!(tbl, Rec(), cx)
    id = recid(rec)
    @assert doevts!(cx) == 1
    @assert haskey(rx, id, cx)
    @assert !isempty(rx)
    @assert length(rx) == 1
    @assert get(rx, id, cx) == offs(rec, tbl)
    
    empty!(tbl)
    @assert get(tbl, rx, id, cx) == rec
    doevts!(cx)
    delete!(tbl, rec, cx)
    doevts!(cx)
    @assert isempty(rx)
    @assert length(rx) == 0
    @assert !haskey(rx, id, cx)
end

testDumpLoadRevix() = begin
    cx = Cx()
    c = BasicCol{Str}(:foo)
    rx = Revix(:foos, c)
    r = insert!(rx, initrec!(RecOf(c => "abc")), cx)
    
    buf = TempBuf()
    dump(rx, buf)
    empty!(rx)
    seekstart(buf)
    load!(rx, buf, cx)
    @assert get(rx, recid(r), cx) == "abc"
end

testIORevix() = begin
    cx = Cx()
    c = BasicCol{Str}(:foo)
    buf = TempBuf()
    rx = IO(Revix(:foos, c), buf)
    rec = insert!(rx, initrec!(RecOf(c => "abc")), cx)
    id = recid(rec)

    empty!(rx)
    seekstart(buf)
    load!(rx, buf, cx)
    
    @assert get(rx, id, cx) == "abc"

    delete!(rx, rec, cx)
    seekstart(buf)
    load!(rx, buf, cx)
    @assert !haskey(rx, id, cx)
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
    testOninsert()
    testRevix()
    testDumpLoadRevix()
    testIORevix()
end

end
