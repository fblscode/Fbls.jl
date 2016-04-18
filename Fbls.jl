module Fbls

import Base: AbstractIOBuffer, ==, convert, delete!, empty!, eof, getindex, haskey, position, seekend, setindex!
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

evtsub!(evt::AnyEvt, sub::EvtSub, cx::Cx) = begin
    bcx = BasicCx(cx)

    subs = if haskey(bcx.evtsubs, evt)
        bcx.evtsubs[evt]
    else
        bcx.evtsubs[evt] = Vec{EvtSub}()
    end

    push!(subs, sub)
end

evtunsub!(evt::AnyEvt, sub::EvtSub, cx::Cx) = 
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

istemp(::AnyCol) = false

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
    name::Str
    fld::Fld

    BasicCol(name::Str, fld::Fld) = new(name, fld)
    BasicCol(name::Str) = new(name, Fld())
end

alias{ValT}(col::BasicCol{ValT}, n::Str) = BasicCol{ValT}(n, col.fld)

convert{ValT}(::Type{Fld}, col::BasicCol{ValT}) = col.fld

defname{ValT}(col::BasicCol{ValT}) = col.name

immutable TempCol{ValT} <: Col{ValT}
    wrapped::Col{ValT}
    name::Str

    TempCol(col::Col{ValT}, name::Str) = new(col, name)
end

asTempCol{ValT}(col::Col{ValT}; name=defname(col)) = TempCol{ValT}(col, name)

convert{ValT}(::Type{Fld}, col::TempCol{ValT}) = Fld(col.wrapped)

defname{ValT}(col::TempCol{ValT}) = defname(col.wrapped)

istemp{ValT}(::TempCol{ValT}) = true

RecId() = uuid4()

idCol = BasicCol{RecId}("fbls/id")
isdelCol = BasicCol{Bool}("fbls/isdel")

recid(r::Rec) = r[idCol]

==(l::Rec, r::Rec) = begin
    lid = recid(l)
    rid = recid(r)
    return lid != Void && rid != Void && lid == rid
end

typealias RevixRecs{ValT} Dict{RecId, ValT}

immutable Revix{ValT}
    name::Str
    col::Col{ValT}
    recs::RevixRecs{ValT}
    ondelrec::Evt{Tuple{Rec}} 
    oninsrec::Evt{Tuple{Rec}} 
    onloadrec::Evt{Tuple{Rec}} 

    Revix(n::Str, c::Col{ValT}) = new(n, c, 
                                      RevixRecs{ValT}(), 
                                      Evt{Tuple{Rec}}(),
                                      Evt{Tuple{Rec}}(),
                                      Evt{Tuple{Rec}}())
end

defname(rx::Revix) = rx.name

getval{ValT}(rx::Revix{ValT}, id::RecId, cx::Cx) = 
    if haskey(rx.recs, id) Nullable{ValT}(rx.recs[id]) else Nullable{ValT}() end

haskey(rx::Revix, id::RecId, cx::Cx) = haskey(rx.recs, id)

delrec!(rx::Revix, rec::Rec, cx::Cx) = begin
    delete!(rx.recs, recid(rec))
    pushevt!(rx.ondelrec, (rec,), cx)
end

insrec!(rx::Revix, rec::Rec, cx::Cx) = begin
    rx.recs[recid(rec)] = rec[rx.col]
    pushevt!(rx.oninsrec, (rec,), cx)
end

loadrec!(rx::Revix, rec::Rec, cx::Cx) = begin
    rx.recs[recid(rec)] = rec[rx.col]
    pushevt!(rx.onloadrec, (rec,), cx)
end

ondelrec!(rx::Revix, sub::EvtSub, cx::Cx) = 
    evtsub!(rx.ondelrec, sub, cx)
oninsrec!(rx::Revix, sub::EvtSub, cx::Cx) = 
    evtsub!(rx.oninsrec, sub, cx) 
onloadrec!(rx::Revix, sub::EvtSub, cx::Cx) = 
    evtsub!(rx.onloadrec, sub, cx)

typealias TblCols Dict{Str, AnyCol}
typealias TblRecs Dict{RecId, Rec} 

abstract Tbl

immutable BasicTbl <: Tbl
    name::Str
    cols::TblCols
    recs::TblRecs
    instimeCol::Col{DateTime}
    revCol::Col{Int64}
    ondelrec::Evt{Tuple{Rec}} 
    oninsrec::Evt{Tuple{Rec}} 
    onloadrec::Evt{Tuple{Rec}} 
    onuprec::Evt{Tuple{Rec, Rec}} 

    BasicTbl(name::Str) = begin
        t = new(name, 
                TblCols(),
                TblRecs(), 
                BasicCol{DateTime}("$name/ins-time"), 
                BasicCol{Int64}("$name/rev"),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec}}(),
                Evt{Tuple{Rec, Rec}}())
        pushcol!(t, idCol, t.instimeCol, t.revCol)
        return t
    end
end

cols(tbl::BasicTbl) = values(tbl.cols)

defname(tbl::BasicTbl) = tbl.name

empty!(tbl::Tbl) = empty!(BasicTbl(tbl).recs)

findcol(tbl::Tbl, name::Str) = begin
    bt = BasicTbl(tbl)
    return if haskey(bt.cols, name) 
        Nullable{AnyCol}(bt.cols[name]) 
    else 
        Nullable{AnyCol}() 
    end
end

haskey(tbl::Tbl, id::RecId, cx::Cx) = haskey(BasicTbl(tbl).recs, id)

instime(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).instimeCol]

recrev(rec::Rec, tbl::Tbl) = rec[BasicTbl(tbl).revCol]

delrec!(tbl::Tbl, id::RecId, cx::Cx) = begin
    bt = BasicTbl(tbl)
    pushevt!(bt.ondelrec, (bt.recs[id],), cx)
    delete!(bt.recs, id) 
end

delrec!(tbl::Tbl, rec::Rec, cx::Cx) = delrec!(tbl, recid(rec), cx)

getrec(tbl::Tbl, id::RecId, cx::Cx) = begin
    bt = BasicTbl(tbl)
    return if haskey(bt.recs, id) 
        Nullable{Rec}(bt.recs[id]) 
    else 
        Nullable{Rec}() 
    end
end

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

insrec!(tbl::Tbl, rec::Rec, cx::Cx) = begin
    bt = BasicTbl(tbl)

    id = recid(rec)
    rec[bt.instimeCol] = now()

    pushevt!(bt.oninsrec, (rec,), cx)

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

loadrec!(tbl::Tbl, rec::Rec, cx::Cx) = begin
    bt = BasicTbl(tbl)

    if haskey(rec, isdelCol)
        id = recid(rec)
        pushevt!(bt.ondelrec, (bt.recs[id],), cx)
        delete!(bt.recs, id)
    else
        bt.recs[recid(rec)] = rec
        pushevt!(bt.onloadrec, (rec,), cx)
    end

    return rec
end

ondelrec!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    evtsub!(BasicTbl(tbl).ondelrec, sub, cx) 
oninsrec!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    evtsub!(BasicTbl(tbl).oninsrec, sub, cx) 
onloadrec!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    evtsub!(BasicTbl(tbl).onloadrec, sub, cx)
onuprec!(tbl::Tbl, sub::EvtSub, cx::Cx) = 
    evtsub!(BasicTbl(tbl).onuprec, sub, cx) 

pushcol!(tbl::Tbl, cols::AnyCol...) = begin
    bt = BasicTbl(tbl)
    for c in cols bt.cols[defname(c)] = c end
end

pushdep!(tbl::Tbl, dep, cx::Cx) = begin
    ondelrec!(tbl, (rec) -> delrec!(dep, rec, cx), cx)
    oninsrec!(tbl, (rec) -> insrec!(dep, rec, cx), cx)
    onloadrec!(tbl, (rec) -> loadrec!(dep, rec, cx), cx)
end

recs(tbl::Tbl) = values(BasicTbl(tbl).recs)

immutable RecCol <: Col{Rec}
    name::Str
    fld::Fld
    tbl::Tbl

    RecCol(name::Str, tbl::Tbl) = new(name, Fld(), tbl)
end

convert(::Type{Fld}, col::RecCol) = col.fld

defname(col::RecCol) = col.name

immutable RefCol <: Col{RecId}
    name::Str
    fld::Fld
    tbl::Tbl

    RefCol(name::Str, tbl::Tbl) = new(name, Fld(), tbl)
end

convert(::Type{Fld}, col::RefCol) = col.fld

defname(col::RefCol) = col.name

getref(col::RefCol, rec::Rec, cx::Cx) = begin 
    gr = getrec(col.tbl, rec[col], cx)
    if isnull(gr) throw(RecNotFound()) end
    return get(gr)
end

typealias RecSize UInt16
typealias ColSize UInt8
typealias ValSize UInt64

readstr{LenT}(::Type{LenT}, in::IOBuf) = begin
    pos = position(in)
    len = read(in, LenT)
    return utf8(read(in, UInt8, len))
end

readval{ValT}(::Type{ValT}, s::ValSize, in::IOBuf) = read(in, ValT) 

readval(t::Type{DateTime}, s::ValSize, in::IOBuf) = begin
    #@assert s == sizeof(Float64)
    return unix2datetime(read(in, Float64))
end

readval(t::Type{Rec}, s::ValSize, in::IOBuf) = readval(RecId, s, in)

readval(t::Type{Str}, s::ValSize, in::IOBuf) = utf8(read(in, UInt8, s))

readval(t::Type{UUID}, s::ValSize, in::IOBuf) = begin
    #@assert s == sizeof(UInt128)
    return RecId(read(in, UInt128))
end

readval{ValT}(col::Col{ValT}, s::ValSize, in::IOBuf, cx::Cx) = 
    readval(ValT, s, in)

readval(col::RecCol, s::ValSize, in::IOBuf, cx::Cx) = begin
    gr = getrec(col.tbl, readval(Rec, s, in), cx)
    if isnull(gr) throw(RecNotFound()) end
    return get(gr)
end

readrec(tbl::Tbl, in::IOBuf, cx::Cx) = begin
    len = read(in, RecSize)
    rec = Rec()
    
    for i = 1:len
        n = readstr(ColSize, in)
        s = read(in, ValSize)
        c = findcol(tbl, n)
        
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
            n = defname(c)
            writestr(ColSize, n, out)
            v = rec[f]
            write(out, ValSize(sizeofval(v)))
            writeval(c, v, out)
        end
    end
end

typealias IOIdx Revix{Offs}

immutable IOTbl <: Tbl
    wrapped::Tbl
    name::Str
    buf::IOBuf
    offsCol::Col{Offs}
    prevoffsCol::Col{Offs}
    
    IOTbl(tbl::Tbl, name::Str, buf::IOBuf;
          offsCol = BasicCol{Offs}("$name/offs")) = begin
        t = new(tbl, name, buf, offsCol, 
                BasicCol{Offs}("$name/prevoffs"))
        pushcol!(tbl, isdelCol, t.offsCol, t.prevoffsCol)
        return t
    end
end

asIOTbl(tbl::Tbl, buf::IOBuf; name = defname(tbl)) = IOTbl(tbl, name, buf)

IOTbl(name::Str, buf::IOBuf) = asIOTbl(BasicTbl(name), buf)

cols(tbl::IOTbl) = cols(tbl.wrapped)

convert(::Type{BasicTbl}, tbl::IOTbl) = BasicTbl(tbl.wrapped)

writerec(tbl::Tbl, rec::Rec) = begin
    iot = IOTbl(tbl)
    seekend(iot.buf)
    writerec(iot, rec, iot.buf)
end

delrec!(tbl::IOTbl, id::RecId, cx::Cx) = begin
    delrec!(tbl.wrapped, id, cx)
    writerec(tbl, RecOf(idCol => id, isdelCol => true))
end

getrec(tbl::IOTbl, idx::IOIdx, id::RecId, cx::Cx) = begin
    r = getrec(tbl.wrapped, id, cx)

    if isnull(r) && haskey(idx, id, cx)
        seek(tbl.buf, get(getval(idx, id, cx)))
        r = loadrec!(tbl, readrec(tbl, tbl.buf, cx), cx)
    end

    return r
end

insrec!(tbl::IOTbl, rec::Rec, cx::Cx) = begin
    if !haskey(rec, tbl.offsCol)
        rec[tbl.offsCol] = -1
    end

    rec[tbl.prevoffsCol] = rec[tbl.offsCol]
    rec[tbl.offsCol] = position(tbl.buf)
    res = insrec!(tbl.wrapped, rec, cx)
    writerec(tbl, rec)
    return res
end

offs(rec::Rec, tbl::Tbl) = rec[IOTbl(tbl).offsCol]
prevoffs(rec::Rec, tbl::Tbl) = rec[IOTbl(tbl).prevoffsCol]

dumprecs(tbl::Tbl, out::IOBuf) = begin
    for r in recs(tbl)
        writerec(tbl, r, out)
    end
end

loadrecs!(tbl::Tbl, in::IOBuf, cx::Cx) = begin
    while !eof(in)
        loadrec!(tbl, readrec(tbl, in, cx), cx)
    end
end

testTblBasics() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    r = insrec!(t, Rec(), cx)
    rid = recid(r)
    @assert rid != Void
    @assert recrev(r, t) == 1
    rinstime = instime(r, t)
    @assert rinstime != Void

    insrec!(t, r, cx)
    @assert recid(r) == rid
    @assert instime(r, t) >= rinstime
    @assert recrev(r, t) == 2
end

testGetrec() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    r = insrec!(t, Rec(), cx)
    gr = getrec(t, recid(r), cx)
    @assert !isnull(gr) && get(gr) == r
end

testIOTblBasics() = begin
    cx = BasicCx()
    t = IOTbl("foos", TempBuf())
    r = insrec!(t, Rec(), cx)
    @assert recid(r) != Void
    @assert recrev(r, t) == 1
    @assert instime(r, t) != Void
    roffs = offs(r, t)
    @assert roffs > -1
    @assert prevoffs(r, t) == -1

    insrec!(t, r, cx)
    @assert offs(r, t) > roffs
    @assert prevoffs(r, t) == roffs
end

testRecBasics() = begin
    r = Rec()
    c = BasicCol{Str}("foo")
    @assert r[c] == Void
    r[c] = "abc"
    r[c] = "def"
    @assert r[c] == "def"
    @assert length(r) == 1
    delete!(r, c)
    @assert length(r) == 0
end

testTempCol() = begin
    cx = BasicCx()
    t = BasicTbl("foo")
    c = BasicCol{Str}("bar")
    tc = asTempCol(c)
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
    cx = BasicCx()
    t = BasicTbl("foos")
    c = RecCol("foo", t)
    foo = insrec!(t, Rec(), cx)
    
    r = Rec()
    r[c] = foo

    @assert r[c] == foo
end

testRefCol() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    c = RefCol("foo", t)
    foo = insrec!(t, Rec(), cx)
    
    r = Rec()
    r[c] = recid(foo)

    @assert getref(c, r, cx) == foo
end

testReadWriteRec() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    c = BasicCol{Str}("bar")
    pushcol!(t, c)
    r = Rec()
    r[c] = "abc"
    insrec!(t, r, cx)
    
    buf = TempBuf()
    writerec(t, r, buf)
    seekstart(buf)
    rr = readrec(t, buf, cx)
    @assert rr[c] == r[c]
end

testReadWriteRecCol() = begin
    cx = BasicCx()
    foos = BasicTbl("foos")
    bars = BasicTbl("bars")
    barFoo = RecCol("foo", foos)
    pushcol!(bars, barFoo)
    foo = insrec!(foos, Rec(), cx)
    
    bar = Rec()
    bar[barFoo] = foo
    
    buf = TempBuf()
    writerec(bars, bar, buf)
    seekstart(buf)
    rbar = readrec(bars, buf, cx)
    @assert rbar[barFoo] == foo
end

testAliasCol() = begin
    foo = BasicCol{Str}("foo")
    bar = alias(foo, "bar")
    
    r = Rec()
    r[foo] = "abc"
    @assert r[bar] == "abc"
end

testEmptyTbl() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    r = insrec!(t, Rec(), cx)
    empty!(t)
    @assert !haskey(t, recid(r), cx)
    @assert isnull(getrec(t, recid(r), cx))
end

testDumpLoadRecs() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    r = insrec!(t, Rec(), cx)
    buf = TempBuf()
    dumprecs(t, buf)
    empty!(t)
    seekstart(buf)
    loadrecs!(t, buf, cx)
    @assert get(getrec(t, recid(r), cx)) == r
end

testDelRec() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    r = insrec!(t, Rec(), cx)
    delrec!(t, recid(r), cx)
    gr = getrec(t, recid(r), cx)
    @assert isnull(gr)
end

testIODelRec() = begin
    cx = BasicCx()
    buf = TempBuf()
    t = IOTbl("foos", buf)
    r = insrec!(t, Rec(), cx)
    delrec!(t, recid(r), cx)
    empty!(t)
    seekstart(buf)
    loadrecs!(t, buf, cx)
    gr = getrec(t, recid(r), cx)
    @assert isnull(gr)
end

testIsdirty() = begin
    cx = BasicCx()
    t = BasicTbl("foobars")
    foo = BasicCol{Str}("foo")
    bar = BasicCol{Str}("bar")
    pushcol!(t, foo, bar)

    r = RecOf(foo => "abc", bar => "def")

    @assert isdirty(t, r)
    @assert isdirty(t, r, foo, bar)

    insrec!(t, r, cx)
    @assert !isdirty(t, r, foo, bar)

    r[foo] = "ghi"
    @assert !isdirty(t, r, bar)
    @assert isdirty(t, r, foo)

    insrec!(t, r, cx)
    @assert !isdirty(t, r, foo, bar)    
end

testOninsrec() = begin
    cx = BasicCx()
    t = BasicTbl("foos")
    rec = Rec()
    wascalled = false
    oninsrec!(t, (r) -> (@assert r == rec; wascalled = true), cx)
    insrec!(t, rec, cx)
    @assert !wascalled
    @assert doevts!(cx) == 1
    @assert wascalled
end

testRevix() = begin
    cx = BasicCx()
    buf = TempBuf()
    tbl = IOTbl("foos", buf)
    rx = Revix{Offs}("offs", tbl.offsCol) 
    pushdep!(tbl, rx, cx)
    rec = insrec!(tbl, Rec(), cx)
    id = recid(rec)
    @assert doevts!(cx) == 1
    @assert haskey(rx, id, cx)
    @assert get(getval(rx, id, cx)) == offs(rec, tbl)
    
    empty!(tbl)
    @assert getrec(tbl, rx, id, cx) == rec
    doevts!(cx)

    delrec!(tbl, rec, cx)
    doevts!(cx)
    @assert !haskey(rx, id, cx)
end

testAll() = begin
    testTblBasics()
    testRecBasics()
    testIOTblBasics()
    testGetrec()
    testTempCol()
    testRecCol()
    testRefCol()
    testReadWriteRec()
    testReadWriteRecCol()
    testAliasCol()
    testEmptyTbl()
    testDumpLoadRecs()
    testDelRec()
    testIODelRec()
    testIsdirty()
    testOninsrec()
    testRevix()
end

end
