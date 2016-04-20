module Fbls

import Base: AbstractIOBuffer, KeyError, ==, convert, delete!, done, empty!, 
eof, get, gethash, getindex, index, isempty, isless, haskey, length, next, 
position, push!, seekend, setindex!, show, start
import Base.Dates: DateTime, datetime2unix, now, unix2datetime
import Base.Random: UUID, uuid4

typealias IOBuf AbstractIOBuffer
typealias Offs Int64
typealias RecId UUID
typealias Revision Int64
typealias Str AbstractString
typealias Vec{T} Array{T, 1}

abstract Err <: Exception
type DupKey <: Err end
type RecNotFound <: Err end

include("Smulti.jl")

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
    r = initrec!(Rec())
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

recid = Col(RecId, :fbls_id)
isdelCol = Col(Bool, :fbls_isdel)

==(l::Rec, r::Rec) = begin
    lid = get(l, recid, nothing)
    rid = get(r, recid, nothing)

    return lid != nothing && rid != nothing && lid == rid
end

hash(r::Rec) = hash(r[recid])

pushdep!(def, dep) = begin
    sub!(ondelete(def), (id) -> delete!(dep, id))
    sub!(onload(def), (rec) -> load!(dep, rec))
    sub!(onupsert(def), (rec) -> upsert!(dep, rec))
end

include("Tbl.jl")

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

writeval(val::Rec, out::IOBuf) = writeval(val[recid], out)

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

include("IOTbl.jl")
include("Revix.jl")
include("Sortix.jl")

get(tbl::IOTbl, idx::Revix{Offs}, id::RecId) = begin
    if haskey(tbl.wrapped, id)
        return get(tbl.wrapped, id)
    elseif haskey(idx, id)
        seek(tbl.buf, get(idx, id))
        return load!(tbl, readrec(tbl, tbl.buf))
    end

    throw(RecNotFound())
end

testTblBasics() = begin
    t = Tbl(:foos)
    @assert isempty(t)
    @assert length(t) == 0

    r = upsert!(t, Rec())
    @assert !isempty(t)
    @assert length(t) == 1

    rid = r[recid]
    @assert r[revision(t)] == 1
    rtimestamp = r[timestamp(t)]

    upsert!(t, r)
    @assert !isempty(t)
    @assert length(t) == 1
    @assert r[recid] == rid
    @assert r[timestamp(t)] >= rtimestamp
    @assert r[revision(t)] == 2
end

testGet() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    gr = get(t, r[recid])

    @assert gr == r
end

testIOTblBasics() = begin
    t = IO(Tbl(:foos), TempBuf())
    r = upsert!(t, Rec())
    @assert r[revision(t)] == 1
    
    roffs = r[offs(t)]
    @assert roffs > -1
    @assert r[prevoffs(t)] == -1

    upsert!(t, r)
    @assert r[offs(t)] > roffs
    @assert r[prevoffs(t)] == roffs
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
    r[c] = foo[recid]
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

    @assert !haskey(t, r[recid])
end

testDumpLoad() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    buf = TempBuf()
    dump(t, buf)
    empty!(t)
    seekstart(buf)
    load!(t, buf)

    @assert get(t, r[recid]) == r
end

testDelete() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    id = r[recid]
    delete!(t, id)

    @assert !haskey(t, id)
end

testIODelete() = begin
    buf = TempBuf()
    t = IO(Tbl(:foos), buf)
    r = upsert!(t, Rec())
    id = r[recid]
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
    @assert isdirty(r, t)
    @assert isdirty(r, t, foo, bar)

    upsert!(t, r)
    @assert !isdirty(r, t, foo, bar)

    r[foo] = "ghi"
    @assert !isdirty(r, t, bar)
    @assert isdirty(r, t, foo)

    upsert!(t, r)
    @assert !isdirty(r, t, foo, bar)    
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
    rx = Revix(:foo_offs, tbl.offs) 
    @assert isempty(rx)
    @assert length(rx) == 0

    pushdep!(tbl, rx)
    rec = upsert!(tbl, Rec())
    id = rec[recid]
    @assert haskey(rx, id)
    @assert !isempty(rx)
    @assert length(rx) == 1
    @assert get(rx, id) == rec[offs(tbl)]
    
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
    r = upsert!(rx, RecOf(c => "abc"))
    
    buf = TempBuf()
    dump(rx, buf)
    empty!(rx)
    seekstart(buf)
    load!(rx, buf)

    @assert get(rx, r[recid]) == "abc"
end

testIORevix() = begin
    c = Col(Str, :foo)
    buf = TempBuf()
    rx = IO(Revix(:foos, c), buf)
    rec = upsert!(rx, RecOf(c => "abc"))
    id = rec[recid]

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
    testSmulti()
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
    testSortix()
end

end
