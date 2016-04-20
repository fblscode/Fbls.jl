module Tests

macro timen(ex, n)
    quote
        local t0 = time_ns()
        for i = 1:$(esc(n))
            local val = $(esc(ex))
        end
        local t1 = time_ns()
        (t1 - t0) / 1.e9
    end
end

macro timeit(ex)
    quote
        local val = $(esc(ex))  # Warm up
        t = zeros(3)
        
        # Determine number of loops so that total time > 0.1s.
        n = 1
        for i = 0:9
            n = 10^i
            t[1] = @timen $(esc(ex)) n
            if t[1] > 0.1
                break
            end
        end

        # Two more production runs.
        for i = 2:3
            t[i] = @timen $(esc(ex)) n
        end
        best = minimum(t) / n

        # Format to nano-, micro- or milliseconds.
        factor = 1.
        if best < 1e-6
            factor = 1e9
            pre = "n"
        elseif best < 1e-3
            factor = 1e6
            pre = "\u00b5"
        elseif best < 1.
            factor = 1e3
            pre = "m"
        else
            factor = 1.
            pre = ""
        end
        @printf "%d loops, best of 3: %4.2f %ss per loop\n" n best*factor pre
        best
    end
end

include("FblsDefs.jl")

runTblBasics() = begin
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

runGet() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    gr = get(t, r[recid])

    @assert gr == r
end

runIOTblBasics() = begin
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

runRecBasics() = begin
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

runTempCol() = begin
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

runRecCol() = begin
    t = Tbl(:foos)
    c = RecCol(:foo, t)
    foo = upsert!(t, Rec())
    
    r = Rec()
    r[c] = foo
    @assert r[c] == foo
end

runRefCol() = begin
    t = Tbl(:foos)
    c = RefCol(:foo, t)
    foo = upsert!(t, Rec())
    
    r = Rec()
    r[c] = foo[recid]
    @assert getref(c, r) == foo
end

runReadWriteRec() = begin
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

runReadWriteRecCol() = begin
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

runAliasCol() = begin
    foo = Col(Str, :foo)
    bar = alias(foo, :bar)
    
    r = Rec()
    r[foo] = "abc"
    @assert r[bar] == "abc"
end

runEmptyTbl() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    empty!(t)

    @assert !haskey(t, r[recid])
end

runDumpLoad() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    buf = TempBuf()
    dump(t, buf)
    empty!(t)
    seekstart(buf)
    load!(t, buf)

    @assert get(t, r[recid]) == r
end

runDelete() = begin
    t = Tbl(:foos)
    r = upsert!(t, Rec())
    id = r[recid]
    delete!(t, id)

    @assert !haskey(t, id)
end

runIODelete() = begin
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

runIsdirty() = begin
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

runOnupsert() = begin
    t = Tbl(:foos)
    rec = Rec()
    wascalled = false
    sub!(onupsert(t), (r) -> (@assert r == rec; wascalled = true))
    upsert!(t, rec)
    @assert wascalled
end

runRevix() = begin
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

runDumpLoadRevix() = begin
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

runIORevix() = begin
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

include("SkipMapTests.jl")
include("HashMapTests.jl")
include("SkipixTests.jl")
include("HashixTests.jl")

runAll() = begin
    runSkipMap()
    runHashMap()
    runTblBasics()
    runRecBasics()
    runIOTblBasics()
    runGet()
    runTempCol()
    runRecCol()
    runRefCol()
    runReadWriteRec()
    runReadWriteRecCol()
    runAliasCol()
    runEmptyTbl()
    runDumpLoad()
    runDelete()
    runIODelete()
    runIsdirty()
    runOnupsert()
    runRevix()
    runDumpLoadRevix()
    runIORevix()
    runSkipix()
    runHashix()
end

end
