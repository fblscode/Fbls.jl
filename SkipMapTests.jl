runSkipMapBasics() = begin
    len = 50

    vs = Array(1:len)
    randArray!(vs)

    s = SkipMap{Int, Int}(5)
    @assert isempty(s)

    try
        v = s[42]
        @assert false
    catch e
        @assert isa(e, KeyError)
    end

    for v in vs insert!(s, v, v) end
    @assert !isempty(s)

    println(s)

    @assert length(s) == len
    @assert first(s).first == 1
    @assert last(s).first == len

    for v in vs 
        @assert haskey(s, v)
        @assert s[v] == v 
    end

    for v in vs s[v] = v * 2 end
    for v in vs @assert s[v] == v * 2 end

    offs = Int(len/2)
    for i in 1:offs
        @assert delete!(s, vs[i]) == 1
    end

    @assert length(s) == len - offs

    empty!(s)
    @assert isempty(s)
    @assert length(s) == 0
end

skipMapPerfreps = 5000

import DataStructures: SortedDict

runSkipMap() = begin
    runSkipMapBasics()

    vs = Array(1:skipMapPerfreps)
    randArray!(vs)

    @timeit runMapPerf(SortedDict(Dict{Int, Int}()), vs)
    @timeit runMapPerf(SkipMap{Int, Int}(10), vs)
end
