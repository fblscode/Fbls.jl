runHashMapBasics() = begin
    len = 50

    vs = Array(1:len)
    randArray!(vs)

    m = HashMap{Int, Int}(10, 5)
    @assert isempty(m)

    try
        v = m[42]
        @assert false
    catch e
        @assert isa(e, KeyError)
    end

    for v in vs insert!(m, v, v) end
    @assert !isempty(m)

    @assert length(m) == len

    for v in vs 
        @assert haskey(m, v)
        @assert m[v] == v 
    end

    for v in vs m[v] = v * 2 end
    for v in vs @assert m[v] == v * 2 end

    offs = Int(len/2)
    for i in 1:offs @assert delete!(m, vs[i]) == 1 end

    @assert length(m) == len - offs

    empty!(m)
    @assert isempty(m)
    @assert length(m) == 0
end

hashMapPerfreps = 10000

runHashMapPerf(m, vs::Array{Int, 1}) = begin
    for v in vs m[v] = v end
    for v in vs @assert haskey(m, v) end
    for v in vs m[v] = v * 2 end
    for v in vs delete!(m, v) end
end

runHashMap() = begin
    runHashMapBasics()

    vs = Array(1:hashMapPerfreps)
    randArray!(vs)

    @timeit runMapPerf(Dict{Int, Int}(), vs)
    @timeit runMapPerf(HashMap{Int, Int}(8000, 1), vs)
end
