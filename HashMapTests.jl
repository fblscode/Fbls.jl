runHashMapBasics() = begin
    len = 50

    vs = Array(1:len)

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

    print(m)
    
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

runHashMap() = begin
    runHashMapBasics()
end
