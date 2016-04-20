type SmultiNode{KeyT, ValT}
    down::SmultiNode{KeyT, ValT}
    next::SmultiNode{KeyT, ValT}
    kv::Any
    prev::SmultiNode{KeyT, ValT}
    up::SmultiNode{KeyT, ValT}

    SmultiNode() = begin
        n = new()
        n.kv = nothing
        n.prev = n
        n.next = n
        n.down = n
        n.up = n
        return n
    end
    
    SmultiNode(key::KeyT, val::ValT, prev::SmultiNode{KeyT, ValT}) = begin
        n = new()
        n.kv = Pair{KeyT, ValT}(key, val)
        n.prev = prev
        n.next = prev.next
        n.down = n
        n.up = n
        return n
    end
end

type Smulti{KeyT, ValT}
    bottom::SmultiNode{KeyT, ValT}
    length::Int
    levels::Rational
    top::SmultiNode{KeyT, ValT}

    Smulti(levels::Int) = begin
        s = new()
        s.levels = levels
        s.top = SmultiNode{KeyT, ValT}()
        n = s.top

        for i in 1:levels-1
            n.down = SmultiNode{KeyT, ValT}()
            n.down.up = n
            n = n.down
        end

        s.bottom = n
        s.length = 0
        return s
    end
end

delete!{KeyT, ValT}(s::Smulti{KeyT, ValT}, 
                    key::KeyT, val = nothing) = begin
    n = findnode(s, key)
    if n == nothing return 0 end

    cnt = 0

    while n.kv != nothing && n.kv.first == key
        if val == nothing || n.kv.second == val
            n.prev.next = n.next
            n.next.prev = n.prev
            cnt += 1
        end
        
        n = n.next
    end

    s.length -= cnt
    return cnt
end

empty!{KeyT, ValT}(s::Smulti{KeyT, ValT}) = begin
    n = s.top
    pn = nothing
    
    while n != pn
        n.prev = n
        n.next = n
        pn = n
        n = n.next
    end

    s.length = 0

    return s
end

first{KeyT, ValT}(s::Smulti{KeyT, ValT}) = s.bottom.next.kv

findnode{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT) = begin
    n = s.top
    depth = 1

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key) n = n.next end

        if n.kv != nothing && n.kv.first == key return n end

        if n.prev.down == n.prev break end
        n = n.prev.down
        depth += 1
    end

    return nothing
end

getindex{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT) = begin
    n = findnode(s, key)
    if n != nothing return n.kv.second end
    throw(KeyError(key))
end

haskey{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT) =
    findnode(s, key) != nothing

insert!{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT, val::ValT; 
                    multi=false, update=false) = begin
    n = s.top
    pnn = nothing
    dprob = 1 // s.levels
    prob = dprob

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key) n = n.next end

        if !multi && n.kv != nothing && n.kv.first == key 
            if update n.kv = Pair{KeyT, ValT}(key, val) end
            return false 
        end

        islast = n.prev.down == n.prev

        if islast || rand() < prob
            nn = SmultiNode{KeyT, ValT}(key, val, n.prev)
            if pnn != nothing nn.up = pnn end
            pnn = nn
        else
            prob += dprob
        end

        if islast break end
        n = n.prev.down
        prob += dprob
    end

    nn = pnn
    while true
        nn.prev.next = nn
        nn.next.prev = nn

        if nn.up == nn break end
        nn.up.down = nn
        nn = nn.up
    end

    s.length += 1
    return true
end

isempty{KeyT, ValT}(s::Smulti{KeyT, ValT}) = s.length == 0

last{KeyT, ValT}(s::Smulti{KeyT, ValT}) = s.bottom.prev.kv

length{KeyT, ValT}(s::Smulti{KeyT, ValT}) = s.length

setindex!{KeyT, ValT}(s::Smulti{KeyT, ValT}, val::ValT, key::KeyT) =
    insert!(s, key, val, update=true)

show{KeyT, ValT}(io::IO, n::SmultiNode{KeyT, ValT}) = begin
    print(io, "[")
    n = n.next
    sep = ""

    while n.kv != nothing
        print(io, sep, n.kv.first)
        if n.kv.second != nothing print(io, ":", n.kv.second) end
        sep = ", "
        n = n.next
    end

    print(io, "]")
end

show{KeyT, ValT}(io::IO, s::Smulti{KeyT, ValT}) = begin
    n = s.top
    pn = nothing

    while n != pn 
        show(io, n)
        println(io)
        pn = n
        n = n.down 
    end
end

randArray(a::Array{Int, 1}) = begin
    len = length(a)

    for i in 1:len
        j = rand(1:l)
        tmp = a[i]
        a[i] = a[j]
        a[j] = tmp
    end
end

testSmultiBasics() = begin
    len = 50

    vs = Array(1:len)

    s = Smulti{Int, Int}(5)
    @assert isempty(s)

    try
        v = s[42]
        @assert false
    catch e
        @assert isa(e, KeyError)
    end

    for v in vs insert!(s, v, v) end
    @assert !isempty(s)

    print(s)
    
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

testSmulti() = begin
    testSmultiBasics()
end
