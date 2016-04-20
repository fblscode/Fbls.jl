module Fbls

import Base: KeyError, empty!, getindex, haskey, isempty, isless, length, setindex, 
show

type SmultiNode{KeyT, ValT}
    down::SmultiNode{KeyT, ValT}
    next::SmultiNode{KeyT, ValT}
    kv::Any
    prev::SmultiNode{KeyT, ValT}

    SmultiNode() = begin
        n = new()
        n.kv = nothing
        n.prev = n
        n.next = n
        n.down = n
        return n
    end
    
    SmultiNode(key::KeyT, val::ValT, prev::SmultiNode{KeyT, ValT}) = begin
        n = new()
        n.kv = Pair{KeyT, ValT}(key, val)
        n.prev = prev
        n.next = prev.next
        n.down = n
        return n
    end
end

type Smulti{KeyT, ValT}
    bottom::SmultiNode{KeyT, ValT}
    length::Int
    prob::Rational
    top::SmultiNode{KeyT, ValT}

    Smulti(levels::Int) = begin
        s = new()
        s.top = SmultiNode{KeyT, ValT}()
        n = s.top
        s.prob = 1 // levels

        for i in 1:levels-1
            n.down = SmultiNode{KeyT, ValT}() 
            n = n.down
        end

        s.bottom = n
        s.length = 0
        return s
    end
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

        if n.kv != nothing && n.kv.first == key return n => depth end

        if n.prev.down == n.prev break end
        n = n.prev.down
        depth += 1
    end

    return nothing
end

getindex{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT) = begin
    n = findnode(s, key)
    if n != nothing return n.first.kv.second end
    throw(KeyError(key))
end

haskey{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT) =
    findnode(s, key) != nothing

insert!{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT, val::ValT; 
                    multi=false, update=false) = begin
    n = s.top
    pnn = nothing
    prob = s.prob

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
            if pnn != nothing nn.down = pnn end
            pnn = nn
        else
            prob += prob
        end

        if islast break end
        n = n.prev.down
        prob += prob
    end

    nn = pnn
    pnn = nothing
    while nn != pnn
        nn.prev.next = nn
        nn.next.prev = nn

        up = nn.down
        nn.down = if pnn == nothing nn else pnn end
        pnn = nn
        nn = up
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

    empty!(s)
    @assert isempty(s)
    @assert length(s) == 0
end

testSmulti() = begin
    testSmultiBasics()
end

end
