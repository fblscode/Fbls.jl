module Fbls

import Base: empty!, getindex, haskey, isempty, isless, length, setindex, 
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
        s.prob = 1

        for i in 1:levels-1
            n.down = SmultiNode{KeyT, ValT}() 
            n = n.down
            s.prob //= 2
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

insert!{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT, val::ValT; 
                    multi=false, update=false) = begin
    n = s.top
    pnn = nothing
    prob = s.prob
    prevadd = 1

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key) n = n.next end

        if !multi && n.kv != nothing && n.kv.first == key 
            if update n.kv = Pair{KeyT, ValT}(key, val) end
            return false 
        end

        p = rand(1)[1]

        islast = n.prev.down == n.prev

        if rand(1)[1] / prevadd < prob || islast
            nn = SmultiNode{KeyT, ValT}(key, val, n.prev)
            if pnn != nothing nn.down = pnn end
            pnn = nn
            prevadd += 1
        end

        if islast break end
        n = n.prev.down

        prob *= 3
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
    insert(d, key, val, update=true)

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

    s = Smulti{Int, Void}(8)
    @assert isempty(s)

    for v in vs insert!(s, v, nothing) end
    @assert !isempty(s)

    print(s)
    
    @assert length(s) == len
    @assert first(s).first == 1
    @assert last(s).first == len

    empty!(s)
    @assert isempty(s)
    @assert length(s) == 0
end

testSmulti() = begin
    testSmultiBasics()
end

end
