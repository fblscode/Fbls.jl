module Fbls

import Base: empty!, getindex, haskey, isempty, isless, length, setindex, 
show

type SmultiNode{KeyT, ValT}
    prev::SmultiNode{KeyT, ValT}
    next::SmultiNode{KeyT, ValT}
    down::SmultiNode{KeyT, ValT}
    kv

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
    top::SmultiNode{KeyT, ValT}
    bottom::SmultiNode{KeyT, ValT}
    length::Int

    Smulti(levels::Int) = begin
        s = new()
        s.top = SmultiNode{KeyT, ValT}()
        n = s.top

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

insert!{KeyT, ValT}(s::Smulti{KeyT, ValT}, key::KeyT, val::ValT; 
                    multi=false, update=false) = begin
    n = s.top
    pnn = nothing

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key)
            n = n.next
        end

        if !multi && n.kv != nothing && n.kv.first == key 
            if update n.kv = Pair{KeyT, ValT}(key, val) end
            return false 
        end

        nn = SmultiNode{KeyT, ValT}(key, val, n.prev)
        if pnn != nothing nn.down = pnn end
        pnn = nn

        if n.prev.down == n.prev 
            break 
        end

        n = n.prev.down
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

testSmultiBasics() = begin
    s = Smulti{Int, Void}(8)
    @assert isempty(s)

    insert!(s, 1, nothing)
    @assert !isempty(s)

    insert!(s, 3, nothing)
    insert!(s, 2, nothing)
    insert!(s, 5, nothing)
    insert!(s, 4, nothing)

    print(s)
    
    @assert length(s) == 5
    @assert first(s).first == 1
    @assert last(s).first == 5

    empty!(s)
    @assert isempty(s)
    @assert length(s) == 0
end

testSmulti() = begin
    testSmultiBasics()
end

end
