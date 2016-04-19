module Fbls

import Base: isless, show

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

type SmultiDict{KeyT, ValT}
    top::SmultiNode{KeyT, ValT}
    bottom::SmultiNode{KeyT, ValT}
    length::Int

    SmultiDict(levels::Int) = begin
        d = new()
        d.top = SmultiNode{KeyT, ValT}()
        n = d.top

        for i in 1:levels-1
            n.down = SmultiNode{KeyT, ValT}() 
            n = n.down
        end

        d.bottom = n
        d.length = 0
        return d
    end
end

first{KeyT, ValT}(d::SmultiDict{KeyT, ValT}) = d.bottom.next.kv
last{KeyT, ValT}(d::SmultiDict{KeyT, ValT}) = d.bottom.prev.kv

insert!{KeyT, ValT}(d::SmultiDict{KeyT, ValT}, key::KeyT, val::ValT) = begin
    n = d.top
    pnn = nothing

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key)
            n = n.next
        end

        if n.kv != nothing && n.kv.first == key 
            n.kv = Pair{KeyT, ValT}(key, val)
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

    d.length += 1
    return true
end

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

show{KeyT, ValT}(io::IO, d::SmultiDict{KeyT, ValT}) = begin
    n = d.top
    pn = nothing

    while n != pn 
        show(io, n)
        println(io)
        pn = n
        n = n.down 
    end
end

testSmultiBasics() = begin
    d = SmultiDict{Int, Void}(8)
    @assert isempty(d)

    insert!(d, 1, nothing)
    @assert !isempty(d)

    insert!(d, 3, nothing)
    insert!(d, 2, nothing)
    insert!(d, 5, nothing)
    insert!(d, 4, nothing)

    print(d)
    
    @assert length(d) == 5
    @assert first(d).first == 1
    @assert last(d).first == 5

    empty!(d)
    @assert isempty(d)
    @assert length(d) == 0
end

testSmulti() = begin
    testSmultiBasics()
end

end
