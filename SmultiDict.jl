module Fbls

import Base: isless, show

type SmultiNode{KeyT, ValT}
    prev::SmultiNode{KeyT, ValT}
    next::SmultiNode{KeyT, ValT}
    down::SmultiNode{KeyT, ValT}
    key
    val

    SmultiNode() = begin
        n = new()
        n.key = nothing
        n.val = nothing
        n.prev = n
        n.next = n
        n.down = n
        return n
    end
    
    SmultiNode(key::KeyT, val::ValT, prev::SmultiNode{KeyT, ValT}) = begin
        n = new()
        n.key = key
        n.val = val
        n.prev = prev
        n.next = prev.next
        n.down = n
        return n
    end
end

immutable SmultiDict{KeyT, ValT}
    root::SmultiNode{KeyT, ValT}

    SmultiDict(levels::Int) = begin
        s = new(SmultiNode{KeyT, ValT}())
        n = s.root
        for i in 1:levels-1
            n.down = SmultiNode{KeyT, ValT}() 
            n = n.down
        end
        return s
    end
end

show{KeyT, ValT}(io::IO, s::SmultiDict{KeyT, ValT}) = begin
    n = s.root
    while n.down != n n = n.down end
    print(io, "[")
    n = n.next
    sep = ""

    while n.key != nothing
        print(io, sep, n.key)
        if n.val != nothing print(io, ":", n.val) end
        sep = ", "
        n = n.next
    end

    print(io, "]")
end

insert!{KeyT, ValT}(s::SmultiDict{KeyT, ValT}, key::KeyT, val::ValT) = begin
    n = s.root
    pnn = nothing

    while true
        n = n.next

        while n.key != nothing && isless(n.key, key)
            n = n.next
        end

        if n.key == key 
            n.val = val
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

    return true
end

testSmultiDict() = begin
    s = SmultiDict{Int, Void}(8)
    insert!(s, 1, nothing)
    insert!(s, 3, nothing)
    insert!(s, 2, nothing)
    insert!(s, 5, nothing)
    insert!(s, 4, nothing)
    println("set: $s")
end

end
