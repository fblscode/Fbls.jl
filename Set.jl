module Fbls

import Base: cmp, show

type SetNode{KeyT, ValT}
    prev::SetNode{KeyT, ValT}
    next::SetNode{KeyT, ValT}
    down::SetNode{KeyT, ValT}
    val

    SetNode() = begin
        n = new()
        n.val = nothing
        n.prev = n
        n.next = n
        n.down = n
        return n
    end
    
    SetNode(val::ValT, prev::SetNode{KeyT, ValT}) = begin
        n = new()
        n.val = val
        n.prev = prev
        n.next = prev.next
        n.down = n
        return n
    end
end

immutable Set{KeyT, ValT}
    cmp::Function
    root::SetNode{KeyT, ValT}

    Set(levels::Int;cmp=Base.cmp) = begin
        s = new(cmp, SetNode{KeyT, ValT}())
        n = s.root
        for i in 1:levels-1
            n.down = SetNode{KeyT, ValT}() 
            n = n.down
        end
        return s
    end
end

show{KeyT, ValT}(io::IO, s::Set{KeyT, ValT}) = begin
    n = s.root
    while n.down != n n = n.down end
    print(io, "[")
    n = n.next
    sep = ""

    while n.val != nothing
        print(io, sep, n.val)
        sep = ", "
        n = n.next
    end

    print(io, "]")
end

insert!{KeyT, ValT}(s::Set{KeyT, ValT}, key::KeyT, val::ValT) = begin
    n = s.root
    pnn = nothing

    while true
        n = n.next

        while n.val != nothing && s.cmp(key, n.val) > 0
            n = n.next
        end

        if n.val != nothing && s.cmp(key, n.val) == 0 return false end

        nn = SetNode{KeyT, ValT}(val, n.prev)
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

testSet() = begin
    s = Set{Int, Int}(8)
    insert!(s, 1, 1)
    insert!(s, 3, 3)
    insert!(s, 2, 2)
    insert!(s, 5, 5)
    insert!(s, 4, 4)
    println("set: $s")
end

end
