module Fbls

import Base: cmp, show

type SetNode{KeyT, ValT}
    next::Union{Void, SetNode{KeyT, ValT}}
    down::Union{Void, SetNode{KeyT, ValT}}
    val::ValT

    SetNode() = new(nothing, nothing)
    
    SetNode(val::ValT,
            next::Union{Void, SetNode{KeyT, ValT}},
            down::Union{Void, SetNode{KeyT, ValT}}) = new(next, down, val)
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
    while n.down != nothing n = n.down end
    print(io, "[")
    n = n.next
    sep = ""
    while n != nothing
        print(io, sep, n.val)
        sep = ", "
        n = n.next
    end
    print(io, "]")
end

insert!{KeyT, ValT}(s::Set{KeyT, ValT}, key::KeyT, val::ValT) = begin
    prev = s.root

    nn = nothing

    while prev != nothing
        n = prev.next

        while n != nothing && s.cmp(key, n.val) > 0
            prev = n
            n = n.next
        end

        if n != nothing && s.cmp(key, n.val) == 0 return false end

        nn = SetNode{KeyT, ValT}(val, prev, nn)
        prev = prev.down
    end

    pnn = nothing
    while nn != nothing
        prev = nn.next
        nn.next = prev.next
        prev.next = nn

        up = nn.down
        nn.down = pnn
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
