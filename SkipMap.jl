typealias SkipKV{K, V} Union{Void, Pair{K, V}}

type SkipNode{K, V}
    down::SkipNode{K, V}
    next::SkipNode{K, V}
    kv::SkipKV{K, V}
    prev::SkipNode{K, V}
    up::SkipNode{K, V}

    SkipNode() = begin
        n = new()
        n.kv = nothing
        n.prev = n
        n.next = n
        n.down = n
        n.up = n
        return n
    end
    
    SkipNode(kv::Pair{K, V}, prev::SkipNode{K, V}) = begin
        n = new()
        n.kv = kv
        n.prev = prev
        n.next = prev.next
        prev.next.prev = n
        prev.next = n
        n.down = n
        n.up = n
        return n
    end
end

type SkipMap{K, V} <: Map{K, V}
    bottom::SkipNode{K, V}
    length::Int
    levels::Int
    top::SkipNode{K, V}

    SkipMap(levels::Int) = begin
        s = new()
        s.levels = levels
        s.top = SkipNode{K, V}()
        n = s.top

        for i in 1:levels-1
            n.down = SkipNode{K, V}()
            n.down.up = n
            n = n.down
        end

        s.bottom = n
        s.length = 0
        return s
    end
end

delete!{K, V}(s::SkipMap{K, V}, key::K, val::Any = nothing) = begin
    n = findnode(s, key)
    if !n.second return 0 end
    n = n.first
    cnt = 0

    while true
        if val == nothing || n.kv.second == val
            n.prev.next = n.next
            n.next.prev = n.prev
            cnt += 1
        end
        
        n = n.next
        if n.kv == nothing || n.kv.first != key break end
    end

    s.length -= cnt
    return cnt
end

empty!{K, V}(s::SkipMap{K, V}) = begin
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

@inline first{K, V}(s::SkipMap{K, V}) = s.bottom.next.kv

findnode{K, V}(s::SkipMap{K, V}, key::K) = begin
    if s.bottom.prev != s.bottom && isless(s.bottom.prev.kv.first, key)
        return s.bottom => false
    end

    n = s.top
    maxsteps = 1
    steps = 1
    pn = nothing
    
    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key) 
            if steps == maxsteps && pn != nothing
                nn = SkipNode{K, V}(n.kv, pn)
                nn.down = n
                n.up = nn
                pn = nn
                steps = 0
            end

            n = n.next 
            steps += 1
        end

        if n.kv != nothing && n.kv.first == key return n => true end

        pn = n.prev
        if pn.down == pn break end
        n = pn.down
        steps = 1
        maxsteps += 1
    end

    return n => false
end

@inline getindex{K, V}(s::SkipMap{K, V}, key::K) = begin
    n = findnode(s, key)
    if n.second return n.first.kv.second end
    throw(KeyError(key))
end

@inline haskey{K, V}(s::SkipMap{K, V}, key::K) = findnode(s, key).second

insert!{K, V}(s::SkipMap{K, V}, key::K, val::V; 
              multi=false, update=false) = begin
    n = findnode(s, key)

    if !multi && n.second 
        if update 
            n.first.kv = key => val 
            return nothing
        end
        
        return n.first.kv.second
    end
                  
    SkipNode{K, V}(Pair{K, V}(key, val), n.first.prev)
    s.length += 1
    return val
end

@inline isempty{K, V}(s::SkipMap{K, V}) = s.length == 0

@inline last{K, V}(s::SkipMap{K, V}) = s.bottom.prev.kv

@inline length{K, V}(s::SkipMap{K, V}) = s.length

@inline setindex!{K, V}(s::SkipMap{K, V}, val::V, key::K) = 
    insert!(s, key, val, update=true)

show{K, V}(io::IO, n::SkipNode{K, V}) = begin
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

show{K, V}(io::IO, s::SkipMap{K, V}) = begin
    n = s.top
    pn = nothing

    while n != pn 
        show(io, n)
        println(io)
        pn = n
        n = n.down 
    end
end
