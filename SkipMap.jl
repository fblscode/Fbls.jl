type SkipNode{K, V}
    down::SkipNode{K, V}
    next::SkipNode{K, V}
    kv::Nullable{Pair{K, V}}
    prev::SkipNode{K, V}
    up::SkipNode{K, V}

    SkipNode() = begin
        n = new()
        n.kv = Nullable{Pair{K, V}}()
        n.prev = n
        n.next = n
        n.down = n
        n.up = n
        return n
    end
    
    SkipNode(kv::Pair{K, V}, prev::SkipNode{K, V}) = begin
        n = new()
        n.kv = Nullable{Pair{K, V}}(kv)
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

delete!{K, V}(s::SkipMap{K, V}, 
              key::K, val::Nullable{V} = Nullable{V}()) = begin
    n = findnode(s, key)
    if !n.second return 0 end
    n = n.first
    cnt = 0

    while true
        if isnull(val) || get(n.kv).second == get(val)
            n.prev.next = n.next
            n.next.prev = n.prev
            cnt += 1
        end
        
        n = n.next
        if isnull(n.kv) || get(n.kv).first != key break end
    end

    s.length -= cnt
    return cnt
end

@inline delete!{K, V}(s::SkipMap{K, V}, key::K, val::V) = 
    delete!(s, key, Nullable{V}(val))

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

@inline first{K, V}(s::SkipMap{K, V}) = get(s.bottom.next.kv)

findnode{K, V}(s::SkipMap{K, V}, key::K) = begin
    if s.bottom.prev != s.bottom && isless(get(s.bottom.prev.kv).first, key)
        return s.bottom => false
    end

    n = s.top
    maxsteps = 1
    steps = 1
    pn = nothing
    
    while true
        n = n.next

        while !isnull(n.kv) && isless(get(n.kv).first, key) 
            if steps == maxsteps && pn != nothing
                nn = SkipNode{K, V}(get(n.kv), pn)
                nn.down = n
                n.up = nn
                pn = nn
                steps = 0
            end

            n = n.next 
            steps += 1
        end

        if !isnull(n.kv) && get(n.kv).first == key return n => true end

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
    if n.second return get(n.first.kv).second end
    throw(KeyError(key))
end

@inline haskey{K, V}(s::SkipMap{K, V}, key::K) = findnode(s, key).second

insert!{K, V}(s::SkipMap{K, V}, key::K, val::V; 
              multi::Bool = false, update::Bool = false) = begin
    n = findnode(s, key)

    if !multi && n.second 
        if update 
            n.first.kv = Nullable{Pair{K, V}}(key => val)
            return nothing
        end
        
        return get(n.first.kv).second
    end
                  
    SkipNode{K, V}(Pair{K, V}(key, val), n.first.prev)
    s.length += 1
    return val
end

@inline isempty{K, V}(s::SkipMap{K, V}) = s.length == 0

@inline last{K, V}(s::SkipMap{K, V}) = get(s.bottom.prev.kv)

@inline length{K, V}(s::SkipMap{K, V}) = s.length

@inline setindex!{K, V}(s::SkipMap{K, V}, val::V, key::K) = 
    insert!(s, key, val, update=true)

show{K, V}(io::IO, n::SkipNode{K, V}) = begin
    print(io, "[")
    n = n.next
    sep = ""

    while !isnull(n.kv)
        print(io, sep, get(n.kv).first)
        if get(n.kv).second != nothing print(io, ":", get(n.kv).second) end
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
