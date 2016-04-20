type SkipNode{K, V}
    down::SkipNode{K, V}
    next::SkipNode{K, V}
    kv::Any
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
    
    SkipNode(key::K, val::V, prev::SkipNode{K, V}) = begin
        n = new()
        n.kv = Pair{K, V}(key, val)
        n.prev = prev
        n.next = prev.next
        n.down = n
        n.up = n
        return n
    end
end

type SkipMap{K, V} <: Map{K, V}
    bottom::SkipNode{K, V}
    length::Int
    levels::Rational
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

delete!{K, V}(s::SkipMap{K, V}, key::K, val = nothing) = begin
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

first{K, V}(s::SkipMap{K, V}) = s.bottom.next.kv

findnode{K, V}(s::SkipMap{K, V}, key::K) = begin
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

getindex{K, V}(s::SkipMap{K, V}, key::K) = begin
    n = findnode(s, key)
    if n != nothing return n.kv.second end
    throw(KeyError(key))
end

haskey{K, V}(s::SkipMap{K, V}, key::K) = findnode(s, key) != nothing

insert!{K, V}(s::SkipMap{K, V}, key::K, val::V; 
              multi=false, update=false) = begin
    n = s.top
    pnn = nothing
    dprob = 1 // s.levels
    prob = dprob

    while true
        n = n.next

        while n.kv != nothing && isless(n.kv.first, key) n = n.next end

        if !multi && n.kv != nothing && n.kv.first == key 
            if update 
                n.kv = Pair{K, V}(key, val) 
                return nothing
            end

            return n.kv.second
        end

        islast = n.prev.down == n.prev

        if islast || rand() < prob
            nn = SkipNode{K, V}(key, val, n.prev)
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
    return val
end

isempty{K, V}(s::SkipMap{K, V}) = s.length == 0

last{K, V}(s::SkipMap{K, V}) = s.bottom.prev.kv

length{K, V}(s::SkipMap{K, V}) = s.length

setindex!{K, V}(s::SkipMap{K, V}, val::V, key::K) = 
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

randArray(a::Array{Int, 1}) = begin
    len = length(a)

    for i in 1:len
        j = rand(1:l)
        tmp = a[i]
        a[i] = a[j]
        a[j] = tmp
    end
end
