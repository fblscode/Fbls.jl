typealias HashSlot{K, V} SkipMap{K, V}
typealias HashMapVals{K, V} Dict{UInt64, HashSlot{K, V}}

type HashMap{K, V} <: Map{K, V}
    length::Int
    levels::Int
    slotcount::Int
    slots::HashMapVals{K, V}
    
    HashMap(sc::Int, ls::Int) = begin
        m = new()
        m.length = 0
        m.levels = ls
        m.slotcount = sc
        m.slots = HashMapVals{K, V}()
        return m
    end
end

delete!{K, V}(m::HashMap{K, V}, key::K, val = nothing) = begin
    i = getslotindex(m, key)
    s = get(m.slots, i, nothing)
    if s == nothing return 0 end
    res = delete!(s, key, val)
    m.length -= res
    return res
end

@inline empty!{K, V}(m::HashMap{K, V}) = begin
    empty!(m.slots)
    m.length = 0
end

@inline getslotindex{K, V}(m::HashMap{K, V}, key::K) = hash(key) % m.slotcount

getindex{K, V}(m::HashMap{K, V}, key::K) = begin
    i = getslotindex(m, key)
    s = get(m.slots, i, nothing)
    if s == nothing throw(KeyError(key)) end
    return s[key]
end

haskey{K, V}(m::HashMap{K, V}, key::K) = begin
    i = getslotindex(m, key)
    s = get(m.slots, i, nothing)
    if s == nothing return false end
    return haskey(s, key)
end

insert!{K, V}(m::HashMap{K, V}, key::K, val::V; multi=false, update=false) = begin
    i = getslotindex(m, key)
    s = get(m.slots, i, nothing)

    if s == nothing 
        s = HashSlot{K, V}(m.levels) 
        m.slots[i] = s
    end

    res = insert!(s, key, val, multi=multi, update=update)
    if res == val m.length += 1 end
    return res
end

@inline setindex!{K, V}(m::HashMap{K, V}, val::V, key::K) =
    insert!(m, key, val, update=true)

@inline isempty{K, V}(m::HashMap{K, V}) = m.length == 0

@inline length{K, V}(m::HashMap{K, V}) = m.length
