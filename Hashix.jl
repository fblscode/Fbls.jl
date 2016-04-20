abstract Hashix{K} <: Mapix{K}

typealias HashixRecs{K} HashMap{K, RecId}

immutable BasicHashix{K} <: Hashix{K}
    name::Symbol
    key::Tuple
    isunique::Bool
    recs::HashixRecs
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicHashix(n::Symbol, key::Tuple, isunique::Bool, 
                slotcount::Int,  levels::Int) =
        new(n, key, isunique,
            HashixRecs{K}(slotcount, levels),
            Evt{Tuple{RecId}}(),
            Evt{Tuple{Rec}}(),
            Evt{Tuple{Rec}}())
end

Hashix(K, n::Symbol, key::Tuple, slotcount::Int, levels::Int; isunique=false) = 
    BasicHashix{K}(n, key, isunique, slotcount, levels)

defname{K}(sx::Hashix{K}) = BasicHashix{K}(sx).name

insert!{K}(sx::Hashix{K}, rec::Rec) = begin
    id = rec[recid]

    if insert!(sx.recs, map((c) -> rec[c], sx.key), id; 
               multi = !sx.isunique) != id
        throw(DupKey())
    end
    
    return rec
end

testHashix() = begin
    col = Col(Str, :foo)
    sx = Hashix(Tuple{Str}, :bar, (col,), 10, 3; isunique=true)
    rec = RecOf(col => "abc")
    insert!(sx, rec)
    insert!(sx, rec)

    try
        insert!(sx, RecOf(col => "abc"))
        @assert false
    catch e
        @assert isa(e, DupKey)
    end
end
