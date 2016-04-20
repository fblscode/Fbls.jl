abstract Skipix{K} <: Mapix{K}

typealias SkipixRecs{K} SkipMap{K, RecId}

immutable BasicSkipix{K} <: Skipix{K}
    name::Symbol
    key::Tuple
    isunique::Bool
    recs::SkipixRecs
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicSkipix(n::Symbol, key::Tuple, isunique::Bool, levels::Int) =
        new(n, key, isunique,
            SkipixRecs{K}(levels),
            Evt{Tuple{RecId}}(),
            Evt{Tuple{Rec}}(),
            Evt{Tuple{Rec}}())
end

Skipix(K, n::Symbol, key::Tuple, levels::Int; isunique=false) = 
    BasicSkipix{K}(n, key, isunique, levels)

defname{K}(sx::Skipix{K}) = BasicSkipix{K}(sx).name

insert!{K}(sx::Skipix{K}, rec::Rec) = begin
    id = rec[recid]

    if insert!(sx.recs, map((c) -> rec[c], sx.key), id; 
               multi = !sx.isunique) != id
        throw(DupKey())
    end
    
    return rec
end
