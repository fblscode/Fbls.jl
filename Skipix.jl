abstract Skipix{KeyT <: Tuple}

typealias SkipixRecs{KeyT} SkipMap{KeyT, RecId}

immutable BasicSkipix{KeyT} <: Skipix{KeyT}
    name::Symbol
    key::Tuple
    isunique::Bool
    recs::SkipixRecs
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicSkipix(n::Symbol, key::Tuple, isunique::Bool, levels::Int) =
        new(n, key, isunique,
            SkipixRecs{KeyT}(levels),
            Evt{Tuple{RecId}}(),
            Evt{Tuple{Rec}}(),
            Evt{Tuple{Rec}}())
end

Skipix(KeyT, n::Symbol, key::Tuple; isunique=false, levels=4) = 
    BasicSkipix{KeyT}(n, key, isunique, levels)

defname{KeyT}(sx::Skipix{KeyT}) = BasicSkipix{KeyT}(sx).name

insert!{KeyT}(sx::Skipix{KeyT}, rec::Rec) = begin
    id = rec[recid]

    if insert!(sx.recs, map((c) -> rec[c], sx.key), id; 
               multi = !sx.isunique) != id
        throw(DupKey())
    end
    
    return rec
end

testSkipix() = begin
    col = Col(Str, :foo)
    sx = Skipix(Tuple{Str}, :bar, (col,); isunique=true)
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
