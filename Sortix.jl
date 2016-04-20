abstract Sortix{KeyT <: Tuple}

typealias SortixRecs{KeyT} Smulti{KeyT, RecId}

immutable BasicSortix{KeyT} <: Sortix{KeyT}
    name::Symbol
    key::Tuple
    isunique::Bool
    recs::SortixRecs
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicSortix(n::Symbol, key::Tuple, isunique::Bool, levels::Int) =
        new(n, key, isunique,
            SortixRecs{KeyT}(levels),
            Evt{Tuple{RecId}}(),
            Evt{Tuple{Rec}}(),
            Evt{Tuple{Rec}}())
end

Sortix(KeyT, n::Symbol, key::Tuple; isunique=false, levels=4) = 
    BasicSortix{KeyT}(n, key, isunique, levels)

defname{KeyT}(sx::Sortix{KeyT}) = BasicSortix{KeyT}(sx).name

insert!{KeyT}(sx::Sortix{KeyT}, rec::Rec) = begin
    id = rec[recid]

    if insert!(sx.recs, map((c) -> rec[c], sx.key), id; 
               multi = !sx.isunique) != id
        throw(DupKey())
    end
    
    return rec
end

testSortix() = begin
    col = Col(Str, :foo)
    sx = Sortix(Tuple{Str}, :bar, (col,); isunique=true)
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
