abstract Sortix{KeyT <: Tuple}

typealias SortixRecs{KeyT} Smulti{KeyT, RecId}

immutable BasicSortix{KeyT} <: Sortix{KeyT}
    name::Symbol
    key::KeyT
    recs::SortixRecs
    ondelete::Evt{Tuple{RecId}} 
    onload::Evt{Tuple{Rec}} 
    onupsert::Evt{Tuple{Rec}} 

    BasicSortix(n::Symbol, key::KeyT, levels::Int) = 
        new(n, key,
            SortixRecs{KeyT}(levels), 
            Evt{Tuple{RecId}}(),
            Evt{Tuple{Rec}}(),
            Evt{Tuple{Rec}}())
end

Sortix{KeyT}(n::Symbol, key::KeyT; levels=4) = 
    BasicSortix{KeyT}(n, key, levels)

defname(sx::Sortix) = BasicSortix(sx).name

testSortix() = begin
    col = Col{Str}(:foo)
    sx = Sortix(:bar, (col,))
end
