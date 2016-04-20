runHashix() = begin
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
