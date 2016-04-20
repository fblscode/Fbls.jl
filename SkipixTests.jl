runSkipix() = begin
    col = Col(Str, :foo)
    sx = Skipix(Tuple{Str}, :bar, (col,), 3; isunique=true)
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
