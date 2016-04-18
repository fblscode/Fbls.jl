module Examples

push!(LOAD_PATH, "..")

import Fbls: Cx, BasicCol, RecCol, RecOf, Tbl, haskey, insert!, length, pushcol!, recid

runExample1() = begin
    # All data ops require a context
    cx = Cx()

    bars = Tbl(:bars)
    bar = BasicCol{Int}(:bar)

    # Columns are added to tables using pushcol!
    pushcol!(bars, bar)

    foos = Tbl(:foos)
    foo = BasicCol{AbstractString}(:foo)

    # RecCols reference other records
    foobar = RecCol(:foobar, bars)
    pushcol!(foos, foo, foobar)

    # RecOf() is a shortcut to create filled records
    brec = insert!(bars, RecOf(bar => 42), cx)
    @assert !isempty(bars)

    # Records are initialized with globally unique id on first insert
    @assert haskey(bars, recid(brec), cx)

    frec = insert!(foos, RecOf(foo => "abc", foobar => brec), cx)
    @assert length(foos) == 1

    # Records are implemented as regular Dicts
    @assert frec[foobar] == brec
end

end
