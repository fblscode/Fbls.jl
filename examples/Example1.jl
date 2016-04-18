module Examples

push!(LOAD_PATH, "..")

import Base: IOBuffer, seekstart
import Fbls: Cx, BasicCol, RecCol, RecOf, Tbl, dump, get, haskey, insert!, isempty, length, load!, pushcol!, recid

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

    # Records are initialized with globally unique ids on first insert
    @assert get(bars, recid(brec), cx) == brec    

    frec = insert!(foos, RecOf(foo => "abc", foobar => brec), cx)
    @assert length(foos) == 1
    @assert haskey(foos, recid(frec), cx)

    # Records are really just Dicts mapping fields to values
    @assert frec[foobar] == brec

    # Tables can be dumped to and loaded from any IO stream
    buf = IOBuffer()
    dump(foos, buf)
    empty!(foos)
    seekstart(buf)
    load!(foos, buf, cx)
    @assert length(foos) == 1
    @assert haskey(foos, recid(frec), cx)
end

end
