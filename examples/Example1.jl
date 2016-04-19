module Examples

push!(LOAD_PATH, "..")

import Base: IOBuffer, seekstart
import Fbls: BasicCol, Col, RecCol, RecOf, Tbl, dump, get, haskey, isempty, 
length, load!, pushcol!, recid, upsert!

runExample1() = begin
    bars = Tbl(:bars)
    bar = Col(Int, :bar)

    # Columns are added to tables using pushcol!
    pushcol!(bars, bar)

    foos = Tbl(:foos)
    foo = Col(AbstractString, :foo)

    # RecCols reference other records
    foobar = RecCol(:foobar, bars)
    pushcol!(foos, foo, foobar)

    # RecOf() is a shortcut to create filled records
    brec = upsert!(bars, RecOf(bar => 42))
    @assert !isempty(bars)

    # Records are initialized with globally unique ids on first upsert
    @assert get(bars, recid(brec)) == brec    

    frec = upsert!(foos, RecOf(foo => "abc", foobar => brec))
    @assert length(foos) == 1
    @assert haskey(foos, recid(frec))

    # Records are really just Dicts mapping fields to values
    @assert frec[foobar] == brec

    

    # Tables can be dumped to and loaded from any IO stream
    buf = IOBuffer()
    dump(foos, buf)
    empty!(foos)
    seekstart(buf)
    load!(foos, buf)
    @assert length(foos) == 1
    @assert haskey(foos, recid(frec))
end

end
