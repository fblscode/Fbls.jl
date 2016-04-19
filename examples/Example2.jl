module Examples

push!(LOAD_PATH, "..")

import Base: IOBuffer, seekstart
import Fbls: Col, IO, RecOf, Revix, Tbl, get, isempty, load!, offs, pushcol!, pushdep!, recid, upsert!

runExample2() = begin
    out = IOBuffer()

    # IO tables log all writes to the specified stream
    tbl = IO(Tbl(:foobars), out)
    col = Col(AbstractString, :bar)
    pushcol!(tbl, col)

    # Let's add an in-memory reverse index on the offset column
    # IO records are tagged with current offset on upsert!
    rix = Revix(:foobar_offs, offs(tbl))

    # pushdep!() registers rix to receive events for all updates to tbl
    pushdep!(tbl, rix)

    rec = upsert!(tbl, RecOf(col => "abc"))

    # Just checking to make sure the offset made it all the way
    @assert get(rix, rec[recid]) == rec[offs(tbl)]

    # Drop all records from table and reload via offset index
    empty!(tbl)
    @assert get(tbl, rix, rec[recid])[col] == "abc"

    # Deletes are also logged to stream
    delete!(tbl, rec[recid])
    empty!(tbl)
    seekstart(out)
    load!(tbl, out)
    @assert isempty(tbl)
end

end
