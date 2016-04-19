# Fbls.jl
### a simple but flexible Julia DB

```julia

import Base: IOBuffer, seekstart
import Fbls: Col, RecCol, RecOf, Tbl, dump, get, haskey, isdirty, isempty, 
length, load!, pushcol!, recid, revision, upsert!

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
    @assert get(bars, brec[recid]) == brec    

    frec = upsert!(foos, RecOf(foo => "abc", foobar => brec))
    @assert length(foos) == 1
    @assert haskey(foos, frec[recid])

    # Records are really just Dicts mapping fields to values
    @assert frec[foobar] == brec

    brec[bar] = 43

    # isdirty() returns true if any specified column has been modified
    @assert isdirty(brec, bars, bar)

    upsert!(bars, brec)

    # calling isdirty() without specifying columns checks all columns in table
    @assert !isdirty(brec, bars)

    # record revision is increased on each upsert!
    @assert brec[revision(bars)] == 2

    # Tables can be dumped to and loaded from any IO stream
    buf = IOBuffer()
    dump(foos, buf)
    empty!(foos)
    seekstart(buf)
    load!(foos, buf)
    @assert length(foos) == 1
    @assert haskey(foos, frec[recid])
end

```

## what's up
Fbls is an attempt at adding more degrees of freedom to the database paradigm, it's an itch I've been scratching for a while now. I'm all for standards as interfaces and theory as a starting point; but I'm drawing a line in the sand right here, the madness has to stop. All I ever really cared about was the ability to store, index and retrieve my data. I'm sick and tired of having feature check lists, academia and arbitrary limitations shoved down my throat; of being forced to bend ideas backwards around hairy solutions to the wrong problem, just to gain basic persistence.

## status
Fbls is currently catching it's breath somewhere between crazy idea and working prototype. It's my first major Julia project and I'm still feeling my way around the language. Basic testing is in place and the examples in this document should work as advertised.

## future
Transactions, hash/ordered indexes and encryption; probably in that order.

## columns
Columns, like pointers, don't do much by themselves besides referencing data that's stored elsewhere. In Fbls, columns are statically typed, meaning they only reference values of a specified type. Julia types with canonical read/write implementations work out of the box, a type conversion facility is provided for anything else.

## records
Records maps fields to values. Fields usually represent columns, but adding custom fields is trivial. Fields are immutable and globally unique. Two columns can share the same field by aliasing.

## tables
Tables map globally unique ids to records. Each table contains a set of columns that all records are projected onto. This means that even though a record can contain values from multiple tables, any specific table will only store values mapped to it's own set of columns. 

## reverse indexes
A reverse index maps record ids to values for a specific column. Combining a reverse offset index with an IO table is a nice trick to enable lazy loading of records.

## events
Tables and indexes have delete, load and upsert events that can be hooked into. Events are triggered before the triggering action is carried out, which allows for accessing previous state and validating new data. pushdep! is provided as a shortcut to hook into all events.

## wrap on, wrap off
Fbls uses wrapping extensively to enable arbitrary combinations of functionality. Any table can be wrapped by an IO table to add stream logging, any number of layers can be wrapped on top; and all the pieces are still accessible in their original state, the initial table reference still knows nothing about IO. Any column can be made temporary, the same column can even be temporary in one table and persistent in another.

```julia

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

    # Drop all records from tbl and reload via offset index
    empty!(tbl)
    @assert get(tbl, rix, rec[recid])[col] == "abc"

    # Deletes are also logged
    delete!(tbl, rec[recid])
    empty!(tbl)
    seekstart(out)
    load!(tbl, out)
    @assert isempty(tbl)
end

```