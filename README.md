# fbls
### a simple but flexible Julia DB

## the biggish picture
fbls is an attempt at adding more degrees of freedom to the database paradigm, it's an itch I've been scratching for a while now. I'm all for standards as interfaces and theory as a starting point; but I'm drawing a line in the sand right here, the madness has to stop. All I ever really cared about was the ability to store, index and retrieve my data. I'm sick and tired of query languages, constraints and limitations; of being forced to bend ideas backwards around hairy solutions to someone else's problems, just to gain basic persistence.

## status
fbls is currently catching it's breath somewhere between crazy idea and working prototype. It represents my first major Julia project and I'm still feeling my way around the language. Basic testing is in place and the examples in this document should work as advertised.

## future
Transactions, hash/ordered indexes and encryption; probably in that order.

## columns
Columns, like pointers, don't do much by themselves besides referencing data that's stored elsewhere. In fbls, columns are statically typed, meaning they only reference values of a specified type. Julia types with canonical read/write implementations work out of the box, a type conversion facility is provided for anything else.

## records
Records maps fields to values. Fields usually represent columns, but adding custom fields is trivial. Fields are immutable and globally unique. Two columns can share the same field by aliasing.

## tables
Tables map globally unique ids to records. Each table contains a set of columns that all records are projected onto. This means that even though a record can contain values from multiple tables, any specific table will only store values mapped to it's own set of columns. 

## reverse indexes
A reverse index maps record ids to values for a specific column. Combining a reverse offset index with an IO table is a nice trick to enable lazy loading of records.

## async events
Tables and indexes have load, insert and delete events that can be hooked into. All events are logged in the current context and pumped asynchronously by calling doevts!.

## wrap on, wrap off
fbls uses wrapping extensively to allow arbitrary combinations of functionality. Any table can be wrapped by an IO table to add stream logging, any number of layers can be wrapped on top; and all the pieces are still accessible in their original state, the initial table reference still knows nothing about IO. Any column can be made temporary, the same column can even be temporary in one table and persistent in another.