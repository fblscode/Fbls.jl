# fbls
### a simple but flexible Julia DB

## the biggish picture
fbls is an attempt at adding more degrees of freedom to the database paradigm, it's an itch I've been scratching for a while now. I'm all for standards as interfaces and theory as a starting point; but I'm drawing a line in the sand right here, the madness has to stop. All I ever really cared about was the ability to store, index and retrieve my data. I'm sick and tired of query languages, constraints and limitations; of being forced to bend ideas backwards around hairy solutions to someone else's problems, just to gain basic persistence.

## status
fbls is currently catching it's breath somewhere between crazy idea and working prototype. It represents my first major Julia project and I'm still feeling my way around the language. Basic testing is in place and the examples in this document should work as advertised.

## future
Transactions, indexing and encryption; most probably in that order.

## columns
Columns, like pointers, don't do much by themselves besides referencing data that's stored elsewhere. In fbls, columns are statically typed, meaning they only reference values of a specified type. Any Julia type will do, as long as it has a canonical read/write implementation or is hooked into fbls type conversion facility.

## records
Records maps fields to values. Fields usually belong to columns, but custom fields can be created by simply instantiating the Fld type. Two columns can even share the same field by aliasing. Fields are immutable and globally unique.

## tables
Tables map globally unique ids to records. Each table contains a set of columns that all records are projected onto. This means that even though a record can contain values from multiple tables, any specific table will only store values mapped to it's own set of columns. 
