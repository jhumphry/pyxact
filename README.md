# pyxact

## Introduction

This is a Python (3.6+) project that aims to provide a different approach to interfacing with SQL
databases. It is centred around the idea of a transaction, a set of data and/or queries that share
a common context (such as a transaction id) and which has to be created, read, updated or deleted
together. Heavy use of metaclasses allows the abstract parent classes provided by the package to be
subclassed to represent the particular database tables, views and queries etc in the schema. The
classes are also able to create the underlying database objects, making it easier to keep the
Python code in sync with the SQL schema. Currently the project supports [SQLite
3](https://www.sqlite.org/index.html) but it is written in such a way that it should be easy to add
support for other SQL databases.

This project is free software (using the ISC permissive licence) and is provided with no
warranties, as set out in the file `LICENSE`.

This package is still at a very early stage of development and breaking API changes should be
expected from time to time.

## Overview of the package

Currently the best detailed documentation is in the form of heavily annotated examples under
`doc/example`, and as docstrings provided for the public methods. What follows is an overview of
the key parts of the basic design and intended usage.

### SQLField

`SQLField` (found in `pyxact.fields`) is the base class for field classes. These represent Python
values that map to values in the database. They are not used on their own, but instances are added
as class attributes to subclasses of one of the 'magic' classes in `pyxact` such as `SQLTable`.
When the attribute is accessed on the new subclass, the `SQLField` is returned and its methods and
attributes can be used. When the new subclass is instantiated, the attribute becomes a
type-enforcing slot which will only accept Python values that can map to the underlying database
value.

`SQLField` subclasses have various parameters. One is of particular note - `context_used` indicates
that the field, while still able to store values like a normal field, relates to a particular
transaction's context - the values that link the different parts of the transaction together.
`SQLField` subclass instances instantiated directly as attributes on `SQLTransaction` subclasses
define this context based on the attribute name and value. When appropriate, `SQLField` subclass
instances with `context_used` parameters set which are instantiated on `SQLTable` or `SQLQuery`
subclasses will update themselves using this context in preference to the stored value. Typical
usage involves a transaction ID, timestamp or similar fields being set on the `SQLTransaction` and
the values then being copied into each of the connected `SQLTable` instances.

Some `SQLField` can generate their own values. For example, `UTCNowTimestampField` has the ability
to set itself to the current UTC time and date. All `SQLField` can also take a `query` parameter.
This links the field with a particular `SQLQuery` representation of a database query. `SQLField`
distinguishes between the `refresh` method, which may regenerate the existing data and should be
idempotent, and the `update` method, which may generate new data and alter the database by
retrieving the next value from a sequence or fetching a fresh timestamp.

### SQLRecord

`SQLRecord` is a parent class that is intended to be subclassed to represent tuples of data from a
database. The new subclass should have `SQLField` attributes added, in order, to represent the
various columns. When the new subclass is instantiated, these attributes will be type-enforcing and
it will not be possible to write to attributes not created when the subclass was defined.

In order to minimise name collisions between the methods of `SQLRecord` and user-defined
attributes, the methods all start with `_`. The usual Python convention that suggests that these
'_'-prefixed methods are private and not for public use is not being followed - while non-standard,
this approach is similar to that used by `collections.namedtuple` to resolve a similar issue.

Most of the methods provided should be reasonably self-explanatory, or are explained by docstrings.
The `_get`, `_values` etc. methods take an optional `context` parameter. If nothing is supplied,
the values returned are those previously stored (if any). If a dictionary is supplied, field
attributes with the `context_used` parameter set may update themselves from the context, and may
update the context if appropriate.

Some methods take an optional `dialect` parameter. This can be set to a subclass of `SQLDialect`
found in `pyxact.dialects`. These subclasses are intended to isolate all of the database-specific
(and Python database adaptor-specific) code. If no `dialect` parameter is passed, the
`pyxact.dialects.DefaultDialect` is used, which as standard is equal to `sqliteDialect`, the
dialect appropriate for the builtin-in `sqlite` module in the Python standard library.

### SQLTable

`SQLTable` is an abstract subclass of `SQLRecord`. It allows for the additional parameters (such as
 `table_name`) and methods necessary to represent an SQL table. It also takes an optional `schema`
parameter, which should be an instance of `SQLSchema` found in `pyxact.schemas`. If present, the
`SQLTable` subclass will be associated with that schema object. Schema objects allow multiple
associated database objects to be created in one method. If the underlying database does not support
true user defined schema, the schema name will be used as a prefix instead.

`SQLTable` can have `SQLConstraint` attributes from `pyxact.constraints` as well as `SQLField`
attributes. These represent the constraints on the table, such as a primary key or foreign key
relationship.

Various methods on `SQLTable` instances and classes allow the generation of suitable SQL command
text, and where appropriate also return the associated data suitable to be passed into the
`execute` method of the database cursor. The `dialect` parameter again allows for any database-vendor
specific modifications.

### SQLRecordList

`SQLRecordList` is a pseudo-list that can hold multiple instances of a specified subclass of
`SQLRecord` (which includes all subclasses of `SQLTable`). Subclasses usually do not define any
attributes or methods. Attributes will be created to match the attributes on the underlying
`SQLRecord` subclass, but with the slight difference that these attributes are read-only and when
read will return not just a single value but a generator giving the field value for each of the
stored `SQLRecord` in turn.

### SQLTransaction

`SQLTransaction` is the core class. When subclassed, it can combine `SQLField`, `SQLRecord`,
`SQLTable` and `SQLRecordList` attributes and provides operations to create, read, update or delete
the database data associated with the attributes in a structured and transactional way. Some
`SQLTransaction` may be data-oriented, tying together `SQLTable` while others may be
query-oriented, tying together `SQLQueryResult`.

`SQLField` attributes on the `SQLTransaction` subclass form the context fields, the values from
which will be retrieved as a context dictionary. There are three retrieval methods: `_get_context`
retrieves the current non-`None` values (if any), `_get_refreshed_context` takes a database cursor
and an optional dialect object, and will 'refresh' the underlying `SQLField`,  and
`_get_updated_context` takes the same two parameters, and will 'update' the `SQLField`.
`get_refreshed_context` is required to be idempotent but `_get_updated_context` is not. For the
latter two methods the context dictionary is built up in the order the fields were added as
attributes to the `SQLTransaction` subclass. The order of context field attributes can be important
for fields with a `query` parameter set - the prior values stored in the context dictionary may be
needed to parametrise the query.

`_insert_new` and `_insert_existing` are the two methods that create new records in the database,
differing in whether `_get_updated_context` or `_get_context` is used to retrieve the context
dictionary. After the context dictionary is created, a database transaction is started and a
`_pre_insert_hook` method is called. On the parent `SQLTransaction` class this does nothing, but it
can be over-ridden in subclasses to carry out automatic adjustment of the data before insertion. A
typical use might involve subclassing and extending a complex transaction class to create a simpler
special-purpose class. The special-purpose class would allow only a few template fields to be
filled in, and an over-ridden `_pre_insert_hook` method would fill in the rest of the data.
Following the call to the hook method, a `_verify` method is called which can be overridden to
carry out internal consistency checks. If these checks pass, then the data in each `SQLTable` and
`SQLTable`-based `SQLRecordList` attribute is inserted into the database.

The `_update` method works in a similar manner. This time `_get_context` is always used to retrieve
the context dictionary and the hook is called `_pre_update_hook`. Note that this method can only be
used if every `SQLTable` and `SQLTable`-based `SQLRecordList` attribute has a primary key
constraint defined.

The `_context_select` method starts by using `_get_refreshed_context` to fetch the context
dictionary. Then, for each `SQLTable` and `SQLTable`-based `SQLRecordList` attribute, an attempt
will be made to retrieve the relevant records by matching up the context dictionary keys with
 `SQLField` attributes on the `SQLTable` that have a matching `context_used` parameter. After
the data is retrieved a hook method `_post_select_hook` is run. The default hook scans in each
`SQLRecord` attribute and tries to locate context-linked fields in the retrieved data, and sets the
transaction context fields to that data. This is useful if not all of the context fields were
needed to retrieve the records. Finally the `_verify` method is called and can be overridden to
check the resulting data is consistent.

The `allow_unlimited` parameter to `_context_select` indicates whether to allow the generation of
`SELECT` queries with no `WHERE` clause. While occasionally deliberate, this might more often be a
sign that there are insufficient non-`None` context fields being used to tie the records together.

For an example of how the normal use of `_context_select` might work,  consider an instance of an
`SQLTransaction` subclass with a context field called `trans_id`. Assuming this has been set to a
non-`None` value, the context dictionary created will contain a key `trans_id` set to that value.
If an `SQLTable` subclass attached as an attribute to the `SQLTransaction` subclass has a
field `foo_id` which was created with a `context_used='trans_id'` parameter then an attempt will be
made to retrieve the data for that `SQLTable` by executing a `SELECT` query with a `WHERE` clause
that looks for column `foo_id` to be equal to the value of `trans_id`.

This means that in practice a linked set of records can be retrieved inside a single database
transaction by instantiating a `SQLTransaction` subclass with no values, setting a few context
parameters and calling `_context_select` - it is easier to use than to explain.

### SQLQuery

This is a parent class that defines a parametrised SQL query. Subclasses need to provide the query
text and an `SQLRecord` type for the result. The text for the query can have parameters designated
by braces like so: `{alpha}`. In that case, the new subclass is expected to have an SQLField
attribute named `alpha`. The value of that field on instances of the new subclass will be used to
replace `{alpha}` when the instance's `_execute` method is called (of course, this uses a
parametrised database call and not simple string substitution which would be vulnerable to SQL
injection). `SQLQuery` provides several methods for retrieving the results once the query has been
executed.

### SQLQueryResult

It is also possible to subclass `SQLQueryResult`, which is a subclass of `SQLRecordList`. The
subclass of `SQLQueryResult` needs to specify an `SQLQuery` rather than an `SQLRecord`, as the base
`SQLRecord` for the `SQLQueryResult` will be picked up as the `SQLRecord` used for the `SQLQuery`
result. Instances of a `SQLQueryResult` subclass will have an instance of its associated `SQLQuery`
stored as the parameter `_query` so that the context fields can be set. The `_refresh` method on
the `SQLQueryResult` subclass will clear out and retrieve the associated records.

