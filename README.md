# dcp - Like cp, but for structured data

dcp is a python library and command line tool for copying datasets efficiently
across formats and storages while preserving structure and logical data types,
and smoothly handling conversion issues. It uses Semantic Schemas and Apache
Arrow under the hood as the "lingua franca" of data structure and format, and
can copy data between dozens of data formats (JSON, CSV, database table, pandas,
arrow, parquet, etc) on many different storage engines (postgres, S3, local
file, python memory, etc) as efficiently and with as high fidelity as the formats
and engines allow.

Copying data between formats and storages is a many-step process loaded
with pitfalls and gotchas, dcp handles these challenges for you and gives you
control over how to deal with type errors, truncations, and downcasts.

In addition, dcp supports other related operations
like inferring the schema of a dataset, conforming a dataset to a schema, and
creating empty objects of a specified schema.

`pip install dcp` or `poetry add dcp`

Command line quick usage:

`dcp orders.csv mysql://localhost:3306/mydb`

This command will load the `orders.csv` file into a mysql table (by default of the same name `orders`)
on the given database, inferring the right schema from the data in the CSV.

A more complex transfer:

`dcp -n orders -s mysql://localhost:3306/mydb --to s3://mybucket.s3/pth --to-name=orders.csv`

This will export your `orders` table to a file on S3 (in the "default" format for
the StorageEngine since none was specified, in the case of S3 a CSV).

## Usage

The python API gives you more powerful tools for more complex operations:

```python

records = [{"f1":"hello", "f2": "world"}]
fields = dcp.infer_fields(records)
print(fields)
# >>> [Field(name="f1", type=Text), Field(name="f2", type=Text)]

dcp.copy(
    from_obj=records,
    to_name='records',
    to_format="csv",
    to_storage='file:///tmp/dcp'
)

assert Storage('file:///tmp/dcp').get_api().exists('records')
with Storage('file:///tmp/dcp').get_api().open('records') as f:
    print(f.read())
    # >>> f1,f2
    # >>> hello,world

dcp.copy(
    from_name='records',
    from_storage='file:///tmp/dcp/',
    to_format='table',
    to_storage='postgres://localhost:5432/mydb'
)

data_format = dcp.infer_format("records", storage='file:///tmp/dcp')
print(data_format)
# >>> CsvFileFormat

dcp.copy(
    from_name='records',
    from_storage='file:///tmp/dcp/',
    to_format='table',
    to_storage='postgres://localhost:5432/mydb'
    fields=fields,
    cast_level='strict',
)

assert Storage('postgres://localhost:5432/mydb').get_api().exists("records")
```

dcp gives you control over

### How it works

Copying data with dcp involves the following steps:

- Find the lowest cost conversion path for the given storages and formats
- For each conversion:
  - load data from source storage into memory if not already there
  - convert in-memory bytes to format that can be worked on if not already
  - potentially, put data on storage in temporary format for conversion
    (eg in case of database table, we use the databases own query language to do conversion)
  - convert in-memory bytes to destination format
    - create empty structure with correct schema on storage
    - fill structure with data
    - user can specify cast behavior:
      - default (let the storage engine do what it does by default with given values)
      - force (do everything possible to coerce to an acceptable value)
    - handle errors
      - due to type-value mismatch (text in a decimal field)
      - user can specify behavior on error: fail, relax field type, or force null
      - may require re-running some or all of conversion process
  - store data back onto destination storage in final format
  - return results of the copy

These steps are handled case-by-case for each storage engine and data format. For
copying a postgres table to mysql, for instance, how this is done depends on the
fields supplied and their respective support.

# Adding your own Storage Engine or Data Format

dcp can easily be extended with storage engines or formats:

```python

class RedisStorageEngine(StorageEngine):
    scheme = "redis"
    api = RedisStorageApi

class RedisStorageApi(StorageApi):
    def exists():...
    def exists():...
    def exists():...
    def exists():...

dcp.add_storage_engine(RedisStorageEngine)
```
