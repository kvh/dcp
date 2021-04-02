![dcp](https://github.com/kvh/dcp/workflows/dcp/badge.svg)

<p>&nbsp;</p>
<p align="center">
  <img width="200" src="assets/dcp.svg">
</p>
<h3 align="center">cp for structured data</h3>
<p>&nbsp;</p>

dcp is a python library and command line tool that provides
a **fast** and **_safe_** way to copy structured data between any two points,
whether copying a CSV to a Mysql table or an in-memory DataFrame to an S3
JSONL file.

### _Fast_

To copy data most efficiently, dcp uses best-in-class
underlying client libraries, employs parallelization and
compression to the extent possible, and
estimates the memory, cpu, and wire
costs of any copy operation to select the _lowest cost copy path_
for available storage engines.

### _Safe_

dcp uses Semantic Schemas under the hood as the "lingua franca" of
structured data, allowing for careful preservation of data types and
values across many formats and storage engines. Error handling behavior
is configurable so when type conversion errors are encountered -- a
value is truncated or cannot be cast -- **dcp can fail, relax the datatype,
or set the value null depending on what the user wants**.

**Currently supported formats:**

- JSON
- CSV file
- Database table
- Pandas dataframe
- Apache arrow

**Currently supported storage engines:**

- Databases: postgres, mysql, sqlite (and most databases supported by SqlAlchemy)
- File systems: local, S3 (coming soon)
- Memory: python

In addition, dcp supports related operations
like inferring the schema of a dataset, conforming a dataset to a schema, and
creating empty objects of a specified schema.

## Usage

`pip install dcp` or `poetry add dcp`

#### Command line:

```bash
dcp orders.csv mysql://localhost:3306/mydb
```

This command will load the `orders.csv` file into a mysql table (by default of the same name `orders`)
on the given database, inferring the right schema from the data in the CSV.

```bash
dcp mysql://localhost:3306/mydb/orders s3://mybucket.s3/pth/orders.csv
```

This will export your `orders` table to a file on S3 (in the "default" format for
the StorageEngine since none was specified, in the case of S3 a CSV).

#### Python library

The python library gives you a powerful API for more complex operations:

```python
import dcp
from dcp import Storage

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

# [WIP] notes

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

```

Adding a new format requires adding the handling logic for that format, for
each storage class or engine that you want to support.

### TODO

-
