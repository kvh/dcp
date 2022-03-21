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

`dcp orders.csv mysql://root@localhost:3306/mydb/orders`

### _Fast_

dcp uses best-in-class underlying client libraries, employs parallelization
and compression to the extent possible, and estimates the memory,
cpu, and wire costs of any copy operation to select the _lowest cost copy path_
for available storages.

### _Safe_

dcp uses [Common Model](github.com/kvh/common-model) Schemas under the hood as the "lingua franca" of
structured data, allowing for careful preservation of logical data types and
values across many formats and storage engines. Error handling behavior
is configurable so when type conversion errors are encountered -- a
value is truncated or cannot be cast -- dcp can fail, relax the datatype,
or set the value null depending on the desired behavior.

**Currently supported formats:**

- JSON
- CSV file
- Database table
- Pandas dataframe
- Apache arrow

**Currently supported storage engines:**

- Databases: postgres, mysql, sqlite (and any database supported by SqlAlchemy)
- File systems: local, S3 (coming soon)
- Memory: python

In addition, dcp supports related operations
like inferring the schema of a dataset, conforming a dataset to a schema, and
creating empty objects of a specified schema.

## Usage

`pip install datacopy` or `poetry add datacopy`

#### Command line:

`dcp orders.csv mysql://localhost:3306/mydb`

This command will load the `orders.csv` file into a mysql table (by default of the same name `orders`)
on the given database, inferring the right schema from the data in the CSV.

`dcp mysql://localhost:3306/mydb/orders s3://mybucket.s3/pth/orders.csv`

This will export your `orders` table to a file on S3 (in the "default" format for
the StorageEngine since none was specified, in the case of S3 a CSV).

#### Python library

The python library gives you a powerful API for more complex operations:

```python
import
from dcp import Storage

records = [{"f1":"hello", "f2": "world"}]
fields = .infer_fields(records)
print(fields)
# >>> [Field(name="f1", type=Text), Field(name="f2", field_type=Text)]

.copy(
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

.copy(
    from_name='records',
    from_storage='file:///tmp/dcp/',
    to_storage='postgresql://localhost:5432/mydb'
)

data_format = .infer_format("records", storage='file:///tmp/dcp')
print(data_format)
# >>> CsvFileFormat

.copy(
    from_name='records',
    from_storage='file:///tmp/dcp/',
    to_storage='postgresql://localhost:5432/mydb'
    fields=fields,
    cast_level='strict',
)

assert Storage('postgresql://localhost:5432/mydb').get_api().exists("records")
```

# [WIP] Adding your own Storage Engine or Data Format

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
