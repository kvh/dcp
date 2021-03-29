from dcp.data_format.formats.memory.dataframe import DataFrameFormat
import decimal
from datetime import date, datetime, time
import pandas as pd

#  python_sample_values
nullish = [None, "None", "null", "none"]
bool_ = True
int_ = 2 ** 16
big_int = 2 ** 33
float_ = 1.119851925872322
floatstr = "1.119851925872322"
decimal_ = decimal.Decimal("109342342.123")
date_ = date(2020, 1, 1)
datestr = "1/1/2020"
dateisostr = "2020-01-01"
datetime_ = datetime(2020, 1, 1)
datetimestr = "2017-02-17T15:09:26-08:00"
timestr = "15:09:26"
time_ = time(20, 1, 1)
long_text = "helloworld" * int(65536 / 9)
json_ = {"hello": "world"}


test_records = [
    {"f1": "hi", "f2": 1, "f3": None, "f4": "2020-01-01", "f5": "2020-01-01 00:00:00"},
    {"f1": "bye", "f2": 2, "f3": None, "f4": "2020-01-01", "f5": "2020-01-01 00:00:00"},
    {"f1": None, "f2": 2, "f3": None, "f4": "2020-01-01", "f5": "2020-01-01 00:00:00"},
    {"f1": "bye", "f2": None, "f3": None, "f4": "2020-01-01", "f5": "202001 bad data",},
]
# rf = (RecordsFormat, lambda: records)
dff = (DataFrameFormat, lambda: pd.DataFrame.from_records(test_records))
# af = (
#     ArrowTableFormat,
#     lambda: pa.Table.from_pydict(
#         {k: [r[k] for r in records] for k in records[0].keys()}
#     ),
# )
# dlff = (DelimitedFileObjectFormat, lambda: StringIO("f1,f2\nhi,1\nbye,2"))
# rif = (RecordsIteratorFormat, lambda: ([r] for r in records))
# dfif = (DataFrameIteratorFormat, lambda: (pd.DataFrame([r]) for r in records))

test_data_format_objects = [
    dff,
]
