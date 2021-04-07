core_data_formats_precedence: List[DataFormat] = [
    ### Memory formats
    # Roughly ordered from most universal / "default" to least
    # Ordering used when inferring DataFormat from raw object and have ambiguous object (eg an empty list)
    RecordsFormat,
    DataFrameFormat,
    ArrowTableFormat,
    DatabaseCursorFormat,
    DatabaseTableRefFormat,
    RecordsIteratorFormat,
    DataFrameIteratorFormat,
    DelimitedFileObjectFormat,
    DelimitedFileObjectIteratorFormat,
    ### Non-python formats (can't be concrete python objects)
    DelimitedFileFormat,
    JsonLinesFileFormat,
    DatabaseTableFormat,
]


def get_data_format_of_object(obj: Any) -> Optional[DataFormat]:
    maybes = []
    for m in global_registry.all(DataFormatBase):
        if not m.is_python_format():
            continue
        assert issubclass(m, MemoryDataFormatBase)
        try:
            if m.definitely_instance(obj):
                return m
        except NotImplementedError:
            pass
        if m.maybe_instance(obj):
            maybes.append(m)
    if len(maybes) == 1:
        return maybes[0]
    elif len(maybes) > 1:
        return [f for f in core_data_formats_precedence if f in maybes][0]
    return None
