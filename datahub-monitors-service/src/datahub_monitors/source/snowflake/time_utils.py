def convert_millis_to_date(millis: int) -> str:
    return f"DATE(TO_TIMESTAMP({millis}, 3))"


def convert_millis_to_timestamp(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)"


def convert_millis_to_timestamp_tz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_TZ"


def convert_millis_to_timestamp_ltz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_LTZ"


def convert_millis_to_timestamp_ntz(millis: int) -> str:
    return f"TO_TIMESTAMP({millis}, 3)::TIMESTAMP_NTZ"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "DATE":
        return convert_millis_to_date(millis)
    elif column_type == "TIMESTAMP":
        return convert_millis_to_timestamp(millis)
    elif column_type == "TIMESTAMP_TZ":
        return convert_millis_to_timestamp_tz(millis)
    elif column_type == "TIMESTAMP_LTZ":
        return convert_millis_to_timestamp_ltz(millis)
    elif column_type == "TIMESTAMP_NTZ":
        return convert_millis_to_timestamp_ntz(millis)
    elif column_type == "DATETIME":
        return convert_millis_to_timestamp_ntz(millis)
    raise Exception(f"Unsupported column type {column_type} provided!")
