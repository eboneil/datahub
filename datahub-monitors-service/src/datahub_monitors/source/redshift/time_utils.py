def convert_millis_to_timestamp(millis: int) -> str:
    seconds = millis / 1000.0
    return f"TIMESTAMP 'epoch' + {seconds} * INTERVAL '1 second'"


def convert_millis_to_timestamptz(millis: int) -> str:
    seconds = millis / 1000.0
    return f"TIMESTAMPTZ 'epoch' + {seconds} * INTERVAL '1 second'"


def convert_millis_to_date(millis: int) -> str:
    seconds = millis / 1000.0
    return f"(TIMESTAMP 'epoch' + {seconds} * INTERVAL '1 second')::DATE"


def convert_millis_to_timestamp_type(millis: int, column_type: str) -> str:
    if column_type == "TIMESTAMP" or column_type == "TIMESTAMP WITHOUT TIME ZONE":
        return convert_millis_to_timestamp(millis)
    elif column_type == "TIMESTAMPTZ" or column_type == "TIMESTAMP WITH TIME ZONE":
        return convert_millis_to_timestamptz(millis)
    elif column_type == "DATE":
        return convert_millis_to_date(millis)
    raise Exception(f"Unsupported column type {column_type} provided!")
