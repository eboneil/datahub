import pathlib

GRAPHQL_LIST_MONITORS_QUERY = (
    pathlib.Path(__file__).parent / "list_monitors.gql"
).read_text()


GRAPHQL_LIST_INGESTION_SOURCES_QUERY = (
    pathlib.Path(__file__).parent / "list_ingestion_sources.gql"
).read_text()
