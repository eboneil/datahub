import json
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from loguru import logger

_connection_urn_prefix = "urn:li:dataHubConnection:"


def _is_connection_urn(urn: str) -> bool:
    return urn.startswith(_connection_urn_prefix)


def _get_id_from_connection_urn(urn: str) -> str:
    assert _is_connection_urn(urn)
    return urn[len(_connection_urn_prefix) :]


def get_connection(graph: DataHubGraph, urn: str) -> Optional[dict]:
    res = graph.execute_graphql(
        query="""
query GetSlackConnection($urn: String!) {
  connection(urn: $urn) {
    urn
    details {
      type
      json {
        blob
      }
    }
  }
}
""".strip(),
        variables={
            "urn": urn,
        },
    )

    if not res["connection"]:
        return None

    connection_type = res["connection"]["details"]["type"]
    if connection_type != "JSON":
        logger.error(
            f"Expected connection details type to be 'JSON', but got {connection_type}"
        )
        return None

    blob = res["connection"]["details"]["json"]["blob"]
    obj = json.loads(blob)

    return obj


def save_connection(graph: DataHubGraph, urn: str, blob: str) -> None:
    id = _get_id_from_connection_urn(urn)

    res = graph.execute_graphql(
        query="""
mutation SetSlackConnection($id: String!, $blob: String!) {
  upsertConnection(
    input: {
      id: $id,
      type: JSON,
      platformUrn: "urn:li:dataPlatform:slack",
      json: {blob: $blob}
    }
  ) {
    urn
  }
}
""".strip(),
        variables={
            "id": id,
            "blob": blob,
        },
    )

    assert res["upsertConnection"]["urn"] == urn
