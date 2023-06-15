from datahub_integrations.graphql.connection import _get_id_from_connection_urn
from datahub_integrations.slack.config import _SLACK_CONFIG_ID, _SLACK_CONFIG_URN


def test_urn_parsing() -> None:
    assert _get_id_from_connection_urn(_SLACK_CONFIG_URN) == _SLACK_CONFIG_ID
