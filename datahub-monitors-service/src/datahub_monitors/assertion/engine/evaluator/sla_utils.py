from typing import Dict, Tuple

from datahub_monitors.types import (
    AssertionEvaluationParameters,
    DatasetSlaSourceType,
    EntityEventType,
)


def get_event_type_parameters_from_parameters(
    parameters: AssertionEvaluationParameters,
) -> Tuple[EntityEventType, Dict]:
    """
    Extracts the standard event type, parameters from the AssertionEvaluationParameters
    These are for use in retrieving information using a Connection object to an external store.

    If these cannot be properly extracted, an exception will be raised.
    """
    # Extract the entity event type from the SLA assertion, along with additional filters / parameters that are required.
    if parameters.dataset_sla_parameters is not None:
        # We are parsing a dataset SLA assertion
        dataset_sla_parameters = parameters.dataset_sla_parameters
        source_type = dataset_sla_parameters.source_type
        if source_type == DatasetSlaSourceType.FIELD_VALUE:
            entity_event_type = EntityEventType.FIELD_UPDATE
            params = dataset_sla_parameters.field.__dict__
            return (entity_event_type, params)
        elif source_type == DatasetSlaSourceType.INFORMATION_SCHEMA:
            entity_event_type = EntityEventType.INFORMATION_SCHEMA_UPDATE
            return (entity_event_type, {})
        elif source_type == DatasetSlaSourceType.AUDIT_LOG:
            entity_event_type = EntityEventType.AUDIT_LOG_OPERATION
            params = dataset_sla_parameters.audit_log.__dict__
            return (entity_event_type, params)
        else:
            raise Exception(
                f"Failed to extract EntityEntityEventType & Parameters from Dataset SLA Assertion. Unsupported source type found {source_type}"
            )

    raise Exception(
        "Failed to extract EntityEventType & Parameters from Dataset SLA Assertion. Malformed assertion type found. Missing dataset_sla_parameters."
    )
