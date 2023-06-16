import logging
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import Field, root_validator

from datahub_monitors.common.config import PermissiveBaseModel

logger = logging.getLogger(__name__)

# The following types are used bound from
# the GraphQL API response objects.
# We allow Pydantic to handle most of the mapping for us.


class MonitorType(Enum):
    """Enumeration of monitor types."""

    ASSERTION = "ASSERTION"


class AssertionType(Enum):
    """Enumeration of assertion types."""

    DATASET = "DATASET"
    SLA = "SLA"


class SlaAssertionType(Enum):
    """Enumeration of sla assertion types."""

    DATASET_CHANGE = "DATASET_CHANGE"


class SlaAssertionScheduleType(Enum):
    """Enumeration of sla assertion schedule types."""

    CRON = "CRON"
    FIXED_INTERVAL = "FIXED_INTERVAL"


class PartitionType(Enum):
    """Enumeration of partition types."""

    FULL_TABLE = "FULL_TABLE"
    QUERY = "QUERY"
    TIMESTAMP_FIELD = "TIMESTAMP_FIELD"
    PARTITION = "PARTITION"


class CalendarInterval(Enum):
    """Enumeration of calendar intervals."""

    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"


class PartitionKeyFieldTransform(Enum):
    """Enumeration of partition key transform types."""

    DATE_DAY = "DATE_DAY"

    DATE_HOUR = "DATE_DAY"


class AssertionResultType(Enum):
    """Enumeration of assertion result types."""

    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class DatasetSlaSourceType(Enum):
    """Enumeration of Dataset SLA source types result types."""

    # The freshness signal comes from a column / field value last updated.
    FIELD_VALUE = "FIELD_VALUE"

    # The freshness signal from a dataset / table last updated statistic, e.g. provided by the catalog.
    INFORMATION_SCHEMA = "INFORMATION_SCHEMA"

    # The freshness signal from the audit log.
    AUDIT_LOG = "AUDIT_LOG"


class EntityEventType(Enum):
    """Enumeration of Entity Events that we support retrieving using a particular connection"""

    # An update has been performed to some rows based on the value of a particular field.
    FIELD_UPDATE = "FIELD_UPDATE"

    # An update has been performed to the table, based on a dataset last updated statistic maintained by the source system
    INFORMATION_SCHEMA_UPDATE = "INFORMATION_SCHEMA_UPDATE"

    # An update has been performed on a particular entity, as per an audit log.
    AUDIT_LOG_OPERATION = "AUDIT_LOG_OPERATION"

    # A data job has completed successfully
    DATA_JOB_RUN_COMPLETED_SUCCESS = "DATA_JOB_RUN_COMPLETED_SUCCESS"

    # A data job has completed successfully
    DATA_JOB_RUN_COMPLETED_FAILURE = "DATA_JOB_RUN_COMPLETED_FAILURE"


class AssertionEvaluationParametersType(Enum):
    """Enumeration of evaluation parameter types for an assertion"""

    DATASET_SLA = "DATASET_SLA"


class CronSchedule(PermissiveBaseModel):
    """The cron string"""

    cron: str

    timezone: str


class SlaCronSchedule(PermissiveBaseModel):
    """The cron string"""

    cron: str

    timezone: str

    # An optional start time offset from the cron schedule. If not provided, the boundary will be computed between the evaluation time and previous evaluation time.
    window_start_offset_ms: Optional[int] = Field(alias="windowStartOffsetMs")


class FixedIntervalSchedule(PermissiveBaseModel):
    """The unit of the fixed interval schedule"""

    unit: CalendarInterval

    multiple: int


class SlaAssertionSchedule(PermissiveBaseModel):
    """The type of the schedule"""

    type: SlaAssertionScheduleType

    cron: Optional[SlaCronSchedule] = None

    fixed_interval: Optional[FixedIntervalSchedule] = Field(alias="fixedInterval")


class SchemaFieldSpec(PermissiveBaseModel):
    """The schema field urn"""

    # The field path of the schema field"""
    path: str

    # The std DataHub type of the field
    type: str

    # The native type of the field collected from source
    native_type: Optional[str] = Field(alias="nativeType")


class AuditLogSpec(PermissiveBaseModel):
    """The type of operation. If not provided all operations will be considered."""

    operation_types: Optional[List[str]] = Field(alias="operationTypes")

    user_name: Optional[str] = Field(alias="userName")


class DatasetSlaAssertionParameters(PermissiveBaseModel):
    """The type of the freshness signal"""

    source_type: DatasetSlaSourceType = Field(alias="sourceType")

    # A descriptor for a Dataset Field to use. Present when source_type is FIELD_LAST_UPDATED
    field: Optional[SchemaFieldSpec] = None

    # A descriptor for a Dataset Column to use. Present when source_type is AUDIT_LOG_OPERATION
    audit_log: Optional[AuditLogSpec] = Field(alias="auditLog")


class SlaAssertion(PermissiveBaseModel):
    """The type of the SLA Assertion"""

    type: SlaAssertionType

    schedule: SlaAssertionSchedule


class AssertionEntity(PermissiveBaseModel):
    """A unique identifier for the assertee (e.g. dataset urn). This represents the unique coordinates inside the data platform"""

    urn: str

    # A unique identifier for the platform urn
    platform_urn: str = Field(alias="platformUrn")

    # Platform instance id
    platform_instance: Optional[str] = Field(alias="platformInstance")

    # A list of sub-types for the entity, inside the platform
    sub_types: Optional[List[str]] = Field(alias="subTypes")


class PartitionKeyFieldSpec(PermissiveBaseModel):
    """Unique id for the partition key field"""

    id: str

    # The name of the source field feeding into the partition
    source_field_name: str

    # The transform to apply to the source field to compute the partition key field
    source_field_transform: Optional[PartitionKeyFieldTransform]

    def __init__(
        self,
        id: str,
        source_field_name: str,
        source_field_transform: Optional[PartitionKeyFieldTransform],
    ):
        self.id = id
        self.source_field_name = source_field_name
        self.source_field_transform = source_field_transform


class PartitionKeySpec(PermissiveBaseModel):
    """A specification of the fields that comprise a partition"""

    field_specs: List[PartitionKeyFieldSpec]

    def __init__(self, field_specs: List[PartitionKeyFieldSpec]):
        self.field_specs = field_specs


class PartitionKey(PermissiveBaseModel):
    """An identifier for a particular partition"""

    # Unique identifier for the partition, encoded.
    partition_id: str

    def __init__(self, partition_id: str):
        self.partition_id = partition_id


class PartitionSpec(PermissiveBaseModel):
    """A type of partition"""

    type: PartitionType

    # Raw encoded partition string
    partition: str

    # Definition of the fields of the partition
    partition_key_spec: PartitionKeySpec

    # A filter for a particular partition. If not provided, all partitions of the spec will be considered.
    partition_key: Optional[PartitionKey]

    def __init__(
        self,
        type: PartitionType,
        partition: str,
        partition_key_spec: PartitionKeySpec,
        partition_key: Optional[PartitionKey] = None,
    ):
        self.type = type
        self.partition = partition
        self.partition_key_spec = partition_key_spec
        self.partition_key = partition_key


class Assertion(PermissiveBaseModel):
    """A unique identifier for the assertion"""

    # A unique identifier for the assertion
    urn: str

    # The type of the assertion
    type: AssertionType

    # The entity being asserted on
    entity: AssertionEntity

    # The urn of the connection required to evaluate the assertion. If there is no connection urn we are limited in terms of what we can do
    connection_urn: Optional[str] = Field(alias="connectionUrn")

    # An SLA Assertion Object
    sla_assertion: Optional[SlaAssertion] = Field(alias="slaAssertion")

    @root_validator(pre=True)
    def extract(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        # Attempt to extract the entity field from the "relationships"
        # response object provided by the GraphQL API.
        if "entity" not in values:
            graphql_entity = values["relationships"]["relationships"][0]["entity"]
            platform_urn = graphql_entity["platform"]["urn"]
            entity_urn = graphql_entity["urn"]

            graphql_entity["urn"]
            sub_types = (
                graphql_entity["subTypes"]["typeNames"]
                if "subTypes" in graphql_entity
                and graphql_entity["subTypes"] is not None
                and "typeNames" in graphql_entity["subTypes"]
                else []
            )

            values["entity"] = {
                "urn": entity_urn,
                "platformUrn": platform_urn,
                "platformInstance": None,
                "subTypes": sub_types,
            }
        if "connectionUrn" not in values:
            graphql_entity = values["relationships"]["relationships"][0]["entity"]
            platform_urn = graphql_entity["platform"]["urn"]
            values["connectionUrn"] = platform_urn
        if "info" in values and "type" in values["info"]:
            values["type"] = values["info"]["type"]
        if "slaAssertion" not in values:
            values["slaAssertion"] = values["info"]["slaAssertion"]
        return values


class AssertionEvaluationParameters(PermissiveBaseModel):
    # The type of the parameters"""
    type: AssertionEvaluationParametersType

    # Dataset SLA Parameters. Present if the type is DATASET_SLA
    dataset_sla_parameters: Optional[DatasetSlaAssertionParameters] = Field(
        alias="datasetSlaParameters"
    )


class AssertionEvaluationSpec(PermissiveBaseModel):
    # The assertion to be evaluated
    assertion: Assertion

    # The schedule on which to evaluate the assertions
    schedule: CronSchedule

    # The parameters required to evaluate an assertion
    parameters: Optional[AssertionEvaluationParameters] = None


class AssertionMonitor(PermissiveBaseModel):
    """A monitor that evaluates assertions"""

    assertions: List[AssertionEvaluationSpec]


class Monitor(PermissiveBaseModel):
    """An asset monitor"""

    urn: str

    type: MonitorType

    assertion_monitor: Optional[AssertionMonitor]

    @root_validator(pre=True)
    def extract_info(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if "info" in values and "type" in values["info"]:
            values["type"] = values["info"]["type"]
        if "assertion_monitor" not in values:
            values["assertion_monitor"] = values["info"]["assertionMonitor"]
        return values


class AssertionEvaluationContext:
    """Context provided during an Assertion Evaluation"""

    dry_run: bool = False

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run


class AssertionEvaluationResult:
    """The result of evaluating an assertion."""

    def __init__(self, type: AssertionResultType, parameters: Optional[dict]):
        self.type = type
        self.parameters = parameters


class ConnectionDetails:
    _dict: Dict

    def __init__(self, _dict: Dict):
        self._dict = _dict


class EntityEvent:
    # The type of the event
    event_type: EntityEventType

    # The timestamp associated with the event
    event_time: int

    def __init__(self, event_type: EntityEventType, event_time: int):
        self.event_type = event_type
        self.event_time = event_time
