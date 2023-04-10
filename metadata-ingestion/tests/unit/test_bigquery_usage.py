import random
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from datahub.configuration.time_window_config import BucketDuration
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.bigquery_v2.bigquery_audit import BigQueryTableRef
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryUsageConfig,
    BigQueryV2Config,
)
from datahub.ingestion.source.bigquery_v2.bigquery_report import BigQueryV2Report
from datahub.ingestion.source.bigquery_v2.usage import (
    OPERATION_STATEMENT_TYPES,
    BigQueryUsageExtractor,
)
from datahub.metadata.schema_classes import (
    DatasetFieldUsageCountsClass,
    DatasetUsageStatisticsClass,
    DatasetUserUsageCountsClass,
    OperationClass,
    TimeWindowSizeClass,
)
from tests.performance.bigquery import generate_events, ref_from_table
from tests.performance.data_generation import generate_data, generate_queries
from tests.performance.data_model import Container, FieldAccess, Query, Table, View

PROJECT_1 = "project-1"
PROJECT_2 = "project-2"
ACTOR_1, ACTOR_1_URN = "a@acryl.io", "urn:li:corpuser:a"
ACTOR_2, ACTOR_2_URN = "b@acryl.io", "urn:li:corpuser:b"
DATABASE_1 = Container("database_1")
DATABASE_2 = Container("database_2")
TABLE_1 = Table("table_1", DATABASE_1, ["id", "name", "age"])
TABLE_2 = Table("table_2", DATABASE_1, ["id", "table_1_id", "value"])
VIEW_1 = View(
    name="view_1",
    container=DATABASE_1,
    columns=["id", "name", "total"],
    definition="VIEW DEFINITION 1",
    parents=[TABLE_1, TABLE_2],
)
ALL_TABLES = [TABLE_1, TABLE_2, VIEW_1]

TABLE_TO_PROJECT = {
    TABLE_1.name: PROJECT_1,
    TABLE_2.name: PROJECT_2,
    VIEW_1.name: PROJECT_1,
}
TABLE_REFS = {
    table.name: str(ref_from_table(table, TABLE_TO_PROJECT)) for table in ALL_TABLES
}

FROZEN_TIME = datetime(year=2023, month=2, day=1, tzinfo=timezone.utc)
TS_1 = datetime(year=2023, month=1, day=1, tzinfo=timezone.utc)
TS_2 = datetime(year=2023, month=1, day=2, tzinfo=timezone.utc)


def query_table_1_a(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM table_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_1),
            FieldAccess("name", TABLE_1),
            FieldAccess("age", TABLE_1),
        ],
    )


def query_table_1_b(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT name FROM table_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[FieldAccess("name", TABLE_1)],
    )


def query_table_2(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM table_2",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_2),
            FieldAccess("table_1_id", TABLE_2),
            FieldAccess("value", TABLE_2),
        ],
    )


def query_tables_1_and_2(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT t1.id, t1.name, t2.id, t2.value FROM table_1 t1 JOIN table_2 t2 ON table_1.id = table_2.table_1_id",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", TABLE_1),
            FieldAccess("name", TABLE_1),
            FieldAccess("id", TABLE_2),
            FieldAccess("value", TABLE_2),
        ],
    )


def query_view_1(timestamp: datetime = TS_1, actor: str = ACTOR_1) -> Query:
    return Query(
        text="SELECT * FROM view_1",
        type="SELECT",
        timestamp=timestamp,
        actor=actor,
        fields_accessed=[
            FieldAccess("id", VIEW_1),
            FieldAccess("name", VIEW_1),
            FieldAccess("total", VIEW_1),
        ],
    )


def make_usage_workunit(
    table: Table, dataset_usage_statistics: DatasetUsageStatisticsClass
) -> MetadataWorkUnit:
    resource = BigQueryTableRef.from_string_name(TABLE_REFS[table.name])
    return MetadataChangeProposalWrapper(
        entityUrn=resource.to_urn("PROD"),
        aspectName=dataset_usage_statistics.get_aspect_name(),
        aspect=dataset_usage_statistics,
    ).as_workunit()


def make_operational_workunit(
    resource: str, operation: OperationClass
) -> MetadataWorkUnit:
    return MetadataChangeProposalWrapper(
        entityUrn=BigQueryTableRef.from_string_name(resource).to_urn("PROD"),
        aspectName=operation.get_aspect_name(),
        aspect=operation,
    ).as_workunit()


@pytest.fixture
def config() -> BigQueryV2Config:
    return BigQueryV2Config(
        file_backed_cache_size=1,
        start_time=TS_1,
        end_time=TS_2 + timedelta(minutes=1),
        usage=BigQueryUsageConfig(
            include_top_n_queries=True,
            top_n_queries=3,
            bucket_duration=BucketDuration.DAY,
            include_operational_stats=False,
        ),
    )


@pytest.fixture
def usage_extractor(config: BigQueryV2Config) -> BigQueryUsageExtractor:
    report = BigQueryV2Report()
    return BigQueryUsageExtractor(config, report)


def test_usage_counts_single_bucket_resource_project(
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    queries = [
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_2),
        query_table_1_b(TS_1, ACTOR_1),
        query_table_1_b(TS_1, ACTOR_2),
    ]
    events = generate_events(
        queries,
        [PROJECT_1, PROJECT_2],
        TABLE_TO_PROJECT,
        config=config,
        proabability_of_project_mismatch=0.5,
    )

    workunits = usage_extractor._run(events, TABLE_REFS.values())
    assert list(workunits) == [
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=len(queries),
                topSqlQueries=[query_table_1_a().text, query_table_1_b().text],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=3,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=5,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                ],
            ),
        )
    ]


def test_usage_counts_multiple_buckets_and_resources(
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    queries = [
        # TS 1
        query_table_1_a(TS_1, ACTOR_1),
        query_table_1_a(TS_1, ACTOR_2),
        query_table_1_b(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_tables_1_and_2(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_1),
        query_view_1(TS_1, ACTOR_2),
        query_view_1(TS_1, ACTOR_2),
        # TS 2
        query_table_1_a(TS_2, ACTOR_1),
        query_table_1_a(TS_2, ACTOR_2),
        query_table_1_b(TS_2, ACTOR_2),
        query_tables_1_and_2(TS_2, ACTOR_2),
        query_table_2(TS_2, ACTOR_2),
        query_view_1(TS_2, ACTOR_1),
    ]
    events = generate_events(
        queries,
        [PROJECT_1, PROJECT_2],
        TABLE_TO_PROJECT,
        config=config,
        proabability_of_project_mismatch=0.5,
    )

    workunits = usage_extractor._run(events, TABLE_REFS.values())
    assert list(workunits) == [
        # TS 1
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=6,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                    query_table_1_a().text,
                    query_table_1_b().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=5,
                        userEmail=ACTOR_1,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=1,
                        userEmail=ACTOR_2,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=6,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=5,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=VIEW_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=3,
                topSqlQueries=[
                    query_view_1().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=1,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=3,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_1.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=3,
                topSqlQueries=[
                    query_tables_1_and_2().text,
                ],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=3,
                        userEmail=ACTOR_1,
                    )
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=3,
                    ),
                ],
            ),
        ),
        # TS 2
        make_usage_workunit(
            table=TABLE_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=4,
                topSqlQueries=[
                    query_table_1_a().text,
                    query_table_1_b().text,
                    query_tables_1_and_2().text,
                ],
                uniqueUserCount=2,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=3,
                        userEmail=ACTOR_2,
                    ),
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=1,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=4,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=3,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="age",
                        count=2,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=VIEW_1,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=1,
                topSqlQueries=[
                    query_view_1().text,
                ],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_1_URN,
                        count=1,
                        userEmail=ACTOR_1,
                    ),
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=1,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="name",
                        count=1,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="total",
                        count=1,
                    ),
                ],
            ),
        ),
        make_usage_workunit(
            table=TABLE_2,
            dataset_usage_statistics=DatasetUsageStatisticsClass(
                timestampMillis=int(TS_2.timestamp() * 1000),
                eventGranularity=TimeWindowSizeClass(
                    unit=BucketDuration.DAY, multiple=1
                ),
                totalSqlQueries=2,
                topSqlQueries=[query_table_2().text, query_tables_1_and_2().text],
                uniqueUserCount=1,
                userCounts=[
                    DatasetUserUsageCountsClass(
                        user=ACTOR_2_URN,
                        count=2,
                        userEmail=ACTOR_2,
                    )
                ],
                fieldCounts=[
                    DatasetFieldUsageCountsClass(
                        fieldPath="id",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="value",
                        count=2,
                    ),
                    DatasetFieldUsageCountsClass(
                        fieldPath="table_1_id",
                        count=1,
                    ),
                ],
            ),
        ),
    ]


@freeze_time(FROZEN_TIME)
@patch.object(BigQueryUsageExtractor, "_generate_usage_workunits")
def test_operational_stats(
    mock: MagicMock,
    usage_extractor: BigQueryUsageExtractor,
    config: BigQueryV2Config,
) -> None:
    mock.return_value = []
    config.usage.include_operational_stats = True
    seed_metadata = generate_data(
        num_containers=3,
        num_tables=5,
        num_views=2,
        time_range=timedelta(days=1),
    )
    all_tables = seed_metadata.tables + seed_metadata.views

    num_projects = 2
    projects = [f"project-{i}" for i in range(num_projects)]
    table_to_project = {table.name: random.choice(projects) for table in all_tables}
    table_refs = {
        table.name: str(ref_from_table(table, table_to_project)) for table in all_tables
    }

    queries = list(
        generate_queries(
            seed_metadata,
            num_selects=10,
            num_operations=20,
            num_users=3,
        )
    )

    events = generate_events(queries, projects, table_to_project, config=config)
    workunits = usage_extractor._run(events, table_refs.values())
    assert list(workunits) == [
        make_operational_workunit(
            table_refs[query.object_modified.name],
            OperationClass(
                timestampMillis=int(FROZEN_TIME.timestamp() * 1000),
                lastUpdatedTimestamp=int(query.timestamp.timestamp() * 1000),
                actor=f"urn:li:corpuser:{query.actor.split('@')[0]}",
                operationType=query.type
                if query.type in OPERATION_STATEMENT_TYPES.values()
                else "CUSTOM",
                customOperationType=None
                if query.type in OPERATION_STATEMENT_TYPES.values()
                else query.type,
                affectedDatasets=list(
                    dict.fromkeys(  # Preserve order
                        BigQueryTableRef.from_string_name(
                            table_refs[field.table.name]
                        ).to_urn("PROD")
                        for field in query.fields_accessed
                        if not field.table.is_view()
                    )
                ),
            ),
        )
        for query in queries
        if query.object_modified and query.type in OPERATION_STATEMENT_TYPES.values()
    ]
