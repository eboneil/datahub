import logging
import os
import tempfile
from random import randint
from typing import Iterable, Union

import pytest
import tenacity
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, RecordEnvelope
from datahub.ingestion.api.sink import NoopWriteCallback
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.ingestion.sink.file import FileSink, FileSinkConfig
from datahub.metadata.schema_classes import (
    ExtendedPropertiesClass, ExtendedPropertyDefinitionClass,
    ExtendedPropertyValueAssignmentClass, LogicalEntityInfoClass)

from tests.utils import (delete_urns, delete_urns_from_file, get_gms_url,
                         get_sleep_info, ingest_file_via_rest,
                         wait_for_writes_to_sync)

logger = logging.getLogger(__name__)

start_index = randint(10, 10000)
dataset_urns = [
    make_dataset_urn("snowflake", f"table_foo_{i}")
    for i in range(start_index, start_index + 10)
]

generated_urns = [d for d in dataset_urns]


class FileEmitter:
    def __init__(self, filename: str) -> None:
        self.sink: FileSink = FileSink(
            ctx=PipelineContext(run_id="create_test_data"),
            config=FileSinkConfig(filename=filename),
        )

    def emit(self, event):
        self.sink.write_record_async(
            record_envelope=RecordEnvelope(record=event, metadata={}),
            write_callback=NoopWriteCallback(),
        )

    def close(self):
        self.sink.close()


def create_logical_entity(entity_name: str) -> Iterable[MetadataChangeProposalWrapper]:
    mcp = MetadataChangeProposalWrapper(
        entityUrn="urn:li:logicalEntity:" + entity_name,
        aspect=LogicalEntityInfoClass(
            fullyQualifiedName="io.datahubproject." + entity_name,
            displayName=entity_name,
        ),
    )
    return [mcp]


def create_test_data(filename: str):
    file_emitter = FileEmitter(filename)
    for mcps in create_logical_entity("dataset"):
        file_emitter.emit(mcps)

    file_emitter.close()


sleep_sec, sleep_times = get_sleep_info()


@pytest.fixture(scope="module", autouse=False)
def graph():
    graph: DataHubGraph = DataHubGraph(config=DatahubClientConfig(server=get_gms_url()))
    return graph


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(request):
    new_file, filename = tempfile.mkstemp()
    try:
        create_test_data(filename)
        print("ingesting extended properties test data")
        ingest_file_via_rest(filename)
        yield
        print("removing extended properties test data")
        delete_urns_from_file(filename)
        delete_urns(generated_urns)
        wait_for_writes_to_sync()
    finally:
        os.remove(filename)


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


def create_property_definition(
    property_name: str,
    graph: DataHubGraph,
    value_type: str = "STRING",
    cardinality: str = "SINGLE",
):
    extended_property_definition = ExtendedPropertyDefinitionClass(
        fullyQualifiedName=f"io.acryl.privacy.{property_name}",
        valueType=value_type,
        # description="The retention policy for the dataset",
        entityTypes=["urn:li:logicalEntity:dataset"],
        cardinality=cardinality,
    )

    mcp = MetadataChangeProposalWrapper(
        entityUrn=f"urn:li:extendedProperty:{property_name}",
        aspect=extended_property_definition,
    )
    graph.emit(mcp)


def attach_property_to_dataset(
    urn: str,
    property_name: str,
    property_value: Union[str, float, list[str], list[float]],
    graph: DataHubGraph,
):
    property_values = (
        [property_value] if not isinstance(property_value, list) else property_value
    )
    # breakpoint()

    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=ExtendedPropertiesClass(
            properties=[
                ExtendedPropertyValueAssignmentClass(
                    propertyUrn=f"urn:li:extendedProperty:{property_name}", value=val
                )
                for val in property_values
            ]
        ),
    )
    graph.emit_mcp(mcp)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
@pytest.mark.dependency(depends=["test_healthchecks"])
def test_extended_property_string(ingest_cleanup_data, graph):
    property_name = "retentionPolicy"

    create_property_definition(property_name, graph)
    generated_urns.append("urn:li:extendedProperty:retentionPolicy")

    attach_property_to_dataset(dataset_urns[0], property_name, ["30d"], graph=graph)

    try:
        attach_property_to_dataset(dataset_urns[0], property_name, 200030, graph=graph)
        raise AssertionError(
            "Should not be able to attach a number to a string property"
        )
    except Exception as e:
        if not isinstance(e, AssertionError):
            pass
        else:
            raise e


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
@pytest.mark.dependency(depends=["test_healthchecks"])
def test_extended_property_double(ingest_cleanup_data, graph):
    property_name = "expiryTime"
    generated_urns.append(f"urn:li:extendedProperty:{property_name}")
    create_property_definition(property_name, graph, value_type="NUMBER")

    attach_property_to_dataset(dataset_urns[0], property_name, 2000034, graph=graph)

    try:
        attach_property_to_dataset(dataset_urns[0], property_name, "30d", graph=graph)
        raise AssertionError(
            "Should not be able to attach a string to a number property"
        )
    except Exception as e:
        if not isinstance(e, AssertionError):
            pass
        else:
            raise e

    try:
        attach_property_to_dataset(
            dataset_urns[0], property_name, [2000034, 2000035], graph=graph
        )
        raise AssertionError("Should not be able to attach a list to a number property")
    except Exception as e:
        if not isinstance(e, AssertionError):
            pass
        else:
            raise e


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
@pytest.mark.dependency(depends=["test_healthchecks"])
def test_extended_property_double_multiple(ingest_cleanup_data, graph):
    property_name = "versions"
    generated_urns.append(f"urn:li:extendedProperty:{property_name}")

    create_property_definition(
        property_name, graph, value_type="NUMBER", cardinality="MULTIPLE"
    )

    attach_property_to_dataset(dataset_urns[0], property_name, [1.0, 2.0], graph=graph)
