import logging

from pathlib import Path
from typing import Dict, List, Optional
from datahub.api.entities.extendedproperties.extendedproperties import ExtendedProperties
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import DatasetPropertiesClass, OtherSchemaClass, SchemaMetadataClass, SubTypesClass, UpstreamClass
from datahub.specific.dataset import DatasetPatchBuilder
from ruamel.yaml import YAML

from datahub.configuration.common import ConfigModel
import yaml

logger = logging.getLogger(__name__)


class Dataset(ConfigModel):

    id: Optional[str]
    platform: Optional[str]
    env: str = "PROD"
    description: Optional[str]
    name: Optional[str]
    downstreams: Optional[List[str]]
    terms: Optional[list]
    properties: Optional[Dict[str, str]]
    subtype: Optional[str]
    subtypes: Optional[List[str]]
    extended_properties: Optional[List[ExtendedProperties]] = None

    @property
    def urn(self) -> str:
        if self.urn:
            return self.urn
        else:
            return make_dataset_urn(platform={self.platform}, name={self.id}, env=self.env)

    def create(file: str):
        emitter: DataHubGraph
        with get_default_graph() as emitter:
            with open(file, "r") as fp:
                datasets: List[dict] = yaml.safe_load(fp)
                for dataset in datasets:
                    dataset = Dataset.parse_obj(dataset)
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset.urn,
                        aspect=DatasetPropertiesClass(
                            description=dataset.description,
                            name=dataset.name,
                            customProperties=dataset.properties,
                        ),
                    )
                    emitter.emit_mcp(mcp)
                    with open(dataset.schema_field.file, "r") as fp:
                        schema_string = fp.read()
                        schema_metadata = SchemaMetadataClass(
                            schemaName="test",
                            platform=make_data_platform_urn(dataset.platform),
                            version=0,
                            hash="",
                            platformSchema=OtherSchemaClass(rawSchema=schema_string),
                            fields=avro_schema_to_mce_fields(schema_string),
                        )
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=dataset.urn, aspect=schema_metadata
                        )
                        emitter.emit_mcp(mcp)

                    if dataset.subtype or dataset.subtypes:
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=dataset.urn,
                            aspect=SubTypesClass(
                                typeNames=[
                                    s
                                    for s in [dataset.subtype] + (dataset.subtypes or [])
                                    if s
                                ]
                            ),
                        )
                        emitter.emit_mcp(mcp)

                    if dataset.downstreams:
                        for downstream in dataset.downstreams:
                            patch_builder = DatasetPatchBuilder(downstream)
                            patch_builder.add_upstream_lineage(
                                UpstreamClass(
                                    dataset=dataset.urn,
                                    type="COPY",
                                )
                            )
                            for mcp in patch_builder.build():
                                emitter.emit(mcp)
                    logger.info(f"Created dataset {dataset.urn}")