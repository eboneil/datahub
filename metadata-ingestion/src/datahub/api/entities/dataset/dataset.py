import logging
from typing import Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, validator

from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.extractor.schema_util import avro_schema_to_mce_fields
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    ExtendedPropertiesClass,
    ExtendedPropertyValueAssignmentClass,
    OtherSchemaClass,
    SchemaMetadataClass,
    SubTypesClass,
    UpstreamClass,
)
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.dataset_urn import DatasetUrn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SchemaSpecification(BaseModel):
    file: Optional[str]

    @validator("file")
    def file_must_be_avsc(cls, v):
        if v and not v.endswith(".avsc"):
            raise ValueError("file must be a .avsc file")
        return v


class Dataset(BaseModel):
    id: Optional[str]
    platform: Optional[str]
    env: str = "PROD"
    urn: Optional[str]
    description: Optional[str]
    name: Optional[str]
    schema_field: Optional[SchemaSpecification] = Field(alias="schema")
    downstreams: Optional[List[str]]
    terms: Optional[list]
    properties: Optional[Dict[str, str]]
    subtype: Optional[str]
    subtypes: Optional[List[str]]
    extended_properties: Optional[Dict[str, Union[str, List[str]]]] = None

    @property
    def platform_urn(self):
        if self.platform:
            return make_data_platform_urn(self.platform)
        else:
            assert self.urn is not None   # validator should have filled this in
            dataset_urn = DatasetUrn.create_from_string(self.urn)
            return dataset_urn.get_data_platform_urn()

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if urn is not"
            assert "platform" in values, "platform must be present if urn is not"
            assert "env" in values, "env must be present if urn is not"
            return make_dataset_urn(values["platform"], values["id"], values["env"])
        return v


    @validator("name", pre=True, always=True)
    def name_filled_with_id_if_not_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if name is not"
            return values["id"]
        return v

    @validator("platform")
    def platform_must_not_be_urn(cls, v):
        if v.startswith("urn:li:dataPlatform:"):
            return v[len("urn:li:dataPlatform:") :]
        return v

    @staticmethod  # TODO: determine if this should be static or not
    def create(file: str) -> None:
        emitter: DataHubGraph
        with get_default_graph() as emitter:
            with open(file, "r") as fp:
                datasets: List[dict] = yaml.safe_load(fp)
                for dataset_raw in datasets:
                    dataset = Dataset.parse_obj(dataset_raw)
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=dataset.urn,
                        aspect=DatasetPropertiesClass(
                            description=dataset.description,
                            name=dataset.name,
                            customProperties=dataset.properties,
                        ),
                    )
                    emitter.emit_mcp(mcp)
                    if dataset.schema_field and dataset.schema_field.file:
                        with open(dataset.schema_field.file, "r") as schema_fp:
                            schema_string = schema_fp.read()
                            schema_metadata = SchemaMetadataClass(
                                schemaName=dataset.name or dataset.id or dataset.urn or "",
                                platform=dataset.platform_urn,
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
                                    for s in [dataset.subtype]
                                    + (dataset.subtypes or [])
                                    if s
                                ]
                            ),
                        )
                        emitter.emit_mcp(mcp)

                    if dataset.extended_properties:
                        extended_properties_flattened = [
                            (key, value)
                            for key, value in dataset.extended_properties.items()
                            if isinstance(value, str)
                        ]
                        for key, value in dataset.extended_properties.items():
                            if isinstance(value, list):
                                for v in value:
                                    extended_properties_flattened.append((key, v))
                        sorted(extended_properties_flattened, key=lambda x: x[0])
                        mcp = MetadataChangeProposalWrapper(
                            entityUrn=dataset.urn,
                            aspect=ExtendedPropertiesClass(
                                properties=[
                                    ExtendedPropertyValueAssignmentClass(
                                        propertyUrn=f"urn:li:extendedProperty:{prop_key}",
                                        value=prop_value,
                                    )
                                    for prop_key, prop_value in extended_properties_flattened
                                ]
                            ),
                        )
                        emitter.emit_mcp(mcp)

                    if dataset.downstreams:
                        for downstream in dataset.downstreams:
                            patch_builder = DatasetPatchBuilder(downstream)
                            assert dataset.urn is not None  # validator should have filled this in
                            patch_builder.add_upstream_lineage(
                                UpstreamClass(
                                    dataset=dataset.urn,
                                    type="COPY",
                                )
                            )
                            for patch_event in patch_builder.build():
                                emitter.emit(patch_event)
                    logger.info(f"Created dataset {dataset.urn}")
