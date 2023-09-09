import logging

from typing import List, Optional
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import ExtendedPropertyDefinitionClass

from datahub.configuration.common import ConfigModel
from pydantic import validator
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtendedProperties(ConfigModel):

    id: Optional[str]
    urn: Optional[str]
    fully_qualified_name: Optional[str]
    type: Optional[str]
    description: Optional[str]
    display_name: Optional[str]
    entity_types: Optional[List[str]]
    cardinality: Optional[str]
    allowed_values: Optional[List[dict[str, str]]]

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if urn is not"
            return f"urn:li:extendedProperty:{values['id']}"
        return v
        
    def make_logical_type_urn(type: str) -> str:
        if not type.startswith("urn:li:logicalType:"):
            return f"urn:li:logicalType:{type}"
        return type

    def make_logical_entity_urn(entity_type: str) -> str:
        if not entity_type.startswith("urn:li:logicalEntity:"):
            return f"urn:li:logicalEntity:{entity_type}"
        return entity_type

    def create(file: str):
        emitter: DataHubGraph
        with get_default_graph() as emitter:
            with open(file, "r") as fp:
                extendedproperties: List[dict] = yaml.safe_load(fp)
                for extendedproperty in extendedproperties:
                    extendedproperty = ExtendedProperties.parse_obj(extendedproperty)
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=extendedproperty.urn,
                        aspect=ExtendedPropertyDefinitionClass(
                            fullyQualifiedName=extendedproperty.fully_qualified_name,
                            valueType=ExtendedProperties.make_logical_type_urn(extendedproperty.type),
                            displayName=extendedproperty.display_name,
                            description=extendedproperty.description,
                            entityTypes=[ExtendedProperties.make_logical_entity_urn(entity_type) for entity_type in extendedproperty.entity_types],
                            cardinality=extendedproperty.cardinality,
                            allowedValues=extendedproperty.allowed_values,
                        ),
                    )
                    emitter.emit_mcp(mcp)

                    logger.info(f"Created extended property {extendedproperty.urn}")
