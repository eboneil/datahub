import logging

from typing import List, Optional
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import ExtendedPropertyDefinitionClass

from datahub.configuration.common import ConfigModel
import yaml

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ExtendedProperties(ConfigModel):

    id: Optional[str] = None
    fully_qualified_name: Optional[str] = None
    type: Optional[str] = None
    description: Optional[str] = None
    display_name: Optional[str] = None
    entity_types: Optional[List[str]] = None
    cardinality: Optional[str] = None
    allowed_values: Optional[dict] = None

    property
    def urn(self) -> str:
        if self.id.startswith("urn:li:extendedProperty:"):
            return self.id
        else:
            return f"urn:li:extendedProperty:{self.id}"
        
    def make_logical_type_urn(type: str) -> str:
        if not type.startswith("urn:li:logicalType:"):
            return f"urn:li:logicalType:{type}"
        return type

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
                            entity_types=extendedproperty.entity_types,
                            cardinality=extendedproperty.cardinality,
                            allowedValues=extendedproperty.allowed_values,
                        ),
                    )
                    emitter.emit_mcp(mcp)

                    logger.info(f"Created extended property {extendedproperty.urn}")
