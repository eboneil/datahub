import logging
from typing import List, Optional

import yaml
from pydantic import validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.metadata.schema_classes import ExtendedPropertyDefinitionClass, PropertyCardinalityClass, PropertyValueClass
from datahub.utilities.urns.urn import Urn

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AllowedValue(ConfigModel):
    value: str
    description: Optional[str]

class ExtendedProperties(ConfigModel):

    id: Optional[str]
    urn: Optional[str]
    fully_qualified_name: Optional[str]
    type: str
    description: Optional[str]
    display_name: Optional[str]
    entity_types: Optional[List[str]]
    cardinality: Optional[str]
    allowed_values: Optional[List[AllowedValue]]

    @property
    def fqn(self) -> str:
        assert self.urn is not None
        return self.fully_qualified_name or self.id or Urn.create_from_string(self.urn).get_entity_id()[0]

    @validator("urn", pre=True, always=True)
    def urn_must_be_present(cls, v, values):
        if not v:
            assert "id" in values, "id must be present if urn is not"
            return f"urn:li:extendedProperty:{values['id']}"
        return v

    @staticmethod
    def create(file: str) -> None:
        emitter: DataHubGraph
        with get_default_graph() as emitter:
            with open(file, "r") as fp:
                extendedproperties: List[dict] = yaml.safe_load(fp)
                for extendedproperty_raw in extendedproperties:
                    extendedproperty = ExtendedProperties.parse_obj(extendedproperty_raw)
                    mcp = MetadataChangeProposalWrapper(
                        entityUrn=extendedproperty.urn,
                        aspect=ExtendedPropertyDefinitionClass(
                            fullyQualifiedName=extendedproperty.fqn,
                            valueType=Urn.make_logical_type_urn(
                                extendedproperty.type
                            ),
                            displayName=extendedproperty.display_name,
                            description=extendedproperty.description,
                            entityTypes=[
                                Urn.make_logical_entity_urn(entity_type)
                                for entity_type in extendedproperty.entity_types or []
                            ],
                            cardinality=extendedproperty.cardinality,
                            allowedValues=[PropertyValueClass(value=v.value, description=v.description) for v in extendedproperty.allowed_values] if extendedproperty.allowed_values else None,
                        ),
                    )
                    emitter.emit_mcp(mcp)

                    logger.info(f"Created extended property {extendedproperty.urn}")
