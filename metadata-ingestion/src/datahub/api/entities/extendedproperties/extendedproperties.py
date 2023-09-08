from pathlib import Path
from typing import List, Optional
from datahub.ingestion.graph.client import DataHubGraph
from ruamel.yaml import YAML

from datahub.configuration.common import ConfigModel


class ExtendedProperties(ConfigModel):

    id: Optional[str] = None
    fully_qualified_name: Optional[str] = None
    type: Optional[str] = None
    cardinality: Optional[str] = None
    display_name: Optional[str] = None
    entity_types: Optional[List[str]] = None
    description: Optional[str] = None
    allowed_values: Optional[dict] = None

    property
    def urn(self) -> str:
        if self.id.startswith("urn:li:extendedProperty:"):
            return self.id
        else:
            return f"urn:li:extendedProperty:{self.id}"

    @classmethod
    def from_yaml(
        cls,
        file: Path,
    ) -> "ExtendedProperties":
        with open(file) as fp:
            yaml = YAML(typ="rt")  # default, if not specfied, is 'rt' (round-trip)
            orig_dictionary = yaml.load(fp)
            parsed_extended_properties = ExtendedProperties.parse_obj_allow_extras(orig_dictionary)
            parsed_extended_properties._original_yaml_dict = orig_dictionary
            return parsed_extended_properties
