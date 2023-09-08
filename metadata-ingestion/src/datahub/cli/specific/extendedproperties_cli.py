import json
import logging
import os
from pathlib import Path
import pathlib

import click
from click_default_group import DefaultGroup
from datahub import telemetry
from datahub.api.entities.extendedproperties.extendedproperties import ExtendedProperties
from datahub.cli.specific.file_loader import load_file
from datahub.ingestion.graph.client import get_default_graph
from datahub.telemetry import telemetry
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def extendedproperties() -> None:
    """A group of commands to interact with extended properties in DataHub."""
    pass


# def mutate(file: Path, validate_assets: bool, external_url: str, upsert: bool) -> None:
#     """Update or Upsert extended properties in DataHub"""

#     config_dict = load_file(pathlib.Path(file))
#     id = config_dict.get("id") if isinstance(config_dict, dict) else None
#     with get_default_graph() as graph:
#         dataset: ExtendedProperties = ExtendedProperties.from_yaml(file, graph)
#         external_url_override = (
#             external_url
#             or os.getenv("DATAHUB_EXTENDED_PROPERTIES_EXTERNAL_URL")
#             or dataset.external_url
#         )
#         dataset.external_url = external_url_override
#         if upsert and not graph.exists(dataset.urn):
#             logger.info(f"Dataset {dataset.urn} does not exist, will create.")
#             upsert = False

#         if validate_assets and dataset.assets:
#             missing_assets = []
#             for asset in dataset.assets:
#                 try:
#                     assert graph.exists(asset)
#                 except Exception as e:
#                     logger.debug("Failed to validate existence", exc_info=e)
#                     missing_assets.append(asset)
#             if missing_assets:
#                 for a in missing_assets:
#                     click.secho(f"Asset: {a} doesn't exist on DataHub", fg="red")
#                 click.secho(
#                     "Aborting update due to the presence of missing assets in the yaml file. Turn off validation of assets using the --no-validate-assets option if you want to proceed.",
#                     fg="red",
#                 )
#                 raise click.Abort()
#         try:
#             for mcp in dataset.generate_mcp(upsert=upsert):
#                 graph.emit(mcp)
#             click.secho(f"Update succeeded for urn {dataset.urn}.", fg="green")
#         except Exception as e:
#             click.secho(
#                 f"Update failed for id {id}. due to {e}",
#                 fg="red",
#             )


@extendedproperties.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert extended properties in DataHub."""

    ExtendedProperties.create(file)
