import json
import logging
from pathlib import Path

import click
from click_default_group import DefaultGroup
from datahub.ingestion.graph.client import get_default_graph

from datahub.telemetry import telemetry
from datahub.api.entities.extendedproperties.extendedproperties import (
    ExtendedProperties,
)
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


@click.group(cls=DefaultGroup, default="upsert")
def extendedproperties() -> None:
    """A group of commands to interact with extended properties in DataHub."""
    pass


@extendedproperties.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert extended properties in DataHub."""

    ExtendedProperties.create(file)


@extendedproperties.command(
    name="get",
)
@click.option("--urn", required=True, type=str)
@click.option("--to-file", required=False, type=str)
@upgrade.check_upgrade
@telemetry.with_telemetry()
def get(urn: str, to_file: str) -> None:
    """Get extended properties from DataHub"""

    if not urn.startswith("urn:li:extendedproperty:"):
        urn = f"urn:li:extendedproperty:{urn}"

    with get_default_graph() as graph:
        if graph.exists(urn):
            extendedproperties: ExtendedProperties = ExtendedProperties.from_datahub(graph=graph, id=urn)
            click.secho(
                f"{json.dumps(extendedproperties.dict(exclude_unset=True, exclude_none=True), indent=2)}"
            )
            if to_file:
                extendedproperties.to_yaml(Path(to_file))
                click.secho(f"Extended property yaml written to {to_file}", fg="green")
        else:
            click.secho(f"Extended property {urn} does not exist")
