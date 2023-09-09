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


@extendedproperties.command(
    name="upsert",
)
@click.option("-f", "--file", required=True, type=click.Path(exists=True))
@upgrade.check_upgrade
@telemetry.with_telemetry()
def upsert(file: Path) -> None:
    """Upsert extended properties in DataHub."""

    ExtendedProperties.create(file)
