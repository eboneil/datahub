import pathlib

from datahub_integrations.slack.config import slack_config
from loguru import logger

slack_details = pathlib.Path("slack_details.json")

if __name__ == "__main__":
    config = slack_config.get_config()

    logger.debug(config.json(indent=2))

    slack_details.write_text(config.json(indent=2))
    logger.info(f"Wrote config to {slack_details}")
