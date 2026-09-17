import logging

import orchestrate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run():
    """
    Orchestrates the pipeline execution.
    orchestrate.run() handles Bronze, Silver, connection closure, and Gold (dbt).
    """
    logger.info("Starting pipeline execution...")
    orchestrate.run()
    logger.info("Pipeline execution completed successfully.")


if __name__ == "__main__":
    run()