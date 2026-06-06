# scripts/run_full_pipeline.py
from datetime import datetime
from utils.logger import get_logger
import sys

logger = get_logger(__name__)


def run_full_pipeline():
    """Execute complete medallion architecture pipeline."""
    pipeline_steps = [
        ("Landing Zone Ingestion", lambda: run_landing_zone()),
        ("Bronze Layer Creation", lambda: run_bronze()),
        ("Silver Layer Creation", lambda: run_silver()),
        ("Gold Layer Creation", lambda: run_gold())
    ]

    for step_name, step_function in pipeline_steps:
        try:
            logger.info(f"Starting: {step_name}")
            start = datetime.now()

            step_function()

            duration = (datetime.now() - start).total_seconds()
            logger.info(f"✓ {step_name} completed in {duration:.1f}s")

        except Exception as e:
            logger.error(f"* {step_name} failed: {e}")
            raise


def run_landing_zone():
    from step_01_album_release_landing_zone_ingestion import run
    run()


def run_bronze():
    from step_02_album_release_bronze_ingestion import run
    from utils.config import LANDING_ZONE_PATH, BRONZE_PATH
    run(LANDING_ZONE_PATH, BRONZE_PATH,
        fmt='json', options={'multiline': True})


def run_silver():
    from step_03_album_release_silver_creator import main
    main()


def run_gold():
    from step_04_album_release_gold_creator import main
    main()


def run_from_layer(start_layer: str):
    """Run pipeline starting from specific layer."""
    layers = {
        'landing': [run_landing_zone, run_bronze, run_silver, run_gold],
        'bronze': [run_bronze, run_silver, run_gold],
        'silver': [run_silver, run_gold],
        'gold': [run_gold]
    }

    if start_layer not in layers:
        logger.error(f"Invalid layer: {start_layer}")
        raise ValueError(f"Layer must be one of {list(layers.keys())}")

    steps_to_run = layers[start_layer]

    for step_func in steps_to_run:
        try:
            logger.info(f"Running: {step_func.__name__}")
            start = datetime.now()

            step_func()

            duration = (datetime.now() - start).total_seconds()
            logger.info(f"✓ {step_func.__name__} completed in {duration:.1f}s")

        except Exception as e:
            logger.error(f"✗ Failed at {step_func.__name__}: {e}")
            raise


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Run Spotify Data Pipeline')
    parser.add_argument(
        '--from',
        dest='start_from',
        choices=['landing', 'bronze', 'silver', 'gold'],
        help='Start pipeline from specific layer (runs this and all subsequent layers)'
    )

    args = parser.parse_args()

    try:
        if args.start_from:
            # Run from specific layer
            run_from_layer(args.start_from)
        else:
            # Run full pipeline
            run_full_pipeline()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        sys.exit(1)
