from utils.logger import get_logger
from datetime import datetime
import sys
from src.pipelines.album_release.gold_layer.gold_artist_album_summary import create_artist_album_summary
from src.pipelines.album_release.gold_layer.gold_artist_albums_growth import create_artist_album_growth
from utils.config import (SILVER_PATH,
                          SILVER_BRIDGE_ARTISTS_ALBUMS,
                          SILVER_FACT_ALBUMS,
                          SILVER_DIM_ARTISTS,
                          GOLD_ARTISTS_SUMMARY,
                          GOLD_ARTISTS_GROWTH,
                          GOLD_PATH)

logger = get_logger(__name__)


def main():
    """Execute gold layer transformations with error handling and metrics."""
    pipeline_start = datetime.now()
    success_count = 0
    failed_tables = []

    logger.info("="*60)
    logger.info("Starting Gold Layer Pipeline")
    logger.info("="*60)

    # Table 1: Artist Summary
    try:
        logger.info("Creating Artist Album Summary table...")
        summary_start = datetime.now()

        create_artist_album_summary(
            SILVER_PATH,
            GOLD_PATH,
            GOLD_ARTISTS_SUMMARY,
            SILVER_FACT_ALBUMS,
            SILVER_DIM_ARTISTS,
            SILVER_BRIDGE_ARTISTS_ALBUMS,
        )

        summary_duration = (datetime.now() - summary_start).total_seconds()
        logger.info(f"✓ Artist Summary created in {summary_duration:.1f}s")
        success_count += 1

    except Exception as e:
        logger.error(f"✗ Failed to create Artist Summary: {e}")
        failed_tables.append("artist_summary")

    # Table 2: Artist Growth Metrics
    try:
        logger.info("Creating Artist Growth Metrics table...")
        growth_start = datetime.now()

        metrics = create_artist_album_growth(
            SILVER_PATH,
            SILVER_FACT_ALBUMS,
            SILVER_BRIDGE_ARTISTS_ALBUMS,
            SILVER_DIM_ARTISTS,
            GOLD_PATH,
            GOLD_ARTISTS_GROWTH,
            ['artist_id', 'release_year'],
            ['release_year']
        )

        growth_duration = (datetime.now() - growth_start).total_seconds()
        logger.info(f"✓ Artist Growth created in {growth_duration:.1f}s")
        logger.info(
            f"  - Processed {metrics['summary']['throughput_rows_per_second']:.0f} rows/sec")
        success_count += 1

    except Exception as e:
        logger.error(f"✗ Failed to create Artist Growth: {e}")
        failed_tables.append("artist_growth")

    # Pipeline Summary
    total_duration = (datetime.now() - pipeline_start).total_seconds()

    logger.info("="*60)
    logger.info("Gold Layer Pipeline Summary")
    logger.info(f"Total Duration: {total_duration:.1f}s")
    logger.info(f"Tables Created: {success_count}/2")

    if failed_tables:
        logger.error(f"Failed Tables: {', '.join(failed_tables)}")
        logger.info("="*60)
        return 1  # Exit code for failure
    else:
        logger.info("✓ All gold tables created successfully!")
        logger.info("="*60)
        return 0  # Exit code for success


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
