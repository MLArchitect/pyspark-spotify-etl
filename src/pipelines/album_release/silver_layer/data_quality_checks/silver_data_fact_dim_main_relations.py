from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from utils.logger import get_logger
from typing import Dict, Tuple

spark = get_spark()
logger = get_logger()


def validate_silver_layer(silver_path: str,
                          fact_albums_path: str,
                          dim_artists_path: str,
                          bridge_album_artists: str) -> Tuple[bool, Dict[str, any]]:
    """Validate silver layer data quality.

    Args:
        silver_path: Base path to silver layer
        fact_albums_path: sub directory path to fact albums model
        dim_artists_path: sub directory path to dim artists model
        bridge_album_artists: sub directory path to bridge table between album and artists

    Returns:
        Tuple of (all_passed, results_dict)
    """
    validation_results = {}
    all_passed = True

    try:
        # loading all silver tables
        logger.info("Loading silver layer tables for validation")
        df_albums = read_data(
            spark, f'{silver_path}/{fact_albums_path}', 'delta')
        df_artists = read_data(
            spark, f'{silver_path}/{dim_artists_path}', 'delta')
        df_bridge_artists = read_data(
            spark, f'{silver_path}/{bridge_album_artists}', 'delta')

        # Test 1: Checking for orphaned artist_ids in bridge_album_artists
        logger.info("Running validation 1: Checking orphaned artist records")
        orphaned_artists = df_bridge_artists.alias('b').join(
            df_artists.alias('d'),
            F.col('b.artist_id') == F.col('d.artist_id'),
            'left'
        ).filter(
            F.col('d.artist_id').isNull()
        ).count()

        validation_results['orphaned_artists_count'] = orphaned_artists
        if orphaned_artists > 0:
            logger.error(
                f"FAILED: Found {orphaned_artists} orphaned artist records in bridge table")
            all_passed = False
        else:
            logger.info("PASSED: No orphaned artist records found")

        # Test 2: Checking album_id uniqueness in fact_albums
        logger.info("Running validation 2: Checking album_id uniqueness")
        total_albums = df_albums.count()
        unique_albums = df_albums.select('album_id').distinct().count()
        duplicate_albums = total_albums - unique_albums

        validation_results['duplicate_albums_count'] = duplicate_albums
        if duplicate_albums > 0:
            logger.error(
                f"FAILED: Found {duplicate_albums} duplicate album_ids in fact_albums")
            all_passed = False
        else:
            logger.info("PASSED: All album_ids are unique")

        # Test 3: Checking row count ratio - bridge should have >= dim_artists
        logger.info("Running validation 3: Checking row count ratios")
        artist_count = df_artists.count()
        bridge_count = df_bridge_artists.count()

        validation_results['artist_count'] = artist_count
        validation_results['bridge_artist_count'] = bridge_count

        if bridge_count < artist_count:
            logger.error(
                f"FAILED: Bridge table ({bridge_count}) has fewer rows than dim_artists ({artist_count})")
            all_passed = False
        else:
            logger.info(
                f"PASSED: Bridge table ({bridge_count}) has >= rows than dim_artists ({artist_count})")

        # Test 4: Checking artist_id is not null in dim_artists
        logger.info("Running validation 4: Checking for null artist_ids")
        null_artist_ids = df_artists.filter(
            F.col('artist_id').isNull()).count()

        validation_results['null_artist_ids_count'] = null_artist_ids
        if null_artist_ids > 0:
            logger.error(
                f"FAILED: Found {null_artist_ids} null artist_ids in dim_artists")
            all_passed = False
        else:
            logger.info("PASSED: No null artist_ids found")

        # Logging summary
        logger.info("="*50)
        logger.info("VALIDATION SUMMARY")
        logger.info("="*50)
        logger.info(f"Overall result: {'PASSED' if all_passed else 'FAILED'}")
        logger.info(f"Validation details: {validation_results}")

        return all_passed, validation_results

    except Exception as e:
        logger.exception("Failed to run silver layer validations")
        raise
