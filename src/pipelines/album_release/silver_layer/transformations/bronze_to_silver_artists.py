from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict

spark = get_spark()
logger = get_logger()
DEFAULT_SILVER_WRITE_OPTIONS = {"overwriteSchema": "true"}


def transform_artists(
    read_path: str,
    write_path_dim: str,
    write_path_bridge: str,
    write_mode: str = 'overwrite',
    write_options: Dict[str, str] = DEFAULT_SILVER_WRITE_OPTIONS
) -> None:
    """Transform artist data from bronze to silver layer.

    Extracts artists from albums and creates:
    1. A deduplicated dimension table (dim_artists)
    2. A bridge table maintaining all album-artist relationships

    Args:
        read_path: Path to bronze album data with nested artists
        write_path_dim: Path to silver dim_artists table
        write_path_bridge: Path to silver bridge_album_artists table
        write_mode: Spark write mode (default: 'overwrite')
        write_options: Additional write options

    Raises:
        ValueError: If no data found or all records filtered out
    """
    df = None

    try:
        df = read_data(spark, read_path, 'delta')

        if not df.head(1):
            raise ValueError(f"No data found at path {read_path}")

    except Exception:
        logger.exception(f"Failed to read data from {read_path}")
        raise

    try:
        df.cache()
        initial_count = df.count()
        logger.info(f'Read {initial_count} album records from {read_path}')

        # Check how many albums have artists
        albums_with_artists = df.filter(F.size('artists') > 0).count()
        logger.info(
            f"Found {albums_with_artists} albums with artist information")

        try:
            logger.info("Starting artist extraction")

            # First, explode artists with album relationship
            df_exploded = df.select(
                F.explode_outer('artists').alias('artist'),
                F.col('id').alias('album_id')
            ).filter(
                F.col('artist.id').isNotNull()  # Remove null artists
            )

            # Create dimension table (unique artists only)
            df_dim_artists = df_exploded.select(
                F.col('artist.id').alias('artist_id'),
                F.col('artist.name').alias('artist_name'),
                F.col('artist.type').alias('artist_type'),
                F.col('artist.uri').alias('artist_uri'),
                F.col('artist.href').alias('artist_href'),
                F.col('artist.external_urls.spotify').alias('spotify_url')
            ).dropDuplicates(['artist_id']).withColumn(
                'extraction_date', F.current_date()
            )

            # create bridge table (all album-artist relationships)
            df_bridge = df_exploded.select(
                'album_id',
                F.col('artist.id').alias('artist_id')
            ).withColumn(
                'extraction_date', F.current_date()
            )

            # get counts for logging
            unique_artists_count = df_dim_artists.count()
            relationships_count = df_bridge.count()

            # additional metrics
            artists_per_album = relationships_count / \
                initial_count if initial_count > 0 else 0
            albums_per_artist = relationships_count / \
                unique_artists_count if unique_artists_count > 0 else 0

            logger.info(
                f"Extracted {unique_artists_count} unique artists from {initial_count} albums")
            logger.info(
                f"Created {relationships_count} album-artist relationships")
            logger.info(f"Average artists per album: {artists_per_album:.2f}")
            logger.info(f"Average albums per artist: {albums_per_artist:.2f}")

            if unique_artists_count == 0:
                raise ValueError("No artists extracted from albums")

        except ValueError:
            raise
        except Exception:
            logger.exception("Failed during artist extraction")
            raise

        try:
            # write dimension table
            logger.info(
                f"Writing {unique_artists_count} unique artists to {write_path_dim}")
            writer(df_dim_artists, write_path_dim,
                   mode=write_mode, options=write_options)
            logger.info("Dimension table write completed successfully")

            # write bridge table
            logger.info(
                f"Writing {relationships_count} relationships to {write_path_bridge}")
            writer(df_bridge, write_path_bridge,
                   mode=write_mode, options=write_options)
            logger.info("Bridge table write completed successfully")

        except Exception:
            logger.exception("Failed to write data")
            raise

    finally:
        if df:
            df.unpersist()
            logger.debug("Unpersisted cached DataFrame")
