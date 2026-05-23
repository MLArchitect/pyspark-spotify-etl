from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict

spark = get_spark()
logger = get_logger()
DEFAULT_SILVER_WRITE_OPTIONS = {"overwriteSchema": "true"}


def transform_images(
    read_path: str,
    write_path_dim: str,
    write_path_bridge: str,
    write_mode: str = 'overwrite',
    write_options: Dict[str, str] = DEFAULT_SILVER_WRITE_OPTIONS
) -> None:
    """Transform image data from bronze to silver layer.

    Extracts images from albums and creates:
    1. A deduplicated dimension table (dim_images)
    2. A bridge table maintaining all album-image relationships

    Args:
        read_path: Path to bronze album data with nested images
        write_path_dim: Path to silver dim_images table
        write_path_bridge: Path to silver bridge_album_images table
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

        # Check how many albums have images
        albums_with_images = df.filter(F.size('images') > 0).count()
        logger.info(
            f"Found {albums_with_images} albums with image information")

        try:
            logger.info("Starting image extraction")

            # exploding images with album relationship
            df_exploded = df.select(
                F.explode_outer('images').alias('image'),
                F.col('id').alias('album_id')
            ).filter(
                F.col('image.url').isNotNull()  # remove null images
            )

            # adding image_id (using hash of URL as unique identifier)
            df_exploded = df_exploded.withColumn(
                'image_id', F.md5(F.col('image.url'))
            )

            # creating dimension table (unique images only)
            df_dim_images = df_exploded.select(
                'image_id',
                F.col('image.url').alias('image_url'),
                F.col('image.height').alias('image_height'),
                F.col('image.width').alias('image_width')
            ).dropDuplicates(['image_id']).withColumn(
                'image_size_category',
                F.when(F.col('image_width') <= 64, 'small')
                .when(F.col('image_width') <= 300, 'medium')
                .otherwise('large')
            ).withColumn(
                'aspect_ratio',
                F.round(F.col('image_width') / F.col('image_height'), 2)
            ).withColumn(
                'extraction_date', F.current_date()
            )

            # creatING bridge table (all album-image relationships)
            # Using posexplode to maintain image order (primary image is usually first)
            df_bridge = df.select(
                F.col('id').alias('album_id'),
                F.posexplode_outer('images').alias('image_position', 'image')
            ).filter(
                F.col('image.url').isNotNull()
            ).withColumn(
                'image_id', F.md5(F.col('image.url'))
            ).select(
                'album_id',
                'image_id',
                'image_position',  # 0 = primary/main image
                F.col('image.width').alias('width'),
                F.col('image.height').alias('height')
            ).withColumn(
                'is_primary_image', F.when(
                    F.col('image_position') == 0, True).otherwise(False)
            ).withColumn(
                'extraction_date', F.current_date()
            )

            # get counts for logging
            unique_images_count = df_dim_images.count()
            relationships_count = df_bridge.count()

            # additional metrics
            images_per_album = relationships_count / \
                initial_count if initial_count > 0 else 0

            # check for duplicate images across albums
            reused_images = df_bridge.groupBy('image_id').agg(
                F.countDistinct('album_id').alias('album_count')
            ).filter(F.col('album_count') > 1).count()

            logger.info(
                f"Extracted {unique_images_count} unique images from {initial_count} albums")
            logger.info(
                f"Created {relationships_count} album-image relationships")
            logger.info(f"Average images per album: {images_per_album:.2f}")
            logger.info(
                f"Found {reused_images} images used across multiple albums")

            if unique_images_count == 0:
                raise ValueError("No images extracted from albums")

        except ValueError:
            raise
        except Exception:
            logger.exception("Failed during image extraction")
            raise

        try:
            # write dimension table
            logger.info(
                f"Writing {unique_images_count} unique images to {write_path_dim}")
            writer(df_dim_images, write_path_dim,
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
