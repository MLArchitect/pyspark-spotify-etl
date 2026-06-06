from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.upsert_into_path import upsert
from src.general_functions.write_into_path import writer
from utils.logger import get_logger
from typing import Dict
from datetime import datetime


spark = get_spark()
logger = get_logger()


def create_artist_album_summary(
    silver_path: str,
    gold_path: str,
    gold_artists_path: str,
    fact_albums_path: str,
    dim_artists_path: str,
    bridge_table_path: str,
    SAFE_BROADCAST_ROWS: int = 1000000,
    write_mode: str = 'upsert'
) -> None:
    """Creating artist album summary gold table with aggregated metrics.

    Args:
        silver_path: Path to silver layer
        gold_path: Path to gold artist_album_summary table
        write_mode: Write mode (upsert/overwrite)
    """

    try:
        # loading silver tables
        logger.info("Loading silver layer tables")
        df_albums = read_data(
            spark, f'{silver_path}/{fact_albums_path}', 'delta').cache()
        df_bridge = read_data(
            spark, f'{silver_path}/{bridge_table_path}', 'delta').cache()
        df_artists = read_data(
            spark, f'{silver_path}/{dim_artists_path}', 'delta').cache()

        # checking size of the dim tables before broadcasting
        counts = {
            'albums': df_albums.count(),
            'bridge': df_bridge.count(),
            'artists': df_artists.count()
        }

        logger.info(f"Table sizes: {counts}")

        broadcast_strategy = {
            'bridge': counts['bridge'] < SAFE_BROADCAST_ROWS,
            'artists': counts['artists'] < SAFE_BROADCAST_ROWS}

        logger.info(f"Broadcast strategy: {broadcast_strategy}")

        # conditional broadcasting
        df_bridge_join = F.broadcast(
            df_bridge) if broadcast_strategy['bridge'] else df_bridge
        df_artists_join = F.broadcast(
            df_artists) if broadcast_strategy['artists'] else df_artists

        # creating a base dataset with all artist-album relationships
        df_base = df_albums.alias('a').join(
            df_bridge_join.alias('br'),
            F.col('a.album_id') == F.col('br.album_id'),
            'inner').join(
            df_artists_join.alias('ar'),
            F.col('br.artist_id') == F.col('ar.artist_id'),
            'inner')

        # creating aggregated metrics per artist
        df_summary = df_base.groupBy(
            F.col('ar.artist_id'),
            F.col('ar.artist_name'),
            F.col('ar.artist_type'),
            F.col('ar.spotify_url')
        ).agg(
            # album counts by type
            F.count('a.album_id').alias('total_albums'),
            F.sum(F.when(F.col('a.album_type') == 'album',
                  1).otherwise(0)).alias('full_albums_count'),
            F.sum(F.when(F.col('a.album_type') == 'single',
                  1).otherwise(0)).alias('singles_count'),
            F.sum(F.when(F.col('a.album_type') == 'compilation',
                  1).otherwise(0)).alias('compilations_count'),

            # track metrics
            F.avg('a.total_tracks').alias('avg_tracks_per_release'),
            F.sum('a.total_tracks').alias('total_tracks_released'),

            # time span metrics
            F.min('a.release_date').alias('first_release_date'),
            F.max('a.release_date').alias('latest_release_date'),
            F.countDistinct('a.release_year').alias('active_years_count'),

            # Market reach
            F.avg('a.available_markets_count').alias('avg_market_reach'),
            F.max('a.available_markets_count').alias('max_market_reach'),

            # Release patterns
            F.collect_set('a.release_year').alias('release_years_array'),
            F.countDistinct('a.album_name').alias('unique_albums_count')
        )

        # Adding partitioning column based on activity level
        df_summary = df_summary.withColumn(
            'artist_tier',
            F.when(F.col('total_albums') >= 20, 'high_volume')
            .when(F.col('total_albums') >= 5, 'medium_volume')
            .otherwise('low_volume')
        )

        # Logging summary stats
        total_artists = df_summary.count()
        logger.info(f"Created summary for {total_artists} artists")

        # Using upsert for incremental updates
        if write_mode == 'upsert':
            logger.info(f"Upserting data to {gold_path}/{gold_artists_path}")
            result = upsert(
                df_new=df_summary,
                output_path=f"{gold_path}/{gold_artists_path}",
                primary_key_cols=['artist_id'],
                partition_by=['artist_tier'],
                enable_schema_evolution=True
            )
            logger.info(f"Upsert results: {result}")
        else:
            # Using the writer function instead
            logger.info(f"Overwriting data to {gold_path}/{gold_artists_path}")
            writer(
                df_summary,
                f"{gold_path}/{gold_artists_path}",
                mode='overwrite',
                options={'overwriteSchema': 'true'},
                partition_by=['artist_tier']
            )
        logger.info("Artist album summary creation completed successfully")

        try:
            # Optimizing with Z-ORDER
            logger.info("Optimizing table with Z-ORDER")

            delta_table = DeltaTable.forPath(
                spark, f"{gold_path}/{gold_artists_path}")
            delta_table.optimize().executeZOrderBy(
                "artist_name", "total_albums", "latest_release_date")

            logger.info("Z-ORDER optimization completed successfully")
        except Exception as e:
            logger.exception(f"failed at z-ordering. error {e}")
    except Exception as e:
        logger.exception("Failed to create artist album summary")
        raise
    finally:
        df_albums.unpersist()
        df_bridge.unpersist()
        df_artists.unpersist()
