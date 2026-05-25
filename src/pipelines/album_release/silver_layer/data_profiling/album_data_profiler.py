from pyspark.sql import functions as F
from utils.logger import get_logger
from pyspark.sql import DataFrame


logger = get_logger(__name__)


def get_basic_stats(df: DataFrame):
    """Get basic statistics for album data

    Args:
        df (DataFrame): pyspark dataframe to be profiled

    Returns:
        dictionary: a dictionary showing the overall profiling stats 
    """
    logger.info("Starting basic statistics calculation")

    df.cache()
    row_count = df.count()
    if row_count == 0:
        logger.warning("found 0 records in the given dataset")
        df.unpersist()
        return {"error": "no data to profile"}

    # Calculate nulls for each column
    null_counts = {}
    unique_counts = {}
    null_percentages = {}
    uniqueness_percentages = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        unique_count = df.select(col).distinct().count()

        null_counts[col] = null_count
        null_percentages[col] = round((null_count / row_count) * 100, 2)
        unique_counts[col] = unique_count
        uniqueness_percentages[col] = round(
            (unique_count / row_count) * 100, 2)

    stats = {
        "total_rows": row_count,
        "total_columns": len(df.columns),
        "unique_albums_count": df.select("album_id").distinct().count(),
        "null_counts": null_counts,
        "null_percentages": null_percentages,
        "unique_count": unique_counts,
        "unique_percentages": uniqueness_percentages
    }

    if "release_date" in df.columns:
        stats["date_range"] = {
            "earliest": df.agg(F.min("release_date")).collect()[0][0],
            "latest": df.agg(F.max("release_date")).collect()[0][0]
        }

    df.unpersist()
    logger.info(f"profiled {row_count} records successfully")

    return stats


def validate_critical_fields(df: DataFrame, required_fields: list = None):
    """Validate that critical fields meet business rules

    Args:
        df (DataFrame): pyspark dataframe to validate
        required_fields (list, optional): fields that must exist and be complete

    Returns:
        dict: validation results
    """
    logger.info("Starting critical field validation")

    if not required_fields:
        required_fields = ["album_id", "album_name", "release_date"]

    df.cache()
    row_count = df.count()

    if row_count == 0:
        logger.warning("Cannot validate empty dataframe")
        df.unpersist()
        return {"error": "no data to validate"}

    validation_results = {}

    # Validate required fields
    for field in required_fields:
        if field in df.columns:
            non_null = df.filter(F.col(field).isNotNull()).count()
            validation_results[field] = {
                "exists": True,
                "non_null_count": non_null,
                "completeness_%": round((non_null / row_count) * 100, 2)
            }
        else:
            validation_results[field] = {"exists": False}
            logger.error(f"Required field '{field}' missing")

    # Business rules
    validation_results["business_rules"] = {}
    if "album_id" in df.columns:
        validation_results["business_rules"]["unique_album_ids"] = df.select(
            "album_id").distinct().count() == row_count
    if "release_date" in df.columns:
        validation_results["business_rules"]["valid_dates"] = df.filter(
            F.col("release_date") <= F.current_date()).count() == row_count
    if "total_tracks" in df.columns:
        validation_results["business_rules"]["has_tracks"] = df.filter(
            F.col("total_tracks") > 0).count() == row_count

    df.unpersist()
    logger.info("Validation completed")

    return validation_results
