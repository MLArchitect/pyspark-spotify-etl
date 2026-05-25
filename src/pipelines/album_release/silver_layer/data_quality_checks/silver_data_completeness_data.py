from pyspark.sql import functions as F
from src.general_functions.spark_manager import get_spark
from src.general_functions.read_path_into_spark import read_data
from utils.logger import get_logger

spark = get_spark()
logger = get_logger()


def validate_release_date(silver_path: str, fact_albums_path: str) -> bool:
    """Running completeness test for the fact data model

    Args:
        silver_path: Base path to silver layer
        fact_albums_path: sub directory path to fact albums model

    Returns:
        bool: True if all validations pass
    """
    try:
        # Loading fact_albums for release_date check
        df_albums = read_data(
            spark, f'{silver_path}/{fact_albums_path}', 'delta')

        # Checking release_date completeness (>90%)
        total_albums = df_albums.count()
        albums_with_release_date = df_albums.filter(
            F.col('release_date').isNotNull()
        ).count()

        completeness_pct = (albums_with_release_date /
                            total_albums) * 100 if total_albums > 0 else 0

        if completeness_pct < 90:
            logger.error(
                f"FAILED: Only {completeness_pct:.1f}% of albums have release_date")
            return False
        else:
            logger.info(
                f"PASSED: {completeness_pct:.1f}% of albums have release_date")
            return True

    except Exception as e:
        logger.exception("Failed to validate critical fields")
        raise
