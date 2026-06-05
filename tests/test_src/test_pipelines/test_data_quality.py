import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType
)
from datetime import date


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_data_quality")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


class TestValidateReleaseDate:

    def test_passes_when_above_90_percent(self, spark, tmp_path):
        silver_path = str(tmp_path / "silver")
        fact_path = "fact_albums"

        rows = [(f"a{i}", date(2024, 1, i + 1)) for i in range(10)]
        schema = StructType([
            StructField("album_id", StringType()),
            StructField("release_date", DateType()),
        ])
        spark.createDataFrame(rows, schema).write.format("delta").save(f"{silver_path}/{fact_path}")

        with patch("src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data import validate_release_date
            assert validate_release_date(silver_path, fact_path) is True

    def test_fails_when_below_90_percent(self, spark, tmp_path):
        silver_path = str(tmp_path / "silver")
        fact_path = "fact_albums"

        rows = [("a1", date(2024, 1, 1))] + [(f"a{i}", None) for i in range(2, 12)]
        schema = StructType([
            StructField("album_id", StringType()),
            StructField("release_date", DateType()),
        ])
        spark.createDataFrame(rows, schema).write.format("delta").save(f"{silver_path}/{fact_path}")

        with patch("src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.data_quality_checks.silver_data_completeness_data import validate_release_date
            assert validate_release_date(silver_path, fact_path) is False
