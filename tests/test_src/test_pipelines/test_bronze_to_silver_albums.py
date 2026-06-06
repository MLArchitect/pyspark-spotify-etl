import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    ArrayType, DateType
)


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_albums")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def _make_bronze_df(spark, rows):
    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("release_date", StringType()),
        StructField("release_date_precision", StringType()),
        StructField("total_tracks", IntegerType()),
        StructField("type", StringType()),
        StructField("album_type", StringType()),
        StructField("available_markets", ArrayType(StringType())),
        StructField("external_urls", StructType([
            StructField("spotify", StringType())
        ])),
        StructField("href", StringType()),
        StructField("uri", StringType()),
    ])
    return spark.createDataFrame(rows, schema)


class TestTransformAlbums:

    def test_deduplicates_by_album_id(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        rows = [
            ("a1", "Album One", "2024-01-15", "day", 10, "album", "album",
             ["US", "UK"], Row(spotify="http://spot.fy/a1"), "href1", "uri1"),
            ("a1", "Album One Dup", "2024-01-15", "day", 10, "album", "album",
             ["US"], Row(spotify="http://spot.fy/a1"), "href1", "uri1"),
            ("a2", "Album Two", "2024-03-20", "day", 8, "album", "single",
             ["US", "DE", "FR"], Row(spotify="http://spot.fy/a2"), "href2", "uri2"),
        ]

        df = _make_bronze_df(spark, rows)
        df.write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums import transform_albums
            transform_albums(bronze_path, silver_path)

        result = spark.read.format("delta").load(silver_path)
        assert result.count() == 2
        assert result.select("album_id").distinct().count() == 2

    def test_derived_columns_are_created(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        rows = [
            ("a1", "Test Album", "2024-06-15", "day", 12, "album", "album",
             ["US", "UK", "DE"], Row(spotify="http://spot.fy/a1"), "href", "uri"),
        ]

        df = _make_bronze_df(spark, rows)
        df.write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums import transform_albums
            transform_albums(bronze_path, silver_path)

        result = spark.read.format("delta").load(silver_path).collect()[0]
        assert result["release_year"] == 2024
        assert result["release_month"] == 6
        assert result["available_markets_count"] == 3

    def test_null_album_id_filtered_out(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        rows = [
            (None, "No ID Album", "2024-01-01", "day", 5, "album", "album",
             ["US"], Row(spotify="http://spot.fy/x"), "href", "uri"),
            ("a1", "Valid Album", "2024-01-01", "day", 5, "album", "album",
             ["US"], Row(spotify="http://spot.fy/a1"), "href", "uri"),
        ]

        df = _make_bronze_df(spark, rows)
        df.write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums import transform_albums
            transform_albums(bronze_path, silver_path)

        result = spark.read.format("delta").load(silver_path)
        assert result.count() == 1

    def test_empty_input_raises_error(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        df = _make_bronze_df(spark, [])
        df.write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_albums import transform_albums

            with pytest.raises(ValueError, match="no data found"):
                transform_albums(bronze_path, silver_path)
