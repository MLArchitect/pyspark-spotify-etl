import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType
)


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_artists")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def _make_bronze_df(spark, rows):
    artist_schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("type", StringType()),
        StructField("uri", StringType()),
        StructField("href", StringType()),
        StructField("external_urls", StructType([
            StructField("spotify", StringType())
        ])),
    ])
    schema = StructType([
        StructField("id", StringType()),
        StructField("artists", ArrayType(artist_schema)),
    ])
    return spark.createDataFrame(rows, schema)


class TestTransformArtists:

    def test_creates_dim_and_bridge_tables(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        dim_path = str(tmp_path / "dim_artists")
        bridge_path = str(tmp_path / "bridge")

        artist1 = Row(id="ar1", name="Artist One", type="artist",
                       uri="uri1", href="href1", external_urls=Row(spotify="http://s/ar1"))
        artist2 = Row(id="ar2", name="Artist Two", type="artist",
                       uri="uri2", href="href2", external_urls=Row(spotify="http://s/ar2"))

        rows = [
            ("alb1", [artist1, artist2]),
            ("alb2", [artist1]),
        ]

        _make_bronze_df(spark, rows).write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists import transform_artists
            transform_artists(bronze_path, dim_path, bridge_path)

        dim = spark.read.format("delta").load(dim_path)
        bridge = spark.read.format("delta").load(bridge_path)

        assert dim.count() == 2
        assert bridge.count() == 3

    def test_deduplicates_artists_in_dim(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")
        dim_path = str(tmp_path / "dim_artists")
        bridge_path = str(tmp_path / "bridge")

        same_artist = Row(id="ar1", name="Same Artist", type="artist",
                          uri="uri1", href="href1", external_urls=Row(spotify="http://s/ar1"))

        rows = [
            ("alb1", [same_artist]),
            ("alb2", [same_artist]),
            ("alb3", [same_artist]),
        ]

        _make_bronze_df(spark, rows).write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists import transform_artists
            transform_artists(bronze_path, dim_path, bridge_path)

        dim = spark.read.format("delta").load(dim_path)
        assert dim.count() == 1

    def test_empty_input_raises_error(self, spark, tmp_path):
        bronze_path = str(tmp_path / "bronze")

        _make_bronze_df(spark, []).write.format("delta").save(bronze_path)

        with patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_spark", return_value=spark), \
             patch("src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists.get_logger", return_value=MagicMock()):

            from src.pipelines.album_release.silver_layer.transformations.bronze_to_silver_artists import transform_artists

            with pytest.raises(ValueError, match="No data found"):
                transform_artists(bronze_path, str(tmp_path / "dim"), str(tmp_path / "bridge"))
