import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .master("local[1]")
        .appName("test_upsert")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


class TestUpsertValidation:

    @patch("src.general_functions.upsert_into_path.get_spark")
    @patch("src.general_functions.upsert_into_path.get_logger")
    def test_empty_dataframe_returns_zero_metrics(self, mock_logger, mock_spark, spark):
        mock_spark.return_value = spark
        mock_logger.return_value = MagicMock()

        from src.general_functions.upsert_into_path import upsert

        df_empty = spark.createDataFrame([], "id STRING, name STRING")
        result = upsert(df_empty, "/tmp/test_empty", primary_key_cols=["id"])

        assert result == {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

    @patch("src.general_functions.upsert_into_path.get_spark")
    @patch("src.general_functions.upsert_into_path.get_logger")
    def test_missing_pk_columns_raises_value_error(self, mock_logger, mock_spark, spark):
        mock_spark.return_value = spark
        mock_logger.return_value = MagicMock()

        from src.general_functions.upsert_into_path import upsert

        df = spark.createDataFrame([("1", "test")], ["id", "name"])

        with pytest.raises(ValueError, match="Primary key columns not found"):
            upsert(df, "/tmp/test_missing_pk", primary_key_cols=["nonexistent_col"])

    @patch("src.general_functions.upsert_into_path.get_spark")
    @patch("src.general_functions.upsert_into_path.get_logger")
    def test_null_pk_rows_are_filtered(self, mock_logger, mock_spark, spark, tmp_path):
        mock_spark.return_value = spark
        mock_logger.return_value = MagicMock()

        from src.general_functions.upsert_into_path import upsert

        df = spark.createDataFrame(
            [("1", "album_a"), (None, "album_b"), ("3", "album_c")],
            ["album_id", "name"]
        )

        output_path = str(tmp_path / "test_null_filter")
        result = upsert(df, output_path, primary_key_cols=["album_id"])

        assert result["rows_inserted"] == 2

        written = spark.read.format("delta").load(output_path)
        assert written.count() == 2
        assert written.filter(F.col("album_id").isNull()).count() == 0
