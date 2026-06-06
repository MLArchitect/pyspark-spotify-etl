from utils.logger import get_logger
from src.general_functions.spark_manager import get_spark
from pyspark.sql import DataFrame
from typing import Optional, List, Dict, Any
from delta.tables import DeltaTable
from pyspark.sql import functions as F

logger = get_logger()
spark = get_spark()


def upsert(
    df_new: DataFrame,
    output_path: str,
    primary_key_cols: Optional[List[str]] = None,
    partition_by: Optional[List[str]] = None,
    enable_schema_evolution: bool = True,
    replace_where_condition: Optional[str] = None
) -> Dict[str, int]:
    """Upsert results with partitioning and replacing capabilities

    Args:
        df_new: DataFrame to upsert
        output_path: Target Delta table path
        primary_key_cols: Primary key columns for merge condition
        partition_by: Columns to partition by
        enable_schema_evolution: Whether to allow schema changes
        replace_where_condition: Optional condition for replaceWhere operation

    Returns:
        Dict with operation metrics
    """
    try:
        # checking for empty df
        df_new.cache()
        row_count = df_new.count()

        if row_count == 0:
            logger.warning(f"input df is empty, skipping operation...")
            return {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

        # handling empty primary key columns
        if not primary_key_cols:
            logger.warning(
                "no primary key columns provided, using all columns for comparison")
            primary_key_cols = df_new.columns

        # checking pk columns exist in df new
        missing_columns = set(primary_key_cols) - set(df_new.columns)
        if missing_columns:
            logger.exception(
                f"primary key columns not found in df_new, missing columns: {missing_columns}")
            raise ValueError

        # find if any of the rows of the primary key cols have null values
        null_count = df_new.filter(
            F.expr(" OR ".join([f"{col} IS NULL" for col in primary_key_cols]))
        ).count()

        # filter out any rows with null values in any of the primary key cols
        if null_count > 0:
            logger.warning(
                f"found {null_count} rows with null values in primary key columns, filtering them out")
            df_new = df_new.filter(
                F.expr(" AND ".join(
                    [f"{col} IS NOT NULL" for col in primary_key_cols]))
            )
            # update rowcount for logging
            row_count = df_new.count()

        logger.info(f"checking if a table exists at path {output_path}")

        # first time table creation
        if not DeltaTable.isDeltaTable(spark, output_path):
            logger.info(
                f"table not found at path {output_path}, writing to path for the first time...")

            writer = df_new.write.format('delta')

            if partition_by:
                writer = writer.partitionBy(*partition_by)

            writer.save(output_path)
            logger.info(
                f"Successfully created new table with {row_count} rows")
            return {"rows_inserted": row_count, "rows_updated": 0, "rows_deleted": 0}

        else:
            logger.info(
                f"an existing table found at the given path, performing merge ...")
            delta_table = DeltaTable.forPath(spark, output_path)

            # replace the whole partition with new data if it's provided
            if replace_where_condition:
                logger.info(
                    f"using replaceWhere with condition {replace_where_condition}")
                # count existing rows that will be replaced
                existing_count = spark.read.format("delta").load(output_path)\
                    .filter(replace_where_condition).count()

                writer = df_new.write.format('delta').mode('overwrite')
                if enable_schema_evolution:
                    writer = writer.option('mergeSchema', 'true')
                writer.option('replaceWhere', replace_where_condition).save(
                    output_path)

                new_count = df_new.count()
                logger.info(
                    f"ReplaceWhere operation completed: replaced {existing_count} rows with {new_count} rows")
                return {"rows_inserted": new_count - existing_count, "rows_updated": min(existing_count, new_count), "rows_deleted": 0}
            else:
                merge_condition = " AND ".join(
                    [f"target.{col} = source.{col}" for col in primary_key_cols])  # type: ignore

                logger.info("starting the merge operation .... ")

                merge_builder = delta_table.alias("target").merge(
                    df_new.alias("source"),
                    merge_condition
                )

                if enable_schema_evolution:
                    spark.conf.set(
                        "spark.databricks.delta.schema.autoMerge.enabled", "true")

                merge_result = merge_builder\
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                    .execute()

                # returning what happened during the merge
                history_df = delta_table.history(1)
                operation_metrics = history_df.select(
                    "operationMetrics").collect()[0][0]

                if operation_metrics:
                    rows_inserted = operation_metrics.get(
                        "numTargetRowsInserted", 0)
                    rows_updated = operation_metrics.get(
                        "numTargetRowsUpdated", 0)
                    rows_deleted = operation_metrics.get(
                        "numTargetRowsDeleted", 0)

                    logger.info(
                        f"Merge completed - Inserted: {rows_inserted}, Updated: {rows_updated}, Deleted: {rows_deleted}")
                    return {"rows_inserted": int(rows_inserted), "rows_updated": int(rows_updated), "rows_deleted": int(rows_deleted)}
                else:
                    logger.warning("Could not retrieve operation metrics")
                    return {"rows_inserted": 0, "rows_updated": 0, "rows_deleted": 0}

    except Exception as e:
        logger.exception(f"unexpected error happened: {e}")
        raise
    finally:
        df_new.unpersist()
