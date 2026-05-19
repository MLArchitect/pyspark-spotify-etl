from utils.logger import get_logger
from pyspark.sql import DataFrame
from typing import Optional, Dict

logger = get_logger()


def writer(
        df: DataFrame,
        save_path: str,
        mode: str = 'overwrite',
        partition_by: Optional[list] = None,
        fmt: str = 'delta',
        options: Optional[Dict[str, str]] = None) -> None:
    """General function: accepts a spark dataframe and writes it to the given path, with the given format. can be extended with options

    Args:
        df (DataFrame): a spark dataframe as the main input
        save_path (str): save to this path
        mode (str, optional): choose from overwrite, append, ignore or error. Defaults to 'overwrite'.
        partition_by (Optional[list], optional): to partition the data by a list of columns. Defaults to None.
        fmt (str, optional): output of the function should be in this format. Defaults to 'delta'.
        options (Optional[Dict[str, str]], optional): extend the function with any number of options. Defaults to None.
    """
    try:
        logger.info(
            f'start writing data to path : {save_path}, format : {fmt}, options : {options}')
        writer = df.write.mode(mode)
        if options:
            writer = writer.options(**options)

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.format(fmt).save(save_path)
        logger.info(f'successfully wrote data to {save_path}')

    except Exception as e:
        logger.exception(
            f'unexpected error in saving the data to path: {save_path} as {e}')
        raise

    finally:
        logger.info('write function call ended')
