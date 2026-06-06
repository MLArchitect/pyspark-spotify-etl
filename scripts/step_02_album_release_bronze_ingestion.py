# reads from the landing zone path and writes as delta lake into bronze layer
from utils.config import LANDING_ZONE_PATH, BRONZE_PATH
from src.general_functions.read_path_into_spark import read_data
from src.general_functions.write_into_path import writer
from src.general_functions.spark_manager import get_spark


def run(LANDING_ZONE_PATH, BRONZE_PATH, **options):
    spark = get_spark()
    # read the data into a spark df
    spark_df = read_data(spark, LANDING_ZONE_PATH,  **options)

    # write data into a location
    writer(spark_df, BRONZE_PATH)


if __name__ == "__main__":
    run(LANDING_ZONE_PATH, BRONZE_PATH,
        fmt='json', options={'multiline': True})
