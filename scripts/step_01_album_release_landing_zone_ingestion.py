# to call the api and ingest the data to the landing zone

import argparse

from src.pipelines.album_release.extraction_layer.album_search_data_extractor import ingestion
from src.general_functions.access_token_generator import generate_temp_token
from utils.config import token_url, LANDING_ZONE_PATH


def run():
    """Entry point"""
    parser = argparse.ArgumentParser(
        description="arguments used in the ingestion layer")

    parser.add_argument(
        "--days",
        required=False,
        default=7,
        type=int,
        help="number of the last days to look back and read the data"
    )

    parser.add_argument(
        "--path",
        required=False,
        default=LANDING_ZONE_PATH,
        type=str,
        help='path to ingest the initial read data'
    )
    args = parser.parse_args()

    access_token = generate_temp_token(token_url)
    ingestion(args.path, args.days, access_token)


if __name__ == '__main__':
    run()
