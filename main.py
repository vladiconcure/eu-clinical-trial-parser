import argparse
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from app.eu_scraper import EUClinicalTrialsScraper
from app.utils import setup_logging

HERE = Path(os.path.abspath(os.path.dirname(__file__)))
DATA_DIR = HERE.parent / "data"


def parse_args():
    parser = argparse.ArgumentParser(description='EU Clinical Trials Scraper')
    parser.add_argument('--start-date', type=str, required=True,
                        help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end-date', type=str, required=True,
                        help='End date in YYYY-MM-DD format')
    return parser.parse_args()


def validate_dates(start_date, end_date):
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        if start_date > end_date:
            raise ValueError("Start date cannot be after end date.")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Date error: {e}")
    return start_date, end_date


def scrape_by_date_range(start_date, end_date):
    scraper = EUClinicalTrialsScraper(start_date, end_date)
    results = scraper.scrape_trials()
    output = {
        "metadata": {
            "query_start_date": start_date.strftime("%Y-%m-%d"),
            "query_end_date": end_date.strftime("%Y-%m-%d"),
            "run_start_datetime": datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        },
        "errors": results["errors"],
        "successes": results["successes"]
    }
    query_details = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "run_date": datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    }
    return output, query_details


def main():
    setup_logging()
    load_dotenv()
    args = parse_args()

    DATA_DIR.mkdir(exist_ok=True, parents=True)

    start_date, end_date = validate_dates(args.start_date, args.end_date)

    current_date = start_date
    while current_date <= end_date:
        logging.info(f"Scraping data for {current_date}")
        output, query_details = scrape_by_date_range(
            current_date, current_date)
        logging.info(f"Scraping complete for {current_date}")

        current_folder = DATA_DIR / current_date.strftime("%Y-%m-%d")
        current_folder.mkdir(exist_ok=True)

        with (current_folder / "query_details.json").open("w") as f:
            json.dump(query_details, f)

        with (current_folder / "successes.jsonl").open("w") as f:
            for result in output["successes"]:
                if not result:
                    continue
                f.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")

        with (current_folder / "errors.jsonl").open("w") as f:
            for result in output["errors"]:
                if not result:
                    continue
                f.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")

        current_date += timedelta(days=1)


if __name__ == "__main__":
    main()
