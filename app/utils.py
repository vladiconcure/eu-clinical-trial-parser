import logging
import os
import json
from datetime import datetime
import pandas as pd
import pdfplumber
import zipfile
from io import BytesIO, StringIO


def setup_logging():
    """
    Sets up logging for the application.

    This function creates a log directory if it doesn't exist and sets up a log file with the current date as the filename.
    The log file will contain log messages with the format: "<timestamp> <log_level>: <message>".

    Args:
        None

    Returns:
        None
    """
    log_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_directory, exist_ok=True)
    log_filename = os.path.join(
        log_directory, datetime.now().strftime("%Y-%m-%d-%H-%M-%S") + "-run.log")

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_filename,
                        filemode='w')

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(levelname)s ->\t %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)


def write_json_to_disk(json_object, query_details):
    data_directory = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_directory, exist_ok=True)
    filename = f"""{query_details['start_date']}_{
    query_details['end_date']}_{query_details['run_date']}.json"""
    file_path = os.path.join(data_directory, filename)
    with open(file_path, 'w') as f:
        json.dump(json_object, f)
    logging.info(f"Successfully wrote JSON to disk: {filename}")


def extract_text_and_tables_from_pdf(zip_bytes):
    text = ""
    tables = []
    zip_in_memory = BytesIO(zip_bytes)

    with zipfile.ZipFile(zip_in_memory, 'r') as zip_ref:
        pdf_name = zip_ref.namelist()[0]
        with zip_ref.open(pdf_name) as pdf_file_in_zip:
            pdf_bytes = BytesIO(pdf_file_in_zip.read())
            with pdfplumber.open(pdf_bytes) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
                    page_tables = page.extract_tables()
                    for table in page_tables:
                        tables.append(table)
    return text, tables


def get_json_data_in_pandas(json_data):
    if not json_data["successes"] or len(json_data["successes"]) < 1:
        return None, None, None

    trial_info_cards_columns = ['eudract_number', 'start_date', 'sponsor_name',
                                'full_title', 'version', 'soc_term', 'classification_code', 'term', 'level', 'json']
    trial_protocols_columns = ['protocol_id', 'eudract_number', 'url', 'json']
    trial_results_columns = ['eudract_number', 'version', 'url', 'json']
    cards_df = pd.DataFrame(columns=trial_info_cards_columns)
    protocols_df = pd.DataFrame(columns=trial_protocols_columns)
    results_df = pd.DataFrame(columns=trial_results_columns)

    for result in json_data["successes"]:
        if not result:
            continue
        trial_info_card = result["card"]
        card_disease = trial_info_card["disease"]
        eudract_number = trial_info_card['eudract_number']
        trial_start_date = pd.to_datetime(trial_info_card['start_date'], errors='coerce')
        protocols = result['protocols']
        if pd.isnull(trial_start_date):
            trial_start_date = None

        if trial_info_card['full_title'].endswith('...'):
            trial_info_card['full_title'] = protocols[0]["A. Protocol Information"]["Full title of the trial"][0]

        new_card_info_row = [
            eudract_number,
            trial_start_date,
            trial_info_card['sponsor_name'],
            trial_info_card['full_title'],
            card_disease['version'],
            card_disease['soc_term'],
            card_disease['classification_code'],
            card_disease['term'],
            card_disease['level'],
            json.dumps(result)
        ]
        cards_df.loc[len(cards_df)] = new_card_info_row

        # protocol id is unique because it combines the eudract number and the protocol letters/digits
        for protocol in protocols:
            protocol_id = protocol['url'].split('/')
            protocol_id = '-'.join(protocol_id[-2:])

            protocols_df.loc[len(protocols_df)] = [protocol_id, eudract_number, protocol['url'], json.dumps(protocol)]

        results = result['results'] if 'results' in result else None
        if not results:
            continue
        for version, value in results.items():
            results_df.loc[len(results_df)] = [eudract_number, version, value['summary']['url'], json.dumps(value)]
        logging.info(f"Cards: {cards_df.shape}, Protocols: {protocols_df.shape}, Results: {results_df.shape}")
    return cards_df, protocols_df, results_df


def write_csv_to_s3(json_object, query_details):
    data_directory = os.path.join(os.path.dirname(__file__), '..', 'data')
    os.makedirs(data_directory, exist_ok=True)
    cards_filename = f"trial_info_cards_{query_details['start_date']}_{query_details['end_date']}_{query_details['run_date']}.csv"
    protocols_filename = f"trial_protocols_{query_details['start_date']}_{query_details['end_date']}_{query_details['run_date']}.csv"
    results_filename = f"trial_results_{query_details['start_date']}_{query_details['end_date']}_{query_details['run_date']}.csv"
    cards_file_path = os.path.join(data_directory, cards_filename)
    protocols_file_path = os.path.join(data_directory, protocols_filename)
    results_file_path = os.path.join(data_directory, results_filename)

    cards_df, protocols_df, results_df = get_json_data_in_pandas(json_object)
    if cards_df is None or protocols_df is None or results_df is None:
        logging.warning(
            f"Failed to write data to disk. No data to write for {query_details['start_date']} to {query_details['end_date']}")
        return

    cards_df.to_csv(cards_file_path, index=False)
    protocols_df.to_csv(protocols_file_path, index=False)
    results_df.to_csv(results_file_path, index=False)
