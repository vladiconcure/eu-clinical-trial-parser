from bs4 import BeautifulSoup
import logging
import re
import requests

from app.card_parser import CardParser
from app.protocol_parser import ProtocolParser
from app.result_parser import ResultParser

# Time in seconds between requests
REQUEST_DELAY = 10

# Maximum backoff time in seconds (5 minutes)
MAX_BACKOFF_TIME = 300

# Base URL for the EU Clinical Trials Register
BASE_URL = "https://www.clinicaltrialsregister.eu/"

# Custom headers for HTTP requests
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36"
}


class EUClinicalTrialsScraper:
    """
    A class for scraping data from the EU Clinical Trials Register.
    """

    def __init__(self, start_date, end_date):
        """
        Initialize the EUClinicalTrialsScraper object.

        Args:
            start_date (datetime): The start date for the data query.
            end_date (datetime): The end date for the data query.
        """
        self.start_date = start_date
        self.end_date = end_date
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.results = {"errors": [], "successes": []}
        self.current_trial_num = 0

    def scrape_trials(self):
        """
        Scrape trials from the EU Clinical Trials Register website.

        Returns:
            dict: A dictionary containing scraped document details and errors.
        """
        try:
            soup = self.get_search_page()

            num_pages, num_results = self.get_num_pages_and_results(soup)
            if not num_results:
                return self.results
            logging.info(
                f"Number of pages: {num_pages}, Number of results: {num_results}")
            self.scrape_page(soup)
            if num_pages > 1:
                for page_number in range(2, num_pages + 1):
                    try:
                        soup = self.get_search_page(page_number)
                        self.scrape_page(soup)

                    except Exception as e:
                        self.results["errors"].append(
                            f"Error scraping page {page_number}: {str(e)}")
                        logging.error(
                            f"Error scraping page {page_number}: {str(e)}")

        except Exception as e:
            self.results["errors"].append(
                f"Error during initial page retrieval: {str(e)}")
        return self.results

    def scrape_page(self, soup):
        """
        Scrape data from a single page of the EU Clinical Trials Register.

        Iterates over each trial listing found on the page, extracts relevant data,
        and appends it to the results dictionary.

        Args:
            soup (BeautifulSoup): The BeautifulSoup object containing the HTML of the page.

        Updates:
            self.results["successes"]: List of dictionaries containing trial data.
            self.results["errors"]: List of error messages encountered during scraping.
        """
        try:
            data_section = soup.find("div", {"id": "tabs"})
            for card in data_section.find_all("table", {"class": "result"}):
                self.current_trial_num += 1
                logging.info(f"Scraping trial {self.current_trial_num}...")
                trial_data = self.get_trial_data(card)
                self.results["successes"].append(trial_data)
        except Exception as e:
            self.results["errors"].append(
                f"Error scraping trial data: {str(e)}")

    def get_search_page(self, page_number=None):
        """
        Retrieve the search page from the EU Clinical Trials Register.

        Constructs the search URL with the specified date range and page number,
        sends a GET request, and returns the HTML content as a BeautifulSoup object.

        Args:
            page_number (int, optional): The page number to retrieve. Defaults to None.

        Returns:
            BeautifulSoup: The BeautifulSoup object of the search page HTML.

        Raises:
            Exception: If the page retrieval fails.
        """
        try:
            search_url = f"{BASE_URL}/ctr-search/search?query=&dateFrom={self.start_date}&dateTo={self.end_date}" + (
                f"&page={page_number}" if page_number else "")
            response = self.session.get(search_url)
            response.raise_for_status()
            return BeautifulSoup(response.text, "html.parser")
        except Exception as e:
            raise Exception(f"Failed to retrieve search page: {str(e)}")

    def get_trial_data(self, trial_card):
        """
        Extract detailed data for a single trial using its card information.

        Gathers comprehensive details for a trial, including card data, protocol URLs,
        and results data, compiling everything into a dictionary.

        Args:
            trial_card (Tag): The BeautifulSoup Tag object representing the trial card.

        Returns:
            dict or None: A dictionary containing detailed trial data, or None if an error occurs.

        Updates:
            self.results["errors"]: List of error messages encountered during data retrieval.
        """
        try:
            trial_data = {}
            trial_data["card"] = self.get_card_data(trial_card)
            protocols_urls = [protocol["protocol_url"] for protocol in trial_data["card"]["trial_protocols"] if
                              protocol["protocol_url"]]
            trial_data["protocols"] = self.get_protocols_data(protocols_urls)
            results_url = trial_data["card"]["trial_results_link"]
            if results_url:
                trial_data["results"] = self.get_results(results_url)
            return trial_data
        except Exception as e:
            self.results["errors"].append(
                f"Error retrieving trial data: {str(e)}")
            return None

    def get_card_data(self, card):
        """
        Parse the main information from a trial card.

        Utilizes a CardParser instance to extract data from a trial card.

        Args:
            card (Tag): The BeautifulSoup Tag object for the trial card.

        Returns:
            dict: Parsed data from the trial card.

        Raises:
            Exception: If parsing the card data fails.
        """
        try:
            card_parser = CardParser(card)
            return card_parser.parse()
        except Exception as e:
            raise Exception(f"Failed to parse card data: {str(e)}")

    def get_protocols_data(self, protocols_urls):
        """
        Retrieve and parse data from trial protocol URLs.

        Iterates over a list of protocol URLs, fetches their content, and parses
        the data, compiling a list of dictionaries with protocol details.

        Args:
            protocols_urls (list of str): URLs to the trial protocols.

        Returns:
            list of dict: A list of dictionaries, each containing data for a single protocol.

        Updates:
            self.results["errors"]: List of error messages encountered during protocol data retrieval.
        """
        protocols = []
        for protocol_url in protocols_urls:
            try:
                response = self.session.get(protocol_url)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, "html.parser")
                protocol_data = {"url": protocol_url}
                protocol_parser = ProtocolParser(soup)
                protocol_data.update(protocol_parser.parse())
                protocols.append(protocol_data)
            except Exception as e:
                self.results["errors"].append(
                    f"Error retrieving protocol data for {protocol_url}: {str(e)}")
        return protocols

    def get_results(self, results_url):
        """
        Fetch and parse the results data for a specific trial.

        Sends a GET request to the specified results URL, parses the response content
        to extract detailed results data using a ResultParser instance.

        Args:
            results_url (str): The URL where the trial's results data is located.

        Returns:
            dict: The parsed results data.

        Raises:
            Exception: If retrieval or parsing of results data fails.
        """
        try:
            response = self.session.get(results_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            result_parser = ResultParser(
                soup, url=results_url, session=self.session)
            return result_parser.parse()
        except Exception as e:
            raise Exception(f"Failed to retrieve results data: {str(e)}")

    def get_num_pages_and_results(self, initial_search_page):
        """
        Determine the total number of pages and results from the initial search page.

        Parses the initial search page to find the total number of results and how
        many pages of results there are, using regex to extract these numbers from
        the page text.

        Args:
            initial_search_page (BeautifulSoup): The BeautifulSoup object of the initial search page.

        Returns:
            tuple: A tuple containing the total number of pages (int) and the total number of results (int).

        Raises:
            Exception: If unable to parse the number of pages and results from the initial search page.
        """
        try:
            data_section = initial_search_page.find(
                "div", {"id": "tabs-1"}).find("div", {"class": "outcome"})
            if not data_section:
                return None
            pattern = r"(\d+) result\(s\) found.*page \d+ of (\d+)"
            text = re.sub(r'\s+', ' ', data_section.text.strip().replace("  ", "").replace(",", "")).strip()
            match = re.search(pattern, text)
            if not match:
                return None
            results = match.group(1)
            pages = match.group(2)
            return int(pages), int(results)
        except Exception as e:
            raise Exception(
                f"Failed to determine number of pages and results: {str(e)}")
