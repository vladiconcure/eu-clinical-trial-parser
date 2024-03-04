from bs4 import BeautifulSoup
import re

from app.utils import extract_text_and_tables_from_pdf


class ResultParser:
    """
    Parses detailed results data from a specific trial results page.

    This class processes a trial's results page to extract summary information, 
    results information, additional information such as trial protocols, and 
    PDF data if available. It handles version detection, parsing HTML content, 
    and converting tables to JSON.

    Args:
        result_page (BeautifulSoup): The BeautifulSoup object of the result page HTML.
        version (str, optional): The version of the results being parsed. Defaults to None.
        session (requests.Session, optional): The session object used for HTTP requests. Defaults to None.
        url (str, optional): The URL of the result page. Defaults to None.
    """

    def __init__(self, result_page, version=None, session=None, url=None):
        self.session = session
        self.soup = result_page
        self.version = version
        self.url = url
        self.data = {}
        self.pdf_link = None

    def parse(self):
        """
        Parses the trial result page to extract various pieces of information.

        Executes a series of parsing functions to gather summary information, detect
        other versions, extract results information, additional info sections, and PDF
        data if a link is available. Aggregates all extracted data into a dictionary.

        Returns:
            dict: A dictionary containing all parsed data from the result page.
        """

        if not self.version:
            self.get_other_versions()
        self.detect_version()
        self.get_summary()
        self.get_results_information()
        self.get_additional_info()
        self.get_pdf_link()
        if self.pdf_link:
            self.get_pdf_data()
        self.get_html()

        return self.data

    def detect_other_versions_available(self):
        """
        Checks if there are other versions of the trial results available.

        Searches the results page for mentions of other versions. Useful for determining
        if additional parsing should be attempted to gather data from different result versions.

        Returns:
            bool: True if other versions are found, False otherwise.
        """

        other_versions = self.soup.find_all(
            "td", class_="labelColumn", string=re.compile("Other versions"))
        if len(other_versions) < 1:
            return False
        return True

    def get_other_versions(self):
        """
        Retrieves and parses data from other versions of the trial results.

        If other versions of the trial results are available, this function follows
        the links to those versions, parses their data, and updates the main data
        dictionary with the information from each version.
        """

        other_versions = self.soup.find_all(
            "td", class_="labelColumn", string=re.compile("Other versions"))
        if len(other_versions) < 1:
            return

        other_versions = other_versions[0].find_next_sibling('td')
        other_versions = other_versions.find_all("a")

        for version in other_versions:
            response = self.session.get(version["href"])
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            version_text = version.text.strip().replace("\n", " ")
            result = ResultParser(
                soup, url=version["href"], session=self.session, version=version_text)

            self.data.update(result.parse())

    def detect_version(self):
        """
        Detects the version of the trial results being viewed.

        Looks for the results version number on the page and sets it as the current
        version. This information is used to organize parsed data within the data
        dictionary under the appropriate version key.
        """

        if self.version is None:
            version = self.soup.find_all(
                "td", class_="labelColumn", string=re.compile("Results version number"))

            self.version = version[0].find_next_sibling(
                'td').text.strip().replace("\n", " ")
            self.data[self.version] = {}
            return self.version

    def get_html(self):
        """
        Saves the HTML content of the current results page.

        Stores the entire HTML of the result page as a string under the current version's
        key in the data dictionary. This is useful for archival purposes or further
        analysis that requires the original page markup.
        """

        self.data[self.version]["html"] = str(self.soup)

    def get_summary(self):
        """
        Extracts summary information from the trial results page.

        Gathers key summary data such as the EudraCT number, trial protocol references,
        and global end date. This information is structured and saved under the 'summary'
        key in the data dictionary for the current version.
        """

        summary = self.soup.find("div", id="resultContent").find("table")
        summary = summary.find_all("td")

        eudract_number = [td.find_next_sibling(
            "td").text.strip().replace("\n", "") for td in summary if td.text.strip() == "EudraCT number"]
        trial_protocol = [td.find_next_sibling(
            "td") for td in summary if "Trial prot" in td.text.strip()]
        trial_protocol = [a.text.strip().replace("\n", "")
                          for a in trial_protocol[0].find_all("a")]
        global_end_date = [td.find_next_sibling(
            "td").text.strip().replace("\n", "") for td in summary if "Global end" in td.text.strip()]
        self.data[self.version] = {"summary": {
            "url": self.url,
            "eudract_number": eudract_number[0] if len(eudract_number) > 0 else None,
            "trial_protocol": trial_protocol,
            "global_end_date": global_end_date[0] if len(global_end_date) > 0 else None
        }}

    def get_results_information(self):
        """
        Parses detailed results information from the trial results page.

        Extracts structured information from the results section, organizing it into
        key-value pairs. This parsed data is then stored under the 'results_information'
        key in the data dictionary for the current version.
        """

        results_information = self.soup.find(
            "div", id="resultContent").find("table")
        results_information = results_information.find_all("td")
        results_information_row_index = results_information.index(
            [td for td in results_information if td.text.strip() == "Results information"][0])
        results_information = results_information[results_information_row_index:]
        results_info = {}
        for i in range(0, len(results_information), 2):
            key = results_information[i].text.strip().replace("\n", "")
            value = results_information[i + 1].text.strip().replace("\n", "")
            results_info[key] = value
        results_info.pop("Results information")
        self.data[self.version]["results_information"] = results_info

    def remove_closed_tables(self):
        """
        Removes tables marked as closed from the BeautifulSoup object.

        Some tables on the results page may be marked as closed and not relevant
        for data extraction. This function finds such tables by their ID and removes
        them from the BeautifulSoup object to simplify further parsing.
        """
        for table in self.soup.find_all("table", id=re.compile("Closed$")):
            table.decompose()

    def parse_table_to_json(self, table, table_title):
        """
        Converts an HTML table to a structured JSON representation.

        Parses a given HTML table, converting its rows and cells into a JSON-like
        list of dictionaries or nested lists, excluding specific banned texts. This
        method is used for detailed parsing of tables within the additional information
        sections.

        Args:
            table (Tag): The BeautifulSoup Tag object for the HTML table.
            table_title (str): The title of the table, used to exclude specific header texts.

        Returns:
            list: A structured list representation of the table's contents.
        """

        result = []
        rows = table.find_all('tr')
        banned_texts = [table_title.lower(), "top of page"]
        for row in rows:
            cells = row.find_all(['th', 'td'])
            row_data = []
            valid_row = False
            for cell in cells:
                nested_table = cell.find('table')
                cell_text = cell.get_text(strip=True)
                if nested_table:
                    nested_result = self.parse_table_to_json(
                        nested_table, table_title)
                    row_data.append(nested_result)
                    if nested_result:
                        valid_row = True
                elif cell_text.lower() not in banned_texts and cell_text != "":
                    if len(cells) > 1:
                        row_data.append({cells[0].get_text(strip=True): [
                            cell.get_text(strip=True) for cell in cells[1:]]})
                    else:
                        row_data.append(cell_text)
                    valid_row = True
                else:
                    continue

            if valid_row:
                result.append(row_data)

        return result

    def get_additional_info(self):
        """
        Extracts additional information from various sections of the results page.

        Identifies and parses additional sections of the trial results page, such as
        detailed study data or outcome measures. Each section is parsed into a structured
        JSON format and added to the data dictionary under the 'additional_info' key
        for the current version.
        """

        self.remove_closed_tables()
        additional_info = {}
        jumper_links_div = self.soup.find('div', id='jumperLinks')
        if not jumper_links_div:
            return additional_info

        jumper_links = jumper_links_div.find_all('a', href=True)[:-2]
        table_ids = [link['href'][1:] for link in jumper_links]

        tables = jumper_links_div.find_next_siblings("table")

        current_section = None
        for table in tables:

            table_id = table.get('id')
            if table_id in table_ids:
                current_section = table_ids.index(table_id)
                title = jumper_links[current_section].get_text().strip()
                additional_info[title] = [
                    self.parse_table_to_json(table, title)]
            elif current_section is not None:
                title = jumper_links[current_section].get_text().strip()
                additional_info[title].append(
                    self.parse_table_to_json(table, title))

        self.data[self.version]["additional_info"] = additional_info

    def get_pdf_link(self):
        """
        Finds and stores the PDF link for the trial results, if available.

        Searches the trial results page for a link to download the results as a PDF.
        If found, the link is saved for later use in downloading and parsing the PDF content.
        """

        pdf_link = self.soup.find('a', id='downloadResultPdf')
        if not pdf_link:
            return None
        pdf_link = pdf_link['href']
        self.pdf_link = pdf_link

    def get_pdf_zip(self, pdf_link):
        """
        Downloads the PDF file (potentially in a ZIP archive) from the given link.

        Sends a GET request to the specified PDF link to download the file. This is
        typically called for links that point directly to a PDF or a ZIP archive containing
        the PDF document of the trial results.

        Args:
            pdf_link (str): The URL to the PDF file or ZIP archive.

        Returns:
            bytes: The byte content of the downloaded file.
        """

        response = self.session.get(pdf_link)
        return response.content

    def get_pdf_data(self):
        """
        Extracts text and tables from the downloaded PDF file.

        Utilizes the PDF link obtained from the results page to download the PDF file,
        extracts text and tables from it, and stores this data in the data dictionary
        under the 'pdf' key for the current version.
        """

        zip_bytes = self.get_pdf_zip(self.pdf_link)

        pdf_text, pdf_tables = extract_text_and_tables_from_pdf(zip_bytes)
        self.data[self.version]["pdf"] = {
            "url": self.pdf_link,
            "text": pdf_text,
            "tables": pdf_tables,
        }

    def print_data(self):
        """
        Prints the aggregated data dictionary to the console.

        Useful for debugging or reviewing the parsed data collected by the ResultParser
        instance. This method simply outputs the entire data dictionary to the console.
        """

        print(self.data)
