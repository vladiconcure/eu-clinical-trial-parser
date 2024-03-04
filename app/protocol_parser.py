import re
import json


class ProtocolParser:
    """
    Parses detailed protocol data from a specific trial protocol page.

    This class processes a trial's protocol page to extract summary information and
    details from various sections of the protocol. It organizes this information into
    a structured dictionary format for easy access and analysis.

    Args:
        protocol_page (BeautifulSoup): The BeautifulSoup object of the protocol page HTML.
        version (str, optional): The version of the protocol being parsed. Defaults to None.
        session (requests.Session, optional): The session object used for HTTP requests. Defaults to None.
    """

    def __init__(self, protocol_page, version=None, session=None):
        self.session = session
        self.soup = protocol_page
        self.version = version
        self.data = {}

    def parse(self):
        """
        Parses the trial protocol page to extract summary information and section data.

        Executes a series of parsing functions to gather the protocol summary and
        detailed section data. Aggregates all extracted data into a dictionary for
        structured access.

        Returns:
            dict: A dictionary containing all parsed data from the protocol page.
        """

        self.get_summary()
        self.get_section_data()
        return self.data

    def get_summary(self):
        """
        Extracts summary information from the trial protocol page.

        Gathers key summary data from the protocol summary section, organizing it into
        key-value pairs. This parsed data is stored under the 'summary' key in the data
        dictionary.
        """

        summary_table = self.soup.find("table", class_="section summary").find(
            "tbody")
        rows = summary_table.find_all("tr")
        summary = {}
        for row in rows:
            cells = row.find_all("td")
            key = cells[0].get_text().strip().replace(
                "\n", "").replace(":", "")
            value = cells[1].get_text().strip().replace("\n", "").replace(
                ":", "")
            summary[key] = value

        self.data["summary"] = summary

    def get_num_sections(self):
        """
        Determines the number of sections in the trial protocol document.

        Counts the sections listed in the protocol index table, providing an indication
        of the document's structure and complexity.

        Returns:
            int: The total number of sections found in the protocol document.
        """

        index_table = self.soup.find("table", class_="section index")
        index = [td.get_text() for td in index_table.find_all("td")]
        index = [re.sub(r"\n", "", item.strip()) for item in index]
        return len(index)

    def get_section_data(self):
        """
        Extracts detailed data from each section of the trial protocol.

        Iterates over each section as denoted by table elements with specific IDs,
        parsing the content into structured data. Each section's data is added to the
        data dictionary under keys corresponding to the section titles.
        """

        tables = self.soup.find_all("table", id=re.compile(r"section-"))
        for table in tables:
            header = table.find("th").get_text().strip().replace("\n", "")
            self.data[header] = self.get_table_data(table)

    def get_table_data(self, table):
        """
        Converts a protocol section table into a structured data representation.

        Parses a given HTML table from a protocol section, converting its rows and
        cells into a dictionary format, excluding rows without relevant data.

        Args:
            table (Tag): The BeautifulSoup Tag object for the HTML table.

        Returns:
            dict: A structured dictionary representation of the table's contents.
        """

        table_body = table.find("tbody")
        if not table_body:
            return None
        rows = table_body.find_all("tr")
        data = {}
        for row in rows:
            cells = row.find_all("td")

            key = cells[1].get_text().strip().replace("\n", "") if len(
                cells) > 1 else cells[0].get_text().strip().replace("\n", "")

            value = [cell.get_text().strip().replace("\n", "")
                     for cell in cells[2:]] if len(cells) > 1 else []

            if len(value) < 1:
                continue

            value = value if len(value) > 1 else [value[0]]
            data[key] = value
        return data

    def print_data(self):
        """
        Outputs the aggregated protocol data dictionary in a formatted JSON string.

        Useful for debugging or reviewing the parsed data collected by the ProtocolParser
        instance. This method formats the data dictionary as a JSON string and prints it
        to the console.
        """
        print(json.dumps(self.data, indent=4))
