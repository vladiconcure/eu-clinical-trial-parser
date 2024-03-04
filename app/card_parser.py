from bs4 import BeautifulSoup

BASE_URL = "https://www.clinicaltrialsregister.eu"


class CardParser:
    """
    Parses essential information from a trial card on the EU Clinical Trials Register.

    This class processes a trial's card (a summarized view of trial details) to extract
    key information such as the EudraCT number, sponsor details, trial title, medical condition,
    and links to trial protocols and results. It organizes this information into a structured
    dictionary for easy access.

    Args:
        card (BeautifulSoup): The BeautifulSoup object representing the HTML of the trial card.
    """

    def __init__(self, card: BeautifulSoup):
        self.card = card
        self.data = {}
        self.table_exists = False

    def parse(self):
        """
        Parses the trial card to extract and organize trial information.

        Executes a series of parsing functions to gather information from the trial card.
        This includes identifying if a detailed disease table exists, extracting basic
        information like EudraCT number, sponsor protocol number, and more detailed data
        such as disease classification and links to trial protocols and results.

        Returns:
            dict: A dictionary containing all parsed information from the trial card.
        """

        self.check_table_exists()
        # Note: The "Disease" row could be present in a table format or a row format

        self.get_eudract_number()
        self.get_sponsor_protocol_number()
        self.get_start_date()
        self.get_sponsor_name()
        self.get_full_title()
        self.get_medical_condition()
        self.get_population_age()
        self.get_gender()
        self.get_disease_row_data()
        self.get_trial_protocols()
        self.get_trial_results_link()

        return self.data

    def print_data(self):
        """
        Outputs the parsed data from the trial card in a structured format.

        Provides a quick way to review the data extracted from the trial card by
        printing key-value pairs to the console. This is useful for debugging or
        verifying the parsing logic.
        """

        for key, value in self.data.items():
            print(f"{key}: {value}")

    def check_table_exists(self):
        """
        Checks if the trial card contains a detailed table for disease information.

        Sets an internal flag based on the presence of a table within the card. This
        flag is later used to determine the parsing strategy for disease-related data.
        """

        if self.card.find("table"):
            self.table_exists = True

    # First Row Data
    def get_eudract_number(self):
        """
        Extracts the EudraCT number from the trial card.

        Finds and processes the EudraCT number, cleaning it of any extraneous text
        or formatting, and stores it in the data dictionary.
        """

        eudract_number = self.card.find("tr").find("td").text
        eudract_number = eudract_number.replace("EudraCT Number:", "").strip()
        eudract_number = eudract_number.replace(" ", "")
        self.data["eudract_number"] = eudract_number

    def get_sponsor_protocol_number(self):
        """
        Extracts the sponsor's protocol number from the trial card.

        Identifies and cleans the sponsor protocol number from the card, making it
        available in the structured data output.
        """

        sponsor_protocol_number = self.card.find("tr").find_all("td")[1].text
        sponsor_protocol_number = sponsor_protocol_number.replace(
            "Sponsor Protocol Number:", "").strip()
        self.data["sponsor_protocol_number"] = sponsor_protocol_number

    def get_start_date(self):
        """
        Determines the start date of the trial from the trial card.

        Parses and formats the start date of the trial, removing any non-date text
        or symbols, and adds it to the data dictionary.
        """

        start_date = self.card.find("tr").find_all("td")[2].text
        start_date = start_date.replace("Start Date", "").strip()
        start_date = start_date.replace("*", "").replace(":", "")
        start_date = start_date.replace(" ", "").replace("\n", "")
        self.data["start_date"] = start_date
    # Second Row Data

    def get_sponsor_name(self):
        """
        Retrieves the sponsor's name from the trial card.

        Extracts the sponsor name, cleans it of any newline characters or leading/trailing
        spaces, and stores it in the data dictionary.
        """

        sponsor_name = self.card.find_all("tr")[1].find("td").text
        sponsor_name = sponsor_name.replace("Sponsor Name:", "").strip()
        sponsor_name = sponsor_name.replace("\n", "")
        self.data["sponsor_name"] = sponsor_name
    # Third Row Data

    def get_full_title(self):
        """
        Extracts the full title of the trial from the trial card.

        Locates and processes the trial's full title, ensuring it is free of newline
        characters and properly trimmed before adding it to the data dictionary.
        """

        full_title = self.card.find_all("tr")[2].find("td").text
        full_title = full_title.replace("Full Title:", "").strip()
        full_title = full_title.replace("\n", "")
        self.data["full_title"] = str(full_title)
    # Fourth Row Data

    def get_medical_condition(self):
        """
        Gathers information about the medical condition being studied in the trial.

        Parses the description of the medical condition from the trial card, cleaning
        it for easy readability and inclusion in the data output.
        """

        medical_condition = self.card.find_all("tr")[3].find("td").text
        medical_condition = medical_condition.replace("Medical condition:", "").strip()
        medical_condition = medical_condition.replace("\n", "")
        self.data["medical_condition"] = medical_condition
    # Fifth Row Data

    def get_disease_row_data(self):
        """
        Extracts detailed disease classification data from the trial card.

        If a detailed table exists, parses disease-related data such as version, SOC term,
        classification code, term, and level. Formats and stores this information in a
        structured way within the data dictionary.
        """

        self.data["disease"] = {
            "version": [],
            "soc_term": [],
            "classification_code": [],
            "term": [],
            "level": []
        }

        if not self.table_exists:
            for key in self.data["disease"]:
                self.data["disease"][key] = None
            return

        disease_table = self.card.find(
            "table")
        tds = disease_table.find_all("td")
        tds = [td for td in tds if not td.get("class")]

        for i, td in enumerate(tds):
            if i % 5 == 0:
                self.data["disease"]["version"].append(td.text.strip())
            elif i % 5 == 1:
                self.data["disease"]["soc_term"].append(td.text.strip())
            elif i % 5 == 2:
                self.data["disease"]["classification_code"].append(
                    td.text.strip())
            elif i % 5 == 3:
                self.data["disease"]["term"].append(td.text.strip())
            elif i % 5 == 4:
                self.data["disease"]["level"].append(td.text.strip())

        for key in self.data["disease"]:
            self.data["disease"][key] = " ||| ".join(
                self.data["disease"][key]) if self.data["disease"][key] else None

    # Sixth Row Data

    def get_population_age(self):
        """
        Identifies the age range of the population involved in the trial.

        Extracts and cleans the population age information from the trial card, adding
        it to the data dictionary for easy reference.
        """

        population_age = self.card.find_all("tr")[-3].find("td").text
        population_age = population_age.replace(
            "Population Age:", "").strip()
        population_age = population_age.replace("\n", "")
        self.data["population_age"] = population_age

    def get_gender(self):
        """
        Determines the gender eligibility for the trial from the trial card.

        Parses the gender criteria for trial participation, cleans the text, and stores
        it in the data dictionary.
        """

        gender = self.card.find_all("tr")[-3].find_all("td")[1].text
        gender = gender.replace("Gender:", "").strip()
        gender = gender.replace("\n", "")
        self.data["gender"] = gender
    # Seventh Row Data

    def get_trial_protocols(self):
        """
        Extracts links and information related to the trial's protocols.

        Gathers data on trial protocols, including names, URLs, and statuses, organizing
        this information into a list of dictionaries within the data output.
        """

        protocols = self.card.find_all("tr")[-2].find("td")
        trial_protocols = []
        for protocol in protocols.find_all("a"):
            protocol_name = protocol.text.strip().replace("\n", "")
            protocol_url = BASE_URL + protocol.get("href")
            protocol_status = protocol.find_next_sibling("span")
            if not protocol_status:
                protocol_status = "No Status Available"
            else:
                protocol_status = protocol_status.text.strip().replace(
                    "(", "").replace(")", "").replace("\n", "")

            trial_protocols.append(
                {"protocol_name": protocol_name, "protocol_url": protocol_url, "protocol_status": protocol_status})
        self.data["trial_protocols"] = trial_protocols

    # Eighth Row Data

    def get_trial_results_link(self):
        """
        Finds and stores the link to the trial's results, if available.

        Checks the trial card for a link to the trial results. If found, the link is
        cleaned, made absolute, and added to the data dictionary.
        """

        trial_results_link = self.card.find_all("tr")[-1].find("td").find("a")
        if not trial_results_link:
            self.data["trial_results_link"] = None
            return
        trial_results_link = trial_results_link.get("href")
        self.data["trial_results_link"] = BASE_URL + trial_results_link
