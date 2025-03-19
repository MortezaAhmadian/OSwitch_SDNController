import csv, json, os
import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
from .OS import OS
from pathlib import Path
from Optical_Switch.wrapper.decorators import decorators
from .logger import Logger

_LOGGER = Logger().get_logger()
_DECORATOR = decorators(_LOGGER)
_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_ofs_config():
    config_path = _DIR / Path("config") / "OFS.json"
    config_path = config_path.resolve()
    with open(config_path, 'r') as f:
        return json.load(f)

class OFS(OS):
    def __init__(self):
        super().__init__()
        self._config = _load_ofs_config()
        self.namespace = self._config["namespace"]
        self.API_URL = self._config["API_URL"]
        self.USERNAME = self._config["USERNAME"]
        self.PASSWORD = self._config["PASSWORD"]
        self.HEADERS = self._config["HEADERS"]

    # Function to create XML payload for the cross-connect
    def _create_xml_payload(self, pairs):
        xml_pairs = "".join(
            [f"<pair><ingress>{ingress}</ingress><egress>{egress}</egress></pair>" for ingress, egress in pairs])
        return f"<cross-connects>{xml_pairs}</cross-connects>"

    # Function to make the POST request
    def _make_post_request(self, xml_payload):
        response = requests.post(
            self.API_URL,
            data=xml_payload,
            headers=self.HEADERS,
            auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD)
        )
        return response.status_code, response.text

    def _make_patch_request(self, xml_payload):
        response = requests.patch(
            self.API_URL,
            data=xml_payload,
            headers=self.HEADERS,
            auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD)
        )
        return response.status_code, response.text

    # Main function to read CSV and make connections
    def make_connections_from_csv(self, csv_file_path):
        pairs = []
        with open(csv_file_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                source_port = row['source_port']
                destination_port = row['destination_port']
                pairs.append((source_port, destination_port))
        if pairs:
            xml_payload = self._create_xml_payload(pairs)
            status_code, response_text = self._make_patch_request(xml_payload)
            _LOGGER.info(f"PATCH request sent. Status Code: {status_code}, Response: {response_text}")


    def delete_all_cross_connects(self):
        """
        Deletes all cross-connects from the optical switch.

        :return: A tuple containing the HTTP status code and response text.
        """
        try:
            response = requests.delete(
                self.API_URL,
                headers=self.HEADERS,
                auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD)
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX, 5XX)
            return response.status_code, response.text
        except requests.exceptions.RequestException as e:
            return None, str(e)

    @_DECORATOR.retrieve_metrics
    def get_existing_connections(self):
        """
        Retrieves the existing connection pairs from the Optical Fiber  Switch (OFS).

        :return: A list of tuples where each tuple contains (ingress, egress) pairs.
        """
        try:
            response = requests.get(
                self.API_URL,
                auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD)
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX, 5XX)

            # Parse the XML response
            if response.text:
                root = ET.fromstring(response.text)
            else:
                return
            pairs = []
            for pair in root.findall('ns:pair', self.namespace):
                ingress = pair.find('ns:ingress', self.namespace).text
                egress = pair.find('ns:egress', self.namespace).text
                pairs.append({"port_in": int(ingress),
                              "port_out": int(egress)})

            return pairs

        except requests.exceptions.RequestException as e:
            _LOGGER.info(f"Error retrieving cross-connects: {e}")
            return []

    def delete_cross_connect(self, pair_id):
        """
        Deletes a specific cross-connect pair by its pair ID.

        :param pair_id: The identifier of the cross-connect pair to delete.
        :return: A tuple containing the HTTP status code and response text.
        """
        url = f"{self.API_URL}/pair={pair_id}"
        try:
            response = requests.delete(
                url,
                headers=self.HEADERS,
                auth=HTTPBasicAuth(self.USERNAME, self.PASSWORD)
            )
            response.raise_for_status()  # Raises HTTPError for bad responses (4XX, 5XX)
            return response.status_code, response.text
        except requests.exceptions.RequestException as e:
            return None, str(e)

