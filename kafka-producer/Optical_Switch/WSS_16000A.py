import requests
import pandas as pd
import csv, io, os, json
from .OS import OS
from pathlib import Path
from Optical_Switch.wrapper.decorators import decorators
from .logger import Logger

_LOGGER = Logger().get_logger()
_DECORATOR = decorators(_LOGGER)
_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_ofs_config():
    config_path = _DIR / Path("config") / "WSS_16000A.json"
    config_path = config_path.resolve()
    with open(config_path, 'r') as f:
        return json.load(f)

class wss16000a(OS):
    def __init__(self):
        super().__init__()
        self._config = _load_ofs_config()
        self._ip =self._config['ip']
        self._response = requests.get(self._config['url'].replace("{{ip}}", str(self._ip)))

    @_DECORATOR.retrieve_metrics
    def get_existing_connections(self):
        """
        Retrieves the existing connection pairs from the Wave Shaper Switch (WSS).

        :return: A table where columns contain (frequency, attenuation, phase, port_number).
        """
        try:
            if self._response.status_code == 200:
                wsp_data = []
                # Convert the text response into a readable format
                response_text = self._response.text.strip()
                csv_reader = csv.reader(io.StringIO(response_text), delimiter='\t')
                for row in csv_reader:
                    if len(row) == 4:  # Ensure valid WSP format
                        frequency = float(row[0])
                        attenuation = float(row[1])
                        phase = float(row[2])
                        port_number = int(row[3])
                        wsp_data.append({
                            'frequency': frequency,
                            'attenuation': attenuation,
                            'phase': phase,
                            'port_number': port_number
                        })
                return wsp_data
            else:
                _LOGGER.info(f"Failed to retrieve wsp_data: {self._response.status_code}")
        except requests.exceptions.RequestException as e:
            _LOGGER.info(f"Error retrieving wsp_data: {e}")
            return []

