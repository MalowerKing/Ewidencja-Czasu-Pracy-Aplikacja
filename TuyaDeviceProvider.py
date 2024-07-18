import logging
import json
from pathlib import Path
from tuya_connector import TuyaOpenAPI, TUYA_LOGGER
from datetime import datetime, time, timedelta

class TuyaDeviceLogProvider:
    __endpoint = "https://openapi.tuyaeu.com"
    __codes = ["unlock_fingerprint_kit","unlock_card_kit"]
    def __init__(self, Credentials):
        # Initialize the class with access credentials and API endpoint
        self.__access_id = Credentials['access_id']
        self.__access_key = Credentials['access_key']
        self.__device_id = Credentials['device_id']

    def __convert_event_time(self, event_time):
        # Convert the timestamp from Tuya's log to a human-readable date and time string
        dt = datetime.fromtimestamp(event_time / 1000)
        date = dt.strftime('%Y-%m-%d')
        time = dt.strftime('%H:%M:%S')
        return date, time

    def __get_logs(self):
        all_logs = []
        TUYA_LOGGER.setLevel(logging.DEBUG)
        openapi = TuyaOpenAPI(self.__endpoint, self.__access_id, self.__access_key)
        openapi.connect()

        for code in self.__codes:
            response = openapi.get(f"/v2.0/cloud/thing/{self.__device_id}/report-logs?codes={code}&end_time=2251515231311&size=100&start_time=1453151331311")
            if response.get('result') and response['result'].get('logs'):
                all_logs.extend(response['result']['logs'])

        return all_logs

    def __clean_json_string(self, json_string):

        replacements = [
            ('"code":', ''), ('"event_time":', ''), ('"value":', ''),
            ('"date":', ''), ('"time":', ''), ('[', ''), (']', ''),
            ('{', ''), ('},', ''), ('}', ''), ('"', ''), (',', ''),
            ('\t', ''), (' ', ''), (' ', '')
        ]
        for old, new in replacements:
            json_string = json_string.replace(old, new)
        return json_string

    def getDeviceLog(self):
        logs = self.__get_logs()

        extracted_data = []
        for log in logs:
            date, time = self.__convert_event_time(log['event_time'])
            extracted_data.append({
                'code': log['code'],
                'date': date,
                'time': time,
                'value': log['value']
            })

        json_string = json.dumps(extracted_data, indent=4)
        cleaned_json_string = self.__clean_json_string(json_string)

        # Save cleaned JSON to a file
        output_file = "cleaned_response.txt"
        with open(output_file, 'w') as f:
            f.write(cleaned_json_string)

        return extracted_data  # Return extracted logs