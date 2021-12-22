# Import

import json
import requests
from requests.exceptions import RequestException
from config import Config
from datetime import timedelta, date, datetime
import os


# Main function


def app(process_date=None):
    if process_date:
        process_date
    else:
        print('please enter date: ')
        process_date = input()

    config = Config('./config.yaml').get_config()

    # Authorization

    try:
        r = requests.post(config['url'] + config['endpoint_auth'], headers=config['headers_auth'],
                          data=json.dumps(config['payload_auth']))
        r.raise_for_status()
        token = r.json()['access_token']
        r.raise_for_status()
    except RequestException:
        print('Error during authorization. Please check URL, login or password in config.yaml file')

# Connecting to API

    headers = {'content-type': 'application/json',
               'Authorization': 'JWT ' + token}

    config['payload_API']['date'] = process_date

    try:
        r = requests.get(config['url'] + config['endpoint_API'], headers=headers,
                         data=json.dumps(config['payload_API']))
        r.raise_for_status()
        path_to_dir = os.path.join('./', 'data', process_date)
        os.makedirs(path_to_dir, exist_ok=True)
        data = r.json()
        with open(os.path.join(path_to_dir, config['filename']), 'w') as json_file:
            json.dump(data, json_file)

    except RequestException:
        print('API error or no data for process_date')
        print('Please check the error number ' + str(r.status_code))


if __name__ == '__main__':
    app()
