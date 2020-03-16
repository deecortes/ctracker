import sys
import json

import argparse
import requests
from tabulate import tabulate
import logging


BASE_URL = 'https://covidtracking.com'

URLS = {
    'states_current': '/api/states',
    'states_daily': '/api/states/daily',
    'states_info': '/api/states/info',
    'us_current': '/api/us',
    'us_daily': '/api/us/daily',
    'counties': '/api/counties',
    'tracker_urls': '/api/urls',
}


def get_data(base_url, endpoint):
    try:
        data = requests.get(f'{base_url}/{URLS[endpoint]}')
    except Exception as e:
        logging.error(f'Cannot get data ({e})')
        raise

    if endpoint != 'tracker_urls':
        try:
            data = data.json()
        except Exception as e:
            logging.error(f'Cannot parse JSON ({e})')
            raise

    return data


def parse_args():
    parser = argparse.ArgumentParser(description='simple tool to get COVID-19 data')

    parser.add_argument(
        'report_type',
        help='specify type of report: {}'.format(','.join(URLS.keys())),
    )

    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    if hasattr(args, 'report_type') and args.report_type not in URLS.keys():
        print('Need valid report type (one of: {})'.format(
            ','.join(URLS.keys())))
        return 1

    data = get_data(BASE_URL, args.report_type)
    headers = data[0].keys(),
    print(tabulate(data, headers='keys'))


if __name__ == '__main__':
    sys.exit(main())
