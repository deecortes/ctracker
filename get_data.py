import sys
import json
import re

import boto3
import argparse
import requests
import logging

from tabulate import tabulate
from datetime import datetime


BASE_URL = 'https://covidtracking.com'
DATE_PATTERN = re.compile('^(.+)/(.+) ([0-9]{2}):([0-9]{2})$')
YEAR = 2020

URLS = {
    'states_current': '/api/states',
    'states_daily': '/api/states/daily',
    'states_info': '/api/states/info',
    'us_current': '/api/us',
    'us_daily': '/api/us/daily',
    'counties': '/api/counties',
    'tracker_urls': '/api/urls',
}

DATE_FIELDS = [
    'lastUpdateEt',
    'checkTimeEt',
]

INT_FIELDS = [
    'positive',
    'negative',
    'pending',
    'death',
    'total',
]


def store_data(data, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)

    with table.batch_writer() as batch:
        for row in data:
            batch.put_item(Item=row)


def get_data(base_url, endpoint):
    try:
        data = requests.get(f'{base_url}/{URLS[endpoint]}')
    except Exception as e:
        logging.error(f'Cannot get data ({e})')
        raise

    if endpoint != 'tracker_urls':
        try:
            data = json.loads(data.text, cls=CustomJSONDecoder)
        except Exception as e:
            logging.error(f'Cannot parse JSON ({e})')
            raise

    return data


# dates look like '3/15 13:00', couldn't use datetime.strptime because there is
# no non-zero-padded month,
# pessimistically adding year as a param, since it's missing in the date
def parse_date(compiled_pattern, date, year):
    m = compiled_pattern.match(date)
    if m:
        dt = datetime(
            year, int(m.group(1)), int(m.group(2)),
            int(m.group(3)), int(m.group(4)),
        )
        return int(dt.timestamp())


class CustomJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(
            self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dict_):
        for date_field in DATE_FIELDS:
            if date_field in dict_:
                dict_[f'ts{date_field}'] = parse_date(
                    DATE_PATTERN,
                    dict_[date_field],
                    YEAR,
                )

        for int_field in INT_FIELDS:
            try:
                dict_[int_field] = int(dict_[int_field])
            except TypeError:
                dict_[int_field] = 0

        # this is for us_current
        if 'lastUpdateEt' not in dict_:
            now = datetime.now()
            dict_['lastUpdateEt'] = now.strftime('%Y-%m-%d %H:%M:%S')
            dict_['tslastUpdateEt'] = int(now.timestamp())

        return dict_


def parse_args():
    parser = argparse.ArgumentParser(
        description='simple tool to get COVID-19 data')
    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        '--report-type',
        help='specify type of report: {}'.format(','.join(URLS.keys())),
    )
    group.add_argument(
        '--test-lambda',
        action='store_true',
        help='only execute the lambda portion'
    )

    return parser.parse_args()


def handler(event, context):
    for report in ('us_current', 'states_current'):
        logging.info('Getting report {report}.')
        data = get_data(BASE_URL, report)
        logging.info('Storing data for report {report}.')
        store_data(data, report)

    return {
        'statusCode': 200,
        'body': 'OK',
    }


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)

    if hasattr(args, 'test_lambda'):
        handler(None, None)
    elif hasattr(args, 'report_type') and args.report_type not in URLS.keys():
        print('Need valid report type (one of: {})'.format(
            ','.join(URLS.keys())))
        return 1
    elif hasattr(args, 'report_type'):
        data = get_data(BASE_URL, args.report_type)
        print(tabulate(data, headers='keys'))


if __name__ == '__main__':
    sys.exit(main())
