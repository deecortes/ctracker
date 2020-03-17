import os
import sys
import json
import re

import boto3
import argparse
import requests
import logging

from tabulate import tabulate
from datetime import datetime
from boto3.dynamodb.conditions import Key


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


def store_data(db, data, table_name):
    table = db.Table(table_name)

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
            except (TypeError, KeyError) as e:
                logging.info(f'Cannot clean int_field {int_field} ({e}).')
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


def get_state(data, state):
    return [row for row in data if row['state'] == state][0]


def get_states_old(db, state):
    t = db.Table('states_current')
    return t.query(
        KeyConditionExpression=Key('state').eq(state),
        ScanIndexForward=False,
        Limit=1,
    ).get('Items')[0]


def get_us_old(db):
    t = db.Table('us_current')
    r = t.scan().get('Items')
    return sorted(r, key=lambda i: i['tslastUpdateEt'], reverse=True)[0]


def handler(event, context):
    db = boto3.resource('dynamodb')
    state = os.environ.get('STATE')
    old_state_data = get_states_old(db, state)
    old_us_data = get_us_old(db)
    new_state_data = None
    new_us_data = None

    for report in ('us_current', 'states_current'):
        logging.info(f'Getting report {report}.')
        data = get_data(BASE_URL, report)
        logging.info(f'Storing data for report {report}.')
        store_data(db, data, report)

        if report == 'states_current':
            new_state_data = get_state(data, state)
        elif report == 'us_current':
            new_us_data = data[0]

    message = None
    if new_state_data['positive'] > old_state_data['positive']:
        message = "Alert: \n"
        message += (
            f'Infections in {state} have increased, '
            f'from {old_state_data["positive"]} to '
            f'{new_state_data["positive"]}. '
        )

    if new_us_data['positive'] > old_us_data['positive']:
        message += (
            f'Infections in the US have increased, '
            f'from {old_us_data["positive"]} to '
            f'{new_us_data["positive"]}. '
        )

    if message:
        print(message)
        phone_numbers = os.environ.get('PHONE_NUMBERS').split(',')
        sns = boto3.client('sns')
        for phone_number in phone_numbers:
            sns.publish(
                PhoneNumber=phone_number,
                Message=message,
            )

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
