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
                logging.debug(f'Cannot clean int_field {int_field} ({e}).')
                dict_[int_field] = 0

        # this is for us_current
        if 'lastUpdateEt' not in dict_:
            now = datetime.utcnow()
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
        default=False,
        help='only execute the lambda portion'
    )

    return parser.parse_args()


def get_state(data, state):
    try:
        state_data = [row for row in data if row['state'] == state][0]
    except Exception as e:
        logging.error(f'Could not get new data for state {state} ({e}).')
        raise

    return state_data


def get_states_old(db, state):
    t = db.Table('states_current')
    try:
        state_data = t.query(
            KeyConditionExpression=Key('state').eq(state),
            ScanIndexForward=False,
            Limit=1,
        ).get('Items')[0]
    except Exception as e:
        logging.error(f'Could not get old data for state {state} ({e}).')
        raise

    return state_data


def get_us_old(db):
    t = db.Table('us_current')
    r = t.scan().get('Items')
    try:
        data = sorted(r, key=lambda i: i['tslastUpdateEt'], reverse=True)[0]
    except Exception as e:
        logging.error(
            f'Cannot get latest US data, by timestamp "tslastUpdateEt" ({e}).')
        raise

    logging.info(
        f'Got us_current with latest timestamp {data["tslastUpdateEt"]}.')

    return data


def analyze_us_data(db):
    logging.info('Getting report us_current.')
    data = get_data(BASE_URL, 'us_current')

    try:
        old_us_data = get_us_old(db)
    except Exception:
        logging.info(
            'Failed to get old US data, probably empty db, '
            'storing latest US data now.'
        )
        store_data(db, data, 'us_current')
        return False

    logging.info(f'Storing data for report us_current.')
    store_data(db, data, 'us_current')

    new_us_data = data[0]
    return (old_us_data, new_us_data)


def send_sms(data, location):
    (old, new) = data
    logging.info(
        f'Got old({old["positive"]})/new({new["positive"]}) '
        f'for location {location}.'
    )

    message = ""
    if new['positive'] > old['positive']:
        message = "Alert: \n"
        message += (
            f'Infections in {location} have increased, '
            f'from {old["positive"]} to '
            f'{new["positive"]}. '
        )

    if message:
        # just for lambda debugging
        print(message)
        phone_numbers = os.environ.get('PHONE_NUMBERS').split(',')
        sns = boto3.client('sns')
        for phone_number in phone_numbers:
            sns.publish(
                PhoneNumber=phone_number,
                Message=message,
            )


def handler(event, context):
    if len(logging.getLogger().handlers) > 0:
        # The Lambda environment pre-configures a handler logging to stderr. If a
        # handler is already configured, `.basicConfig` does not execute.
        # Thus we set the level directly.
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.basicConfig(level=logging.INFO)

    db = boto3.resource('dynamodb')

    us_data = analyze_us_data(db)
    if us_data:
        send_sms(us_data, 'the US')

    data = get_data(BASE_URL, 'states_current')
    states = os.environ.get('STATES').split(',')
    for state in states:
        logging.info(f'Working on state {state}.')
        send_sms(
            (get_states_old(db, state), get_state(data, state)),
            state,
        )

    logging.info(f'Storing data for states_current.')
    store_data(db, data, 'states_current')

    return {
        'statusCode': 200,
        'body': 'OK',
    }


def main():
    args = parse_args()

    if hasattr(args, 'test_lambda') and args.test_lambda:
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
