import boto3
import logging

from common import TABLES, INDICES


logging.basicConfig(level=logging.INFO)

dynamodb = boto3.resource('dynamodb')

for table_name, value in TABLES.items():
    logging.info(f'Creating table {table_name} ...')
    index = [ { 'AttributeName': k,
                'KeyType': v } for k, v in INDICES[table_name].items() ]
    defs = [ { 'AttributeName': k,
               'AttributeType': v } for k, v in value.items() ]

    table = dynamodb.create_table(
        TableName=table_name,
        KeySchema=index,
        AttributeDefinitions=defs,
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        },
    )

    table.meta.client.get_waiter('table_exists').wait(TableName='users')
    logging.info(f'Table {table_name} created.')
