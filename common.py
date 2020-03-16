from collections import OrderedDict as OD


TABLES = {
    'us_current': OD({
        'lastUpdateEt': 'S',
        'tslastUpdateEt': 'N',
    }),
    'states_current': OD({
        'state': 'S',
        'tscheckTimeEt': 'N',
    }),
}

INDICES = {
    'us_current': OD({
        'lastUpdateEt': 'HASH',
        'tslastUpdateEt': 'RANGE',
    }),
    'states_current': OD({
        'state': 'HASH',
        'tscheckTimeEt': 'RANGE',
    })
}
