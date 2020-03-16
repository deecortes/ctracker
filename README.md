# COVID-19 Tracker Tool

This is just a very small tool that reads Corona Virus infection data from an
API, saves the data in AWS DynamoDB and if the total number of infections in
the US or a specific state went up, it sends an SMS to a list of phone numbers.
The tool is meant to be run as a Lambda. This is just a hobby project, not
meant for anything that needs reliable alerting, since it's missing a lot of
things (error handling, terraform, ...).

## DynamoDB Tables and Fields

### Table: us_current

- positive
- negative
- posNeg
- pending
- death
- total
- lastUpdateEt
- tslastUpdateEt

### Table: states_current

- state
- positive
- negative
- pending
- death
- total
- lastUpdateEt
- checkTimeEt
- tslastUpdateEt
- tscheckTimeEt
