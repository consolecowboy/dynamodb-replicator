#!/bin/env python
from __future__ import print_function  # Python 2/3 compatibility
import boto3, botocore
import argparse
import pprint
import subprocess

parser = argparse.ArgumentParser(description='Dynamo Test Data Generator')
parser.add_argument('S3Bucket', type=str,
                    help='S3 bucket to backfill to')
parser.add_argument('S3Prefix', type=str,
                    help='S3 key prefix to backfill to')
parser.add_argument('Lambda', type=str,
                    help='Lambda Name to add trigger to')
parser.add_argument('--region', type=str, default='us-east-1',
                    help='Region SQS Queue exists in, defaults to us-east-1')
parser.add_argument('--tableprefix', type=str, default=None,
                    help='Dynamo Table prefix to enable backups on')
args = parser.parse_args()

# Read all records in s3 bucket and replay into new dynamo table
pp = pprint.PrettyPrinter(indent=4)

ddb = boto3.client('dynamodb')
lc = boto3.client('lambda')

# Doesn't work with more than 100 tables
# mytables = ddb.list_tables()

# Handles infinite tables
mytables = list()
paginator = ddb.get_paginator('list_tables')
response_iterator = paginator.paginate()
for page in response_iterator:
    mytables = mytables + page['TableNames']

print("Analyzing " + str(len(mytables)) + " tables for stream enabling and backfilling")
if args.tableprefix is not None:
    print("* Only working with tables starting with " + str(args.tableprefix) + " *")

it=0

for table in mytables:
    if args.tableprefix is not None and table.startswith(args.tableprefix) is not True:
        # print('- Skipping ' + table + ' does not match prefix ' + args.tableprefix)
        continue
    else:
        it += 1
        print('Table ' + table)
        print('+ Enabling Streams')

        response = None
        try:
            thisddbstream = None
            response = ddb.update_table(
                TableName=table,
                StreamSpecification={
                    'StreamEnabled': True,
                    'StreamViewType': 'NEW_AND_OLD_IMAGES'
                }
            )
            thisddbstream = response['TableDescription']['LatestStreamArn']
            print(' + Created stream ' + thisddbstream)
        except botocore.exceptions.ClientError:
            print(' - Skipping, stream already enabled')

        if thisddbstream is None:
            print(" - Skipping, no stream to add to lambda")
        else:
            print('+ Adding to lambda')

            response = None
            try:
                response = lc.create_event_source_mapping(
                    EventSourceArn=thisddbstream,
                    FunctionName=args.Lambda,
                    Enabled=True,
                    BatchSize=10,
                    StartingPosition='TRIM_HORIZON'
                )
                print(' + Added ' + response['EventSourceArn'] + ' to ' + response['FunctionArn'])
            except botocore.exceptions.ClientError:
                print(' - Skipping, trigger already in lambda')

        print('+ Backfilling S3')
        myproc = subprocess.Popen(['/usr/local/bin/incremental-backfill',
                                   str(args.region + '/' + table),
                                   str('s3://' + args.S3Bucket + '/' + args.S3Prefix)],
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = myproc.communicate()
        if len(stderr) > 0:
            print('ERROR:' + str(pp.pprint(stderr)))
        else:
            print('SUCCESS: Rows/spd: ' + stdout.rsplit('\r\x1b[K', 1)[1])

        print('')

print("Complete: " + str(it) + " out of  " + str(len(mytables)) + " processed.")