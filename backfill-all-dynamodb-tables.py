#!/bin/env python
from __future__ import print_function  # Python 2/3 compatibility
import boto3
import argparse
import pprint
import subprocess

parser = argparse.ArgumentParser(description='Dynamo Test Data Generator')
parser.add_argument('S3Bucket', type=str,
                    help='S3 bucket to backfill to')
parser.add_argument('S3Prefix', type=str,
                    help='SS3 key prefix to backfill to')
parser.add_argument('--region', type=str, default='us-east-1',
                    help='Region SQS Queue exists in, defaults to us-east-1')
args = parser.parse_args()

# Read all records in s3 bucket and replay into new dynamo table
pp = pprint.PrettyPrinter(indent=4)

ddb = boto3.client('dynamodb')

mytables = ddb.list_tables()

print("Backfilling " + str(len(mytables['TableNames'])) +
      " tables into s3://" + str('s3://' + args.S3Bucket + '/' + args.S3Prefix))

for table in mytables['TableNames']:
    print(table, end=': ')
    myproc = subprocess.Popen(['/usr/local/bin/incremental-backfill',
                               str(args.region + '/' + table),
                               str('s3://' + args.S3Bucket + '/' + args.S3Prefix)],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = myproc.communicate()
    if len(stderr) > 0:
        print('ERROR:' + str(pp.pprint(stderr)))
    else:
        print('SUCCESS: Rows: ' + stdout.rsplit('\r\x1b[K', 1)[1])

