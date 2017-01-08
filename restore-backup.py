#!/bin/env python
from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json
import argparse
import hashlib
import datetime
import random
import string
import pprint

parser = argparse.ArgumentParser(description='Dynamo Test Data Generator')
parser.add_argument('sourceS3Bucket', type=str,
                    help='Source s3 bucket to restore from')
parser.add_argument('sourceS3Prefix', type=str,
                    help='Source s3 key prefix to restore from')
parser.add_argument('tablename', type=str,
                    help='Dynamo Table to restore (must exist w/ same partition key, '
                         'will overwrite existing rows with same key)')
parser.add_argument('--region', type=str, default='us-east-1',
                    help='Region SQS Queue exists in, defaults to us-east-1')
args = parser.parse_args()

# Read all records in s3 bucket and replay into new dynamo table
pp = pprint.PrettyPrinter(indent=4)
s3 = boto3.resource('s3')
bucket = s3.Bucket(args.sourceS3Bucket)

ddb = boto3.client('dynamodb')

# Iterates through all the objects, doing the pagination for you. Each obj
# is an ObjectSummary, so it doesn't contain the body. You'll need to call
# get to get the whole body.
for obj in bucket.objects.all():
    key = obj.key
    response = ddb.put_item(
        TableName=args.tablename,
        Item=json.loads(obj.get()['Body'].read())
    )
    pp.pprint(response)

