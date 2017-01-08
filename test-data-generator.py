#!/bin/env python
# Generate test data and insert into dynamo database
from __future__ import print_function  # Python 2/3 compatibility
import boto3
import json
import decimal
import argparse
import hashlib
import datetime
import random
import string

parser = argparse.ArgumentParser(description='Dynamo Test Data Generator')
parser.add_argument('tablename', type=str,
                    help='Dynamo Table to Write to')
parser.add_argument('--min', type=int, default=1,
                    help='Min Row Number to Write to')
parser.add_argument('--max', type=int, default=100,
                    help='Max Row Number to Write to')
parser.add_argument('--update', type=bool, default=False,
                    help='Set to true to update existing data')
parser.add_argument('--clear', type=bool, default=False,
                    help='Set to true to delete existing data'),
parser.add_argument('--region', type=str, default='us-east-1',
                    help='Region SQS Queue exists in')
args = parser.parse_args()


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

dynamodb = boto3.resource('dynamodb', region_name=args.region)

table = dynamodb.Table(args.tablename)

for i in range(args.min, args.max):
    thisnow = str(datetime.datetime.now())
    myid = hashlib.md5(str(i)).hexdigest()

    if args.clear is True:
        response = table.delete_item(Key={'id': myid})
        print("Deleted " + str(i) + ": " + str(response['ResponseMetadata']['HTTPStatusCode']))
    elif args.update is True:
        response = table.update_item(
            Key={'id': myid},
            UpdateExpression='SET updated = :val1, nonce = :val2',
            ExpressionAttributeValues={
                ':val1': thisnow,
                ':val2': ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
            }
        )
        print("Updated " + str(i) + ": " + str(response['ResponseMetadata']['HTTPStatusCode']))
    else:
        myitem = {
            'id': myid,
            'updated': thisnow,
            'created': thisnow,
            'nonce': ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20)),
            'raw': i
        }
        response = table.put_item(Item=myitem)
        print("Created " + str(i) + ": " + str(response['ResponseMetadata']['HTTPStatusCode']))

    #print(json.dumps(response, indent=4, cls=DecimalEncoder))