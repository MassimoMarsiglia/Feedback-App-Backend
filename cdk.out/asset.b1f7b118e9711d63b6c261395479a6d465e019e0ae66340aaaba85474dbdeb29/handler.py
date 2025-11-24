import os
import boto3
import json
from boto3.resources.base import ServiceResource

table_name = os.environ["FEEDBACK_TABLE"]
dynamo = boto3.resource("dynamodb")
table = dynamo.Table(table_name) # type: ignore

def lambda_handler(event, context):
    print("Received event:", event)
    return {
        "statusCode": 200,
        "body": json.dumps("OK")
    }
