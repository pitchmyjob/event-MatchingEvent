from lambda_function import lambda_handler
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os


os.environ["DATABASE"] = "pitchdev"
os.environ["HOST"] = "v2-backend-dev.cw2dxsgsldpl.eu-west-1.rds.amazonaws.com"
os.environ["USER"] = "pitch"
os.environ["PASSWORD"] = "vbH456jE"
os.environ["PORT"] = "5432"
os.environ["PORT"] = "5432"

os.environ["EVENT_LOG_DYNAMODB"] = "EventLog-dev"
os.environ["EVENT_LOG_REGION"] = "eu-west-1"



session = boto3.session.Session(region_name="eu-west-1")
dynamodb = session.resource('dynamodb')
table = dynamodb.Table('EventLog-dev')


response = table.query(
    IndexName="type-timestamp-index",
    KeyConditionExpression=Key('type').eq("MatchingEvent")
)


for res in response['Items']:
    event = {
        'uuid': res['uuid'],
        'setting': "dev"
    }
    print(event)
    lambda_handler(event, None)