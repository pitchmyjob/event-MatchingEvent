import psycopg2
import boto3
import json
import time
import os


class Candidacy(object):
    conn = None
    host = None
    database = None
    user = None
    password = None
    port = None
    id = None
    event = {
        "ApplicantMatchingWasRequested" : {"lambda" : "matching-Job", "field" : "job", "insert": "job_id, applicant_id" },
        "JobMatchingWasRequested" : {"lambda" : "matching-Applicant", "field" : "applicant", "insert": "applicant_id, job_id" },
    }
    matching = None
    max_score = 0
    results = []

    def __init__(self, id, event):
        self.reset()
        self.id = id
        self.matching = self.event[event]
        self.set_variables()
        self.get_matching()
        self.insert()


    def reset(self):
        self.id = None
        self.max_score = 0
        self.results = []
        self.matching = None


    def set_variables(self):
        self.host = os.environ["HOST"]
        self.database = os.environ["DATABASE"]
        self.user = os.environ["USER"]
        self.password = os.environ["PASSWORD"]
        self.port = os.environ["PORT"]



    def insert(self):
        conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host, port=self.port)
        cur = conn.cursor()

        datas = ','.join(cur.mogrify("(%s,%s,'M', now())", (self.id, row['_id'])) for row in self.results)
        cur.execute('insert into candidacy_candidacy ('+self.matching['insert']+', status, date_matching) values ' + datas)

        conn.commit()
        conn.close()


    def get_matching(self, scroll_id=None):
        lm = boto3.client("lambda")

        payload = {
            "job" : 140,
            "scroll" : True,
            "scroll_id" : scroll_id
        }

        response = lm.invoke(FunctionName=self.matching['lambda'], InvocationType='RequestResponse', Payload=json.dumps(payload))
        res = json.load(response['Payload'])

        self.max_score = res['max_score']
        self.results =  self.results + res['results']

        if "scroll_id" in res:
            if res['scroll_id']:
                self.get_matching(res['scroll_id'])




def lambda_handler(event, context):
    dynamodb = boto3.session.Session(region_name=os.environ["EVENT_LOG_REGION"]).resource('dynamodb')
    table = dynamodb.Table(os.environ["EVENT_LOG_DYNAMODB"])

    reponse = table.get_item(Key={'type': 'MatchingEvent', 'uuid': event['uuid']})
    res = reponse['Item']

    Candidacy(res['id'], res['event'])




