import boto3
import logging
import json
import gzip
import datetime as dt
from StringIO import StringIO

logger = logging.getLogger()
logger.setLevel(logging.INFO)

client = boto3.client('firehose')

def lambda_handler(event, context):

    #capture the CloudWatch log data
    outEvent = str(event['awslogs']['data'])

    #decode and unzip the log data
    outEvent = gzip.GzipFile(
        fileobj = StringIO(outEvent.decode('base64','strict'))
    ).read()

    #convert the log data from JSON into a dictionary
    cleanEvent = json.loads(outEvent)

    #initiate a list
    records = []
    source = []

    #set the name of the Kinesis Firehose Stream
    firehoseName = ''

    #loop through the events line by line
    for event in cleanEvent['logEvents']:

        #Transform the data and store it in the "Data" field.
        source = event['extractedFields']
        source['owner'] = cleanEvent['owner']
        source['logGroup'] = cleanEvent['logGroup']
        source['logStream'] = cleanEvent['logStream']
        source['message'] = event['message']

        timestamp = dt.datetime.utcfromtimestamp(log['timestamp']/1000)
        payload['timestamp'] = timestamp.isoformat() + 'Z'

        data = { 'Data': json.dumps(source) + '\n' }

        #write the data to our list
        records.insert(len(records),data)

        #limit of 500 records per batch. Break it up if you have to.
        if len(records) > 499:
            #send the response to Firehose in bulk
            SendToFireHose(firehoseName, records)

            #Empty the list
            records = []

    #when done, send the response to Firehose in bulk
    if len(records) > 0:
        SendToFireHose(firehoseName, records)

#function to send record to Kinesis Firehose
def SendToFireHose(streamName, records):
    response = client.put_record_batch(
        DeliveryStreamName = streamName,
        Records=records
    )

    #log the number of data points written to Kinesis
    print "Wrote the following records to Firehose: " + str(len(records))
