import base64
import json
import gzip
import datetime as dt
from StringIO import StringIO

def lambda_handler(event, context):
    output = []

    for record in event['records']:
        #decode and unzip the log data
        outEvent = gzip.GzipFile(
            fileobj = StringIO(record['data'].decode('base64','strict'))
        ).read()

        #convert the log data from JSON into a dictionary
        cleanEvent = json.loads(outEvent)

        #loop through the events line by line
        for log in cleanEvent['logEvents']:

            #start transform of data
            payload = log['extractedFields']
            payload['owner'] = cleanEvent['owner']
            payload['logGroup'] = cleanEvent['logGroup']
            payload['logStream'] = cleanEvent['logStream']
            payload['message'] = log['message']

            timestamp = dt.datetime.utcfromtimestamp(log['timestamp']/1000)
            payload['timestamp'] = timestamp.isoformat() + 'Z'

            output_record = {
                'recordId': record['recordId'],
                'result': 'Ok',
                'data': base64.b64encode(json.dumps(payload).encode('utf-8'))
            }

            # overwrite record id
            record['recordId'] = log['id']
            output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))

    return {'records': output}
