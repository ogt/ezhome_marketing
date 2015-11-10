import sys
import boto.sqs
import boto.sns
import json
from boto.sqs.message import Message
from datetime import datetime

import lob

AWS_ACCESS_KEY=''
AWS_SECRET_KEY=''

aws_region = "us-east-1"

topic = ""

queue_name = ''

# connection
conn = boto.sqs.connect_to_region(
     aws_region,
     aws_access_key_id=AWS_ACCESS_KEY,
     aws_secret_access_key=AWS_SECRET_KEY)

# Create or Get queue
queue = conn.create_queue(queue_name)
# Long Polling Setting
queue.set_attribute('ReceiveMessageWaitTimeSeconds', 20)

sns_connection = boto.sns.connect_to_region(
    aws_region,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY)

i=0
while 1:
    # fetch 10 messages
    msgs = queue.get_messages(10)
    for msg in msgs:
        dt = datetime.today().strftime('%Y/%m/%d %H:%M:%S')
        parameters = json.loads(msg.get_body())
        lob.api_key = parameters['lob_api_key']
        result = lob.Postcard.create(to_address = parameters['to'],
            front = parameters['front'],
            back = parameters['back'],
            setting = parameters['setting'],
            description = parameters['description'])

        sys.stdout.write("%s recv%s: dispatched\n" % (dt, str(i)))
        i = i+1
        # delete message
        queue.delete_message(msg)

        message = json.dumps({
            'to': parameters['to'],
            'description': parameters['description'],
            'campaign_name': parameters['campaign_name'],
            'region': parameters['region'],
            'parameters': parameters['template_parameters'],
            'lob_response': result,
            'event': parameters['event']
        })
        sns_connection.publish(topic = topic, message = message)

        break

    dt = datetime.today().strftime('%Y/%m/%d %H:%M:%S')
    sys.stdout.write("%s loop...\n" % (dt))