"""Event tracking backend that sends events to Amazon Lambda"""

from __future__ import absolute_import
from datetime import datetime
from datetime import date
import logging
import json
from pytz import UTC
from django.conf import settings
import os
import boto3
from django.contrib.auth.models import User

# Temp: logging to tracker's log
log = logging.getLogger('track.backends.application_log')

class AwsLambdaBackend(object):
    """

    Send events to Amazon Lambda, where it can be routed to other AWS resources or 3rd party applications.

    Requires all emitted events to have the following structure (at a minimum):

        {
            'name': 'some.edx.event.name',
            'context': {
                'user_id': 10,
                'email': 'somebody@somewhere.com"
            }
        }


    """

    def __init__(self):
        """
        Connect to Lambda
        """
        self.lambda_arn = settings.AWS_EVENT_TRACKER_ARN
        access_key = settings.AWS_ACCESS_KEY_ID
        secret_key = settings.AWS_SECRET_ACCESS_KEY
        self.client = boto3.client('lambda',
                                   aws_access_key_id=access_key,
                                   aws_secret_access_key=secret_key,
                                   region_name="us-west-2")


    def send(self, event):
        """
        Use the boto3 to send async events to AWS Lambda
        """

        # Lookup user's email and set in context.
        # We're only do this b/c we whitelisted only a few events, so this
        # db operation won't happen on *every* event. Ideally, email should arrive here
        # already with email set, but that's not the case at the moment...

        context = event.get('context')
        if not context:
            log.error('AWSLambdaService: Event was missing context.', event)
            return None

        user_id = context.get('user_id')
        if not user_id:
            log.error('AWSLambdaService: user_id attribute missing from event')
            return None

        try:
            user = User.objects.get(pk=user_id)
        except User.DoesNotExist:
            log.error('Can not find a user with user_id: %s', user_id)
            return None

        setattr(context, 'email', user.email)

        #Encode event info
        event_str = json.dumps(event, cls=DateTimeJSONEncoder)

        # Send event to the target AWS Lambda function
        # Use 'Event' for Invocation type so that the call is async (?)
        #
        # Note that boto3 call should return a response as a dictionary like:
        # {
        #    'StatusCode': 123,
        #    'FunctionError': 'string',
        #     'LogResult': 'string',
        #     'Payload': StreamingBody()
        # }

        response = self.client.invoke(
            FunctionName=self.lambda_arn,
            InvocationType='Event',
            Payload=event_str.encode('utf-8')
        )

        # TODO: Do we want to log error response codes?
        log.info("AWSLambdaService: aws lambda call response: ", response)



class DateTimeJSONEncoder(json.JSONEncoder):
    """JSON encoder aware of datetime.datetime and datetime.date objects"""

    def default(self, obj):
        """
        Serialize datetime and date objects of iso format.

        datatime objects are converted to UTC.
        """

        if isinstance(obj, datetime):
            if obj.tzinfo is None:
                # Localize to UTC naive datetime objects
                obj = UTC.localize(obj)  # pylint: disable=no-value-for-parameter
            else:
                # Convert to UTC datetime objects from other timezones
                obj = obj.astimezone(UTC)
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()

        return super(DateTimeJSONEncoder, self).default(obj)