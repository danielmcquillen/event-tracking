"""Event tracking backend that sends events to Amazon Lambda"""

from __future__ import absolute_import
from datetime import datetime
from datetime import date
import logging
import json
from pytz import UTC
from django.conf import settings
import os

log = logging.getLogger(__name__)

try:
    import boto3
except:
    log.warning(
        'Could not import boto3 for AWS_Lambda event tracker. No events will be sent to the Lambda backend.'
    )


class AwsLambdaBackend(object):
    """

    Send events to Amazon Lambda, where it can be routed to other AWS resources or 3rd party applications.

    Since this is a test implementation, we're only interested in sending along a few events:

        name = edx.bi.user.account.registered
        name = edx.course.enrollment.activated

    Requires all emitted events to have the following structure (at a minimum):

        {
            'name': 'something',
            'context': {
                'user_id': 10,
            }
        }


    """

    def __init__(self):
        """
        Connect to Lambda
        """
        self.lambda_arn = getattr(settings, 'AWS_EVENT_TRACKER_ARN', None)
        access_key = getattr(settings, 'AWS_ACCESS_KEY_ID', None),
        secret_key = getattr(settings, 'AWS_SECRET_ACCESS_KEY', None),
        self.client = boto3.client('lambda', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name="us-west-2")


    def send(self, event):
        """
        Use the boto3 to send async events to AWS Lambda
        """
        if boto3 is None:
            return

        if self.lambda_arn is None:
            return

        #Encode event info
        event_str = json.dumps(event, cls=DateTimeJSONEncoder)

        # Send event
        # Use 'Event' for Invocation type so that the call is async
        response = client.invoke(
            FunctionName=self.lambda_arn,
            InvocationType='Event',
            Payload=event_str.encode('utf-8')
        )


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