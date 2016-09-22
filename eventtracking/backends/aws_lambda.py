"""Event tracking backend that sends events to Amazon Lambda"""

from __future__ import absolute_import
from datetime import datetime
from datetime import date
import logging
import json
from pytz import UTC
import os

log = logging.getLogger(__name__)

try:
    import boto3
except:
    log.warning(
        'Could not import boto for AWS_Lambda event tracker. No events will be sent to the Lambda backend.'
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
    lambda_arn = None
    client = boto3.client('lambda', region_name="us-west-1")

    def __init__(self, **kwargs):
        """
        `lambda_arn` is the full ARN for the Lambda function, e.g. arn:aws:lambda:us-west-2:account-id:function:EventTracker
        """

        # TEMP: Use enviro variable. Later this should be passed in via config
        self.lambda_arn = os.environ.get('AWS_EVENT_TRACKER_ARN')
        # self.lambda_arn = kwargs.get('lambda_arn', None)

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