"""Lambda handlers for validating and routing requests to SNS"""
import os
import json
import boto3
from typing import Dict, Any

sns_client = boto3.client('sns')

# SNS Topic ARNs are dynamically retrieved from environment variables set by CDK
# These are set during deployment and passed to each validation Lambda function


def create_response(status_code: int, body: Any) -> Dict[str, Any]:
    """Create a standardized API response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(body)
    }


def publish_to_sns(topic_arn: str, message: Dict[str, Any]) -> bool:
    """Publish message to SNS topic"""
    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Message=json.dumps(message),
            MessageAttributes={
                'RequestType': {
                    'DataType': 'String',
                    'StringValue': message.get('operation', 'unknown')
                }
            }
        )
        return True
    except Exception as e:
        print(f"Error publishing to SNS: {str(e)}")
        return False


def validate_create_topic(event, context):
    """
    Validate create topic request and publish to SNS
    Expected body: {"name": "string", "description": "string"}
    """
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        if 'name' not in body:
            return create_response(400, {'error': 'Missing required field: name'})
        
        if 'description' not in body:
            return create_response(400, {'error': 'Missing required field: description'})
        
        # Validate field types and content
        if not isinstance(body['name'], str) or not body['name'].strip():
            return create_response(400, {'error': 'Field "name" must be a non-empty string'})
        
        if not isinstance(body['description'], str) or not body['description'].strip():
            return create_response(400, {'error': 'Field "description" must be a non-empty string'})
        
        # Prepare message for SNS
        message = {
            'operation': 'create_topic',
            'data': {
                'name': body['name'].strip(),
                'description': body['description'].strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['TOPIC_CREATE_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Topic creation request accepted',
            'status': 'processing'
        })
        
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        print(f"Error in validate_create_topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_get_topic(event, context):
    """
    Validate get topic request and publish to SNS
    Path parameter: id
    """
    try:
        # Validate path parameters
        if 'pathParameters' not in event or not event['pathParameters']:
            return create_response(400, {'error': 'Missing path parameter: id'})
        
        topic_id = event['pathParameters'].get('id')
        if not topic_id or not topic_id.strip():
            return create_response(400, {'error': 'Invalid topic id'})
        
        # Prepare message for SNS
        message = {
            'operation': 'get_topic',
            'data': {
                'id': topic_id.strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['TOPIC_GET_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Get topic request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_get_topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_delete_topic(event, context):
    """
    Validate delete topic request and publish to SNS
    Path parameter: id
    """
    try:
        # Validate path parameters
        if 'pathParameters' not in event or not event['pathParameters']:
            return create_response(400, {'error': 'Missing path parameter: id'})
        
        topic_id = event['pathParameters'].get('id')
        if not topic_id or not topic_id.strip():
            return create_response(400, {'error': 'Invalid topic id'})
        
        # Prepare message for SNS
        message = {
            'operation': 'delete_topic',
            'data': {
                'id': topic_id.strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['TOPIC_DELETE_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Delete topic request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_delete_topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_list_topics(event, context):
    """
    Validate list topics request and publish to SNS
    """
    try:
        # Prepare message for SNS
        message = {
            'operation': 'list_topics',
            'data': {}
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['TOPIC_LIST_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'List topics request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_list_topics: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_create_feedback(event, context):
    """
    Validate create feedback request and publish to SNS
    Expected body: {"topic_id": "string", "comments": "string"}
    """
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        if 'topic_id' not in body:
            return create_response(400, {'error': 'Missing required field: topic_id'})
        
        if 'comments' not in body:
            return create_response(400, {'error': 'Missing required field: comments'})
        
        # Validate field types and content
        if not isinstance(body['topic_id'], str) or not body['topic_id'].strip():
            return create_response(400, {'error': 'Field "topic_id" must be a non-empty string'})
        
        if not isinstance(body['comments'], str) or not body['comments'].strip():
            return create_response(400, {'error': 'Field "comments" must be a non-empty string'})
        
        # Prepare message for SNS
        message = {
            'operation': 'create_feedback',
            'data': {
                'topic_id': body['topic_id'].strip(),
                'comments': body['comments'].strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['FEEDBACK_CREATE_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Feedback creation request accepted',
            'status': 'processing'
        })
        
    except json.JSONDecodeError:
        return create_response(400, {'error': 'Invalid JSON in request body'})
    except Exception as e:
        print(f"Error in validate_create_feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_get_feedback(event, context):
    """
    Validate get feedback request and publish to SNS
    Path parameter: id
    """
    try:
        # Validate path parameters
        if 'pathParameters' not in event or not event['pathParameters']:
            return create_response(400, {'error': 'Missing path parameter: id'})
        
        feedback_id = event['pathParameters'].get('id')
        if not feedback_id or not feedback_id.strip():
            return create_response(400, {'error': 'Invalid feedback id'})
        
        # Prepare message for SNS
        message = {
            'operation': 'get_feedback',
            'data': {
                'id': feedback_id.strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['FEEDBACK_GET_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Get feedback request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_get_feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_delete_feedback(event, context):
    """
    Validate delete feedback request and publish to SNS
    Path parameter: id
    """
    try:
        # Validate path parameters
        if 'pathParameters' not in event or not event['pathParameters']:
            return create_response(400, {'error': 'Missing path parameter: id'})
        
        feedback_id = event['pathParameters'].get('id')
        if not feedback_id or not feedback_id.strip():
            return create_response(400, {'error': 'Invalid feedback id'})
        
        # Prepare message for SNS
        message = {
            'operation': 'delete_feedback',
            'data': {
                'id': feedback_id.strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['FEEDBACK_DELETE_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'Delete feedback request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_delete_feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def validate_list_feedback_by_topic(event, context):
    """
    Validate list feedback by topic request and publish to SNS
    Path parameter: id (topic id)
    """
    try:
        # Validate path parameters
        if 'pathParameters' not in event or not event['pathParameters']:
            return create_response(400, {'error': 'Missing path parameter: id'})
        
        topic_id = event['pathParameters'].get('id')
        if not topic_id or not topic_id.strip():
            return create_response(400, {'error': 'Invalid topic id'})
        
        # Prepare message for SNS
        message = {
            'operation': 'list_feedback_by_topic',
            'data': {
                'topic_id': topic_id.strip()
            }
        }
        
        # Publish to SNS using ARN from environment variable
        sns_topic_arn = os.environ['FEEDBACK_LIST_BY_TOPIC_SNS_ARN']
        if not publish_to_sns(sns_topic_arn, message):
            return create_response(500, {'error': 'Failed to process request'})
        
        return create_response(202, {
            'message': 'List feedback request accepted',
            'status': 'processing'
        })
        
    except Exception as e:
        print(f"Error in validate_list_feedback_by_topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})
