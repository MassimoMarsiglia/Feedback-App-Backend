"""Lambda handlers for Feedback operations"""
import os
import json
import uuid
import boto3
from typing import Dict, Any

dynamodb = boto3.resource('dynamodb')
feedback_table = dynamodb.Table(os.environ['FEEDBACK_TABLE'])
topics_table = dynamodb.Table(os.environ['TOPICS_TABLE'])


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


def create_feedback(event, context):
    """
    Create new feedback for a topic
    Expected body: {"topic_id": "string", "comments": "string"}
    """
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        if 'topic_id' not in body or 'comments' not in body:
            return create_response(400, {
                'error': 'Missing required fields: topic_id and comments'
            })
        
        # Verify topic exists
        topic_response = topics_table.get_item(Key={'id': body['topic_id']})
        if 'Item' not in topic_response:
            return create_response(404, {
                'error': f"Topic with id {body['topic_id']} not found"
            })
        
        # Create feedback
        feedback_id = str(uuid.uuid4())
        feedback = {
            'id': feedback_id,
            'topic_id': body['topic_id'],
            'comments': body['comments']
        }
        
        feedback_table.put_item(Item=feedback)
        
        return create_response(201, feedback)
        
    except Exception as e:
        print(f"Error creating feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def get_feedback(event, context):
    """
    Get feedback by ID
    Path parameter: id
    """
    try:
        feedback_id = event['pathParameters']['id']
        
        response = feedback_table.get_item(Key={'id': feedback_id})
        
        if 'Item' not in response:
            return create_response(404, {'error': 'Feedback not found'})
        
        return create_response(200, response['Item'])
        
    except Exception as e:
        print(f"Error getting feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def delete_feedback(event, context):
    """
    Delete feedback by ID
    Path parameter: id
    """
    try:
        feedback_id = event['pathParameters']['id']
        
        # Check if feedback exists
        response = feedback_table.get_item(Key={'id': feedback_id})
        if 'Item' not in response:
            return create_response(404, {'error': 'Feedback not found'})
        
        # Delete the feedback
        feedback_table.delete_item(Key={'id': feedback_id})
        
        return create_response(200, {
            'message': 'Feedback deleted successfully',
            'id': feedback_id
        })
        
    except Exception as e:
        print(f"Error deleting feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def list_feedback_by_topic(event, context):
    """
    List all feedback for a specific topic
    Path parameter: topicId
    """
    try:
        topic_id = event['pathParameters']['topicId']
        
        # Verify topic exists
        topic_response = topics_table.get_item(Key={'id': topic_id})
        if 'Item' not in topic_response:
            return create_response(404, {'error': 'Topic not found'})
        
        # Query feedback by topic using GSI
        response = feedback_table.query(
            IndexName='topic-index',
            KeyConditionExpression='topic_id = :topic_id',
            ExpressionAttributeValues={
                ':topic_id': topic_id
            }
        )
        
        feedback_items = response.get('Items', [])
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = feedback_table.query(
                IndexName='topic-index',
                KeyConditionExpression='topic_id = :topic_id',
                ExpressionAttributeValues={
                    ':topic_id': topic_id
                },
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            feedback_items.extend(response.get('Items', []))
        
        return create_response(200, {
            'feedback': feedback_items,
            'count': len(feedback_items),
            'topic_id': topic_id
        })
        
    except Exception as e:
        print(f"Error listing feedback by topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})
