"""Lambda handlers for Topic operations"""
import os
import json
import uuid
import boto3
from typing import Dict, Any

dynamodb = boto3.resource('dynamodb')
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


def create_topic(event, context):
    """
    Create a new topic
    Expected body: {"name": "string", "description": "string"}
    """
    try:
        body = json.loads(event.get('body', '{}'))
        
        # Validate required fields
        if 'name' not in body or 'description' not in body:
            return create_response(400, {
                'error': 'Missing required fields: name and description'
            })
        
        # Create topic
        topic_id = str(uuid.uuid4())
        topic = {
            'id': topic_id,
            'name': body['name'],
            'description': body['description']
        }
        
        topics_table.put_item(Item=topic)
        
        return create_response(201, topic)
        
    except Exception as e:
        print(f"Error creating topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def get_topic(event, context):
    """
    Get a topic by ID
    Path parameter: id
    """
    try:
        topic_id = event['pathParameters']['id']
        
        response = topics_table.get_item(Key={'id': topic_id})
        
        if 'Item' not in response:
            return create_response(404, {'error': 'Topic not found'})
        
        return create_response(200, response['Item'])
        
    except Exception as e:
        print(f"Error getting topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def delete_topic(event, context):
    """
    Delete a topic by ID
    Path parameter: id
    """
    try:
        topic_id = event['pathParameters']['id']
        
        # Check if topic exists
        response = topics_table.get_item(Key={'id': topic_id})
        if 'Item' not in response:
            return create_response(404, {'error': 'Topic not found'})
        
        # Delete the topic
        topics_table.delete_item(Key={'id': topic_id})
        
        return create_response(200, {
            'message': 'Topic deleted successfully',
            'id': topic_id
        })
        
    except Exception as e:
        print(f"Error deleting topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def list_topics(event, context):
    """
    List all topics
    """
    try:
        response = topics_table.scan()
        topics = response.get('Items', [])
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = topics_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            topics.extend(response.get('Items', []))
        
        return create_response(200, {
            'topics': topics,
            'count': len(topics)
        })
        
    except Exception as e:
        print(f"Error listing topics: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})
