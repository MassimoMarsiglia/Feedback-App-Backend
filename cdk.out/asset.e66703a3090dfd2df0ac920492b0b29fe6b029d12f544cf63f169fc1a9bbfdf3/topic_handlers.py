"""Lambda handlers for Topic operations - SNS triggered"""
import os
import json
import uuid
import boto3
from typing import Dict, Any
from mypy_boto3_dynamodb.service_resource import Table, DynamoDBServiceResource

dynamodb: DynamoDBServiceResource = boto3.resource('dynamodb') # type: ignore
topics_table: Table = dynamodb.Table(os.environ['TOPICS_TABLE'])

def create_topic(event, context):
    """
    Create a new topic from SNS message
    SNS message contains: {"operation": "create_topic", "data": {"name": "string", "description": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                
                # Create topic
                topic_id = str(uuid.uuid4())
                topic = {
                    'id': topic_id,
                    'name': data['name'],
                    'description': data['description']
                }
                
                topics_table.put_item(Item=topic)
                
                print(f"Successfully created topic: {topic_id}")
                return {'statusCode': 200, 'body': json.dumps(topic)}
        
    except Exception as e:
        print(f"Error creating topic: {str(e)}")
        raise


def get_topic(event, context):
    """
    Get a topic by ID from SNS message
    SNS message contains: {"operation": "get_topic", "data": {"id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                topic_id = data['id']
                
                response = topics_table.get_item(Key={'id': topic_id})
                
                if 'Item' not in response:
                    print(f"Topic not found: {topic_id}")
                    return {'statusCode': 404, 'body': json.dumps({'error': 'Topic not found'})}
                
                print(f"Successfully retrieved topic: {topic_id}")
                return {'statusCode': 200, 'body': json.dumps(response['Item'])}
        
    except Exception as e:
        print(f"Error getting topic: {str(e)}")
        raise


def delete_topic(event, context):
    """
    Delete a topic by ID from SNS message
    SNS message contains: {"operation": "delete_topic", "data": {"id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                topic_id = data['id']
                
                # Check if topic exists
                response = topics_table.get_item(Key={'id': topic_id})
                if 'Item' not in response:
                    print(f"Topic not found: {topic_id}")
                    return {'statusCode': 404, 'body': json.dumps({'error': 'Topic not found'})}
                
                # Delete the topic
                topics_table.delete_item(Key={'id': topic_id})
                
                print(f"Successfully deleted topic: {topic_id}")
                return {'statusCode': 200, 'body': json.dumps({
                    'message': 'Topic deleted successfully',
                    'id': topic_id
                })}
        
    except Exception as e:
        print(f"Error deleting topic: {str(e)}")
        raise


def list_topics(event, context):
    """
    List all topics from SNS message
    SNS message contains: {"operation": "list_topics", "data": {}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                response = topics_table.scan()
                topics = response.get('Items', [])
                
                # Handle pagination if needed
                while 'LastEvaluatedKey' in response:
                    response = topics_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                    topics.extend(response.get('Items', []))
                
                print(f"Successfully listed {len(topics)} topics")
                return {'statusCode': 200, 'body': json.dumps({
                    'topics': topics,
                    'count': len(topics)
                })}
        
    except Exception as e:
        print(f"Error listing topics: {str(e)}")
        raise
