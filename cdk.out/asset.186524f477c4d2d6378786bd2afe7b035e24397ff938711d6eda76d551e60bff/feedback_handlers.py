"""Lambda handlers for Feedback operations - SNS triggered"""
import os
import json
import uuid
import boto3
from datetime import datetime
from typing import Dict, Any

dynamodb = boto3.resource('dynamodb')
feedback_table = dynamodb.Table(os.environ['FEEDBACK_TABLE'])
topics_table = dynamodb.Table(os.environ['TOPICS_TABLE'])
sns_client = boto3.client('sns')


def create_feedback(event, context):
    """
    Create new feedback for a topic from SNS message
    SNS message contains: {"operation": "create_feedback", "data": {"topic_id": "string", "comments": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                
                # Verify topic exists
                topic_response = topics_table.get_item(Key={'id': data['topic_id']})
                if 'Item' not in topic_response:
                    print(f"Topic not found: {data['topic_id']}")
                    return {'statusCode': 404, 'body': json.dumps({
                        'error': f"Topic with id {data['topic_id']} not found"
                    })}
                
                # Create feedback with timestamp
                feedback_id = str(uuid.uuid4())
                created_at = datetime.utcnow().isoformat()
                feedback = {
                    'id': feedback_id,
                    'topic_id': data['topic_id'],
                    'comments': data['comments'],
                    'created_at': created_at
                }
                
                feedback_table.put_item(Item=feedback)
                
                print(f"Successfully created feedback: {feedback_id}")
                
                # Publish to SNS for sentiment analysis
                sentiment_sns_arn = os.environ.get('SENTIMENT_ANALYSIS_SNS_ARN')
                if sentiment_sns_arn:
                    sentiment_message = {
                        'feedback_id': feedback_id,
                        'topic_id': data['topic_id'],
                        'comments': data['comments']
                    }
                    sns_client.publish(
                        TopicArn=sentiment_sns_arn,
                        Message=json.dumps(sentiment_message)
                    )
                    print(f"Published feedback {feedback_id} for sentiment analysis")
                
                return {'statusCode': 200, 'body': json.dumps(feedback)}
        
    except Exception as e:
        print(f"Error creating feedback: {str(e)}")
        raise


def get_feedback(event, context):
    """
    Get feedback by ID from SNS message
    SNS message contains: {"operation": "get_feedback", "data": {"id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                feedback_id = data['id']
                
                response = feedback_table.get_item(Key={'id': feedback_id})
                
                if 'Item' not in response:
                    print(f"Feedback not found: {feedback_id}")
                    return {'statusCode': 404, 'body': json.dumps({'error': 'Feedback not found'})}
                
                print(f"Successfully retrieved feedback: {feedback_id}")
                return {'statusCode': 200, 'body': json.dumps(response['Item'])}
        
    except Exception as e:
        print(f"Error getting feedback: {str(e)}")
        raise


def delete_feedback(event, context):
    """
    Delete feedback by ID from SNS message
    SNS message contains: {"operation": "delete_feedback", "data": {"id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                feedback_id = data['id']
                
                # Check if feedback exists
                response = feedback_table.get_item(Key={'id': feedback_id})
                if 'Item' not in response:
                    print(f"Feedback not found: {feedback_id}")
                    return {'statusCode': 404, 'body': json.dumps({'error': 'Feedback not found'})}
                
                # Delete the feedback
                feedback_table.delete_item(Key={'id': feedback_id})
                
                print(f"Successfully deleted feedback: {feedback_id}")
                return {'statusCode': 200, 'body': json.dumps({
                    'message': 'Feedback deleted successfully',
                    'id': feedback_id
                })}
        
    except Exception as e:
        print(f"Error deleting feedback: {str(e)}")
        raise


def list_feedback_by_topic(event, context):
    """
    List all feedback for a specific topic from SNS message
    SNS message contains: {"operation": "list_feedback_by_topic", "data": {"topic_id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                topic_id = data['topic_id']
                
                # Verify topic exists
                topic_response = topics_table.get_item(Key={'id': topic_id})
                if 'Item' not in topic_response:
                    print(f"Topic not found: {topic_id}")
                    return {'statusCode': 404, 'body': json.dumps({'error': 'Topic not found'})}
                
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
                
                print(f"Successfully listed {len(feedback_items)} feedback items for topic {topic_id}")
                return {'statusCode': 200, 'body': json.dumps({
                    'feedback': feedback_items,
                    'count': len(feedback_items),
                    'topic_id': topic_id
                })}
        
    except Exception as e:
        print(f"Error listing feedback by topic: {str(e)}")
        raise
