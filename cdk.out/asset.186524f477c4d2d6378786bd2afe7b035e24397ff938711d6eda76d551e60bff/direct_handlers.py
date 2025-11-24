"""Direct handlers for GET operations - no SNS, synchronous responses"""
import os
import json
import boto3
from typing import Dict, Any

dynamodb = boto3.resource('dynamodb')

def get_topics_table():
    """Lazy load topics table"""
    return dynamodb.Table(os.environ['TOPICS_TABLE'])

def get_feedback_table():
    """Lazy load feedback table"""
    return dynamodb.Table(os.environ['FEEDBACK_TABLE'])


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


def get_topic(event, context):
    """
    Get a topic by ID directly
    Path parameter: id
    """
    try:
        topic_id = event['pathParameters']['id']
        
        response = get_topics_table().get_item(Key={'id': topic_id})
        
        if 'Item' not in response:
            return create_response(404, {'error': 'Topic not found'})
        
        return create_response(200, response['Item'])
        
    except Exception as e:
        print(f"Error getting topic: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def list_topics(event, context):
    """
    List all topics directly
    """
    try:
        topics_table = get_topics_table()
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


def get_feedback(event, context):
    """
    Get feedback by ID directly
    Path parameter: id
    """
    try:
        feedback_id = event['pathParameters']['id']
        
        response = get_feedback_table().get_item(Key={'id': feedback_id})
        
        if 'Item' not in response:
            return create_response(404, {'error': 'Feedback not found'})
        
        return create_response(200, response['Item'])
        
    except Exception as e:
        print(f"Error getting feedback: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})


def list_feedback_by_topic(event, context):
    """
    List all feedback for a topic directly
    Path parameter: id (topic_id)
    """
    try:
        topic_id = event['pathParameters']['id']
        
        # Verify topic exists
        topic_response = get_topics_table().get_item(Key={'id': topic_id})
        if 'Item' not in topic_response:
            return create_response(404, {'error': 'Topic not found'})
        
        # Query feedback by topic_id using GSI
        feedback_table = get_feedback_table()
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


def get_sentiment_history(event, context):
    """
    Get sentiment analysis history for a topic
    Path parameter: id (topic_id)
    """
    try:
        topic_id = event['pathParameters']['id']
        
        # Verify topic exists
        topic_response = get_topics_table().get_item(Key={'id': topic_id})
        if 'Item' not in topic_response:
            return create_response(404, {'error': 'Topic not found'})
        
        # Query feedback with sentiment analysis
        feedback_table = get_feedback_table()
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
        
        # Filter only items with sentiment analysis and sort by analyzed_at
        analyzed_items = [
            item for item in feedback_items 
            if 'sentiment_score' in item and 'analyzed_at' in item
        ]
        analyzed_items.sort(key=lambda x: x.get('analyzed_at', ''), reverse=True)
        
        # Calculate aggregate statistics
        if analyzed_items:
            total_score = sum(item['sentiment_score'] for item in analyzed_items)
            avg_sentiment = total_score / len(analyzed_items)
            
            sentiment_counts = {
                'POSITIVE': sum(1 for item in analyzed_items if item.get('sentiment') == 'POSITIVE'),
                'NEGATIVE': sum(1 for item in analyzed_items if item.get('sentiment') == 'NEGATIVE'),
                'NEUTRAL': sum(1 for item in analyzed_items if item.get('sentiment') == 'NEUTRAL'),
                'MIXED': sum(1 for item in analyzed_items if item.get('sentiment') == 'MIXED')
            }
        else:
            avg_sentiment = 0
            sentiment_counts = {'POSITIVE': 0, 'NEGATIVE': 0, 'NEUTRAL': 0, 'MIXED': 0}
        
        return create_response(200, {
            'topic_id': topic_id,
            'feedback_count': len(analyzed_items),
            'average_sentiment_score': round(avg_sentiment, 3) if analyzed_items else 0,
            'sentiment_distribution': sentiment_counts,
            'feedback_history': analyzed_items
        })
        
    except Exception as e:
        print(f"Error getting sentiment history: {str(e)}")
        return create_response(500, {'error': 'Internal server error'})
