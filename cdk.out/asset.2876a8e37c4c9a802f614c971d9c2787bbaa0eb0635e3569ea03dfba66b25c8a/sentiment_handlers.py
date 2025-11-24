"""Lambda handlers for sentiment analysis using AWS Comprehend"""
import os
import json
import boto3
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any

comprehend = boto3.client('comprehend')
dynamodb = boto3.resource('dynamodb')
feedback_table = dynamodb.Table(os.environ['FEEDBACK_TABLE'])
sns_client = boto3.client('sns')


def analyze_feedback_sentiment(event, context):
    """
    Analyze sentiment of feedback using AWS Comprehend and update DynamoDB
    Triggered by SNS after feedback is created
    SNS message contains: {"feedback_id": "string", "topic_id": "string", "comments": "string"}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                feedback_id = message['feedback_id']
                topic_id = message['topic_id']
                comments = message['comments']
                
                print(f"Analyzing sentiment for feedback: {feedback_id}")
                
                # Get sentiment from AWS Comprehend
                sentiment_response = comprehend.detect_sentiment(
                    Text=comments,
                    LanguageCode='en'  # Change if you need multi-language support
                )
                
                # Extract sentiment details
                sentiment = sentiment_response['Sentiment']  # POSITIVE, NEGATIVE, NEUTRAL, MIXED
                sentiment_scores = sentiment_response['SentimentScore']
                
                # Calculate overall sentiment score (-1 to 1)
                # Positive contributes positively, Negative contributes negatively
                sentiment_score = Decimal(str(
                    sentiment_scores['Positive'] - sentiment_scores['Negative']
                ))
                
                # Update feedback in DynamoDB with sentiment analysis
                analyzed_at = datetime.utcnow().isoformat()
                
                feedback_table.update_item(
                    Key={'id': feedback_id},
                    UpdateExpression='SET sentiment = :sentiment, sentiment_score = :score, analyzed_at = :analyzed_at',
                    ExpressionAttributeValues={
                        ':sentiment': sentiment,
                        ':score': sentiment_score,
                        ':analyzed_at': analyzed_at
                    }
                )
                
                print(f"Sentiment analysis complete for {feedback_id}: {sentiment} (score: {float(sentiment_score):.3f})")
                
                # Optionally publish to another SNS for notifications if sentiment is very negative
                if float(sentiment_score) < -0.5:
                    print(f"Warning: Very negative feedback detected for topic {topic_id}")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'feedback_id': feedback_id,
                        'sentiment': sentiment,
                        'sentiment_score': sentiment_score,
                        'analyzed_at': analyzed_at
                    })
                }
        
    except Exception as e:
        print(f"Error analyzing sentiment: {str(e)}")
        raise


def get_topic_sentiment_history(event, context):
    """
    Get sentiment analysis history for a specific topic from SNS message
    SNS message contains: {"operation": "get_sentiment_history", "data": {"topic_id": "string"}}
    """
    try:
        # Parse SNS message
        for record in event['Records']:
            if record['EventSource'] == 'aws:sns':
                message = json.loads(record['Sns']['Message'])
                data = message.get('data', {})
                topic_id = data['topic_id']
                
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
                
                print(f"Retrieved sentiment history for topic {topic_id}: {len(analyzed_items)} analyzed items")
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'topic_id': topic_id,
                        'feedback_count': len(analyzed_items),
                        'average_sentiment_score': round(avg_sentiment, 3),
                        'sentiment_distribution': sentiment_counts,
                        'feedback_history': analyzed_items
                    })
                }
        
    except Exception as e:
        print(f"Error getting sentiment history: {str(e)}")
        raise
