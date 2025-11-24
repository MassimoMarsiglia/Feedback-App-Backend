"""Data models for the Feedback App"""
from typing import Dict, Any
from datetime import datetime


class Topic:
    """Topic model representing a feedback topic"""
    
    def __init__(self, id: str, name: str, description: str):
        self.id = id
        self.name = name
        self.description = description
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Topic to dictionary for DynamoDB"""
        return {
            'id': self.id,
            'name': self.name,
            'description': self.description
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Topic':
        """Create Topic from DynamoDB dictionary"""
        return cls(
            id=data['id'],
            name=data['name'],
            description=data['description']
        )


class Feedback:
    """Feedback model representing user feedback on a topic"""
    
    def __init__(self, id: str, topic_id: str, comments: str, 
                 sentiment_score: float = None, sentiment: str = None, 
                 analyzed_at: str = None, created_at: str = None):
        self.id = id
        self.topic_id = topic_id
        self.comments = comments
        self.sentiment_score = sentiment_score  # Comprehend sentiment score (-1 to 1)
        self.sentiment = sentiment  # POSITIVE, NEGATIVE, NEUTRAL, MIXED
        self.analyzed_at = analyzed_at  # ISO timestamp when sentiment was analyzed
        self.created_at = created_at  # ISO timestamp when feedback was created
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert Feedback to dictionary for DynamoDB"""
        data = {
            'id': self.id,
            'topic_id': self.topic_id,
            'comments': self.comments
        }
        if self.sentiment_score is not None:
            data['sentiment_score'] = self.sentiment_score
        if self.sentiment:
            data['sentiment'] = self.sentiment
        if self.analyzed_at:
            data['analyzed_at'] = self.analyzed_at
        if self.created_at:
            data['created_at'] = self.created_at
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Feedback':
        """Create Feedback from DynamoDB dictionary"""
        return cls(
            id=data['id'],
            topic_id=data['topic_id'],
            comments=data['comments'],
            sentiment_score=data.get('sentiment_score'),
            sentiment=data.get('sentiment'),
            analyzed_at=data.get('analyzed_at'),
            created_at=data.get('created_at')
        )
