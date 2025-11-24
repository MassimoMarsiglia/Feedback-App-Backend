# Feedback App Backend

AWS CDK application for managing topics and feedback with Lambda functions and DynamoDB.

## Architecture

- **DynamoDB Tables:**
  - `Topics`: Stores topic information (id, name, description)
  - `Feedback`: Stores feedback (id, topic_id, comments) with GSI on topic_id

- **Lambda Functions:**
  - Topic operations: create, get, delete, list
  - Feedback operations: create, get, delete, list by topic

- **API Gateway:** REST API with endpoints for all operations

## Project Structure

```
.
├── main.py                      # CDK stack definition
├── requirements.txt             # Python dependencies
├── lambda/
│   ├── topic_handlers.py        # Topic Lambda handlers
│   └── feedback_handlers.py     # Feedback Lambda handlers
└── store/
    ├── models.py                # Data models (Topic, Feedback)
    └── __init__.py
```

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Bootstrap CDK (first time only):
```bash
cdk bootstrap
```

3. Deploy the stack:
```bash
cdk deploy
```

## API Endpoints

### Topics
- `POST /topics` - Create a topic
  - Body: `{"name": "string", "description": "string"}`
- `GET /topics` - List all topics
- `GET /topics/{id}` - Get a specific topic
- `DELETE /topics/{id}` - Delete a topic

### Feedback
- `POST /feedback` - Create feedback
  - Body: `{"topic_id": "string", "comments": "string"}`
- `GET /feedback/{id}` - Get specific feedback
- `DELETE /feedback/{id}` - Delete feedback
- `GET /topics/{topicId}/feedback` - List all feedback for a topic

## Models

### Topic
```python
{
    "id": "uuid",
    "name": "string",
    "description": "string"
}
```

### Feedback
```python
{
    "id": "uuid",
    "topic_id": "uuid",
    "comments": "string"
}
```

## Development

To destroy the stack:
```bash
cdk destroy
```

To see the CloudFormation template:
```bash
cdk synth
```
