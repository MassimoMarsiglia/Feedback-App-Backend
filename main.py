from aws_cdk import (
    App,
    Stack,
    aws_lambda as lambda_,
    aws_dynamodb as dynamodb,
    aws_apigateway as apigateway,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_iam as iam,
    RemovalPolicy
)
from constructs import Construct


class FeedbackAppStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # DynamoDB Tables
        # Topics Table
        topics_table = dynamodb.Table(
            self, "TopicsTable",
            table_name="Topics",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY  # For dev purposes
        )

        # Feedback Table
        feedback_table = dynamodb.Table(
            self, "FeedbackTable",
            table_name="Feedback",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY  # For dev purposes
        )

        # Add GSI for querying feedback by topic_id
        feedback_table.add_global_secondary_index(
            index_name="topic-index",
            partition_key=dynamodb.Attribute(
                name="topic_id",
                type=dynamodb.AttributeType.STRING
            )
        )

        # ===== SNS Topics for Topic Operations =====
        topic_delete_sns = sns.Topic(self, "TopicDeleteSNS", topic_name="TopicDelete")

        # ===== SNS Topics for Feedback Operations =====
        feedback_create_sns = sns.Topic(self, "FeedbackCreateSNS", topic_name="FeedbackCreate")
        feedback_delete_sns = sns.Topic(self, "FeedbackDeleteSNS", topic_name="FeedbackDelete")
        
        # ===== SNS Topics for Sentiment Analysis =====
        sentiment_analysis_sns = sns.Topic(self, "SentimentAnalysisSNS", topic_name="SentimentAnalysis")

        # ===== CRUD Lambda Functions for Topics (SNS-triggered for write ops) =====
        delete_topic_fn = lambda_.Function(
            self, "DeleteTopicFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="topic_handlers.delete_topic",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        # ===== Direct Lambda Functions for GET operations =====
        get_topic_fn = lambda_.Function(
            self, "GetTopicFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="direct_handlers.get_topic",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        list_topics_fn = lambda_.Function(
            self, "ListTopicsFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="direct_handlers.list_topics",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        # ===== CRUD Lambda Functions for Feedback (SNS-triggered for write ops) =====
        create_feedback_fn = lambda_.Function(
            self, "CreateFeedbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="feedback_handlers.create_feedback",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name,
                "TOPICS_TABLE": topics_table.table_name,
                "SENTIMENT_ANALYSIS_SNS_ARN": sentiment_analysis_sns.topic_arn
            }
        )

        delete_feedback_fn = lambda_.Function(
            self, "DeleteFeedbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="feedback_handlers.delete_feedback",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name
            }
        )

        # ===== Direct Lambda Functions for Feedback GET operations =====
        get_feedback_fn = lambda_.Function(
            self, "GetFeedbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="direct_handlers.get_feedback",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name
            }
        )

        list_feedback_by_topic_fn = lambda_.Function(
            self, "ListFeedbackByTopicFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="direct_handlers.list_feedback_by_topic",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name,
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        # ===== Sentiment Analysis Lambda Functions =====
        analyze_sentiment_fn = lambda_.Function(
            self, "AnalyzeSentimentFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="sentiment_handlers.analyze_feedback_sentiment",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name
            }
        )

        get_sentiment_history_fn = lambda_.Function(
            self, "GetSentimentHistoryFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="direct_handlers.get_sentiment_history",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_TABLE": feedback_table.table_name,
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        # ===== Subscribe CRUD Lambdas to SNS Topics (write operations only) =====
        topic_delete_sns.add_subscription(subscriptions.LambdaSubscription(delete_topic_fn))
        
        feedback_create_sns.add_subscription(subscriptions.LambdaSubscription(create_feedback_fn))
        feedback_delete_sns.add_subscription(subscriptions.LambdaSubscription(delete_feedback_fn))
        
        # Subscribe sentiment analysis Lambda to its SNS topic
        sentiment_analysis_sns.add_subscription(subscriptions.LambdaSubscription(analyze_sentiment_fn))

        # ===== Grant DynamoDB permissions to CRUD Lambda functions =====
        topics_table.grant_read_data(get_topic_fn)
        topics_table.grant_read_write_data(delete_topic_fn)
        topics_table.grant_read_data(list_topics_fn)

        feedback_table.grant_read_write_data(create_feedback_fn)
        topics_table.grant_read_data(create_feedback_fn)  # To verify topic exists
        feedback_table.grant_read_data(get_feedback_fn)
        feedback_table.grant_read_write_data(delete_feedback_fn)
        feedback_table.grant_read_data(list_feedback_by_topic_fn)
        topics_table.grant_read_data(list_feedback_by_topic_fn)
        
        # Grant permissions for sentiment analysis
        feedback_table.grant_read_write_data(analyze_sentiment_fn)
        feedback_table.grant_read_data(get_sentiment_history_fn)
        topics_table.grant_read_data(get_sentiment_history_fn)
        
        # Grant Comprehend permissions to sentiment analysis Lambda
        analyze_sentiment_fn.add_to_role_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "comprehend:DetectSentiment",
                "comprehend:BatchDetectSentiment"
            ],
            resources=["*"]
        ))
        
        # Grant SNS publish permission to create_feedback_fn
        sentiment_analysis_sns.grant_publish(create_feedback_fn)

        # ===== Validation Lambda Functions (write operations only) =====
        validate_create_topic_fn = lambda_.Function(
            self, "ValidateCreateTopicFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="validation_handlers.validate_create_topic",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "TOPICS_TABLE": topics_table.table_name
            }
        )

        validate_delete_topic_fn = lambda_.Function(
            self, "ValidateDeleteTopicFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="validation_handlers.validate_delete_topic",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "TOPIC_DELETE_SNS_ARN": topic_delete_sns.topic_arn
            }
        )

        validate_create_feedback_fn = lambda_.Function(
            self, "ValidateCreateFeedbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="validation_handlers.validate_create_feedback",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_CREATE_SNS_ARN": feedback_create_sns.topic_arn
            }
        )

        validate_delete_feedback_fn = lambda_.Function(
            self, "ValidateDeleteFeedbackFunction",
            runtime=lambda_.Runtime.PYTHON_3_11,
            handler="validation_handlers.validate_delete_feedback",
            code=lambda_.Code.from_asset("lambda"),
            environment={
                "FEEDBACK_DELETE_SNS_ARN": feedback_delete_sns.topic_arn
            }
        )

        # ===== Grant DynamoDB permissions to validation handler for sync topic creation =====
        topics_table.grant_write_data(validate_create_topic_fn)
        
        # ===== Grant SNS publish permissions to Validation Lambdas =====
        topic_delete_sns.grant_publish(validate_delete_topic_fn)
        
        feedback_create_sns.grant_publish(validate_create_feedback_fn)
        feedback_delete_sns.grant_publish(validate_delete_feedback_fn)

        # ===== API Gateway REST API =====
        api = apigateway.RestApi(
            self, "FeedbackApi",
            rest_api_name="Feedback Service",
            description="API for managing topics and feedback with validation layer",
            default_cors_preflight_options=apigateway.CorsOptions(
                allow_origins=apigateway.Cors.ALL_ORIGINS,
                allow_methods=apigateway.Cors.ALL_METHODS
            )
        )

        # ===== Topics endpoints =====
        topics = api.root.add_resource("topics")
        topics.add_method("POST", apigateway.LambdaIntegration(validate_create_topic_fn))  # Sync - returns topic with ID
        topics.add_method("GET", apigateway.LambdaIntegration(list_topics_fn))  # Direct/sync

        topic = topics.add_resource("{id}")
        topic.add_method("GET", apigateway.LambdaIntegration(get_topic_fn))  # Direct/sync
        topic.add_method("DELETE", apigateway.LambdaIntegration(validate_delete_topic_fn))  # Async with validation

        # Feedback by topic - uses the same {id} variable
        topic_feedback = topic.add_resource("feedback")
        topic_feedback.add_method("GET", apigateway.LambdaIntegration(list_feedback_by_topic_fn))  # Direct/sync
        
        # Sentiment history for topic
        topic_sentiment = topic.add_resource("sentiment")
        topic_sentiment.add_method("GET", apigateway.LambdaIntegration(get_sentiment_history_fn))  # Direct/sync

        # ===== Feedback endpoints =====
        feedback = api.root.add_resource("feedback")
        feedback.add_method("POST", apigateway.LambdaIntegration(validate_create_feedback_fn))  # Async with validation

        feedback_item = feedback.add_resource("{id}")
        feedback_item.add_method("GET", apigateway.LambdaIntegration(get_feedback_fn))  # Direct/sync
        feedback_item.add_method("DELETE", apigateway.LambdaIntegration(validate_delete_feedback_fn))  # Async with validation


# CDK App
app = App()
FeedbackAppStack(app, "FeedbackAppStack")
app.synth()
