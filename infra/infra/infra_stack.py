from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct

class InfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # new S3 bucket -
        #bucket = s3.Bucket(self,"QuestBucket",block_public_access=s3.BlockPublicAccess.BLOCK_ALL,)
        # I have created the s3 bucket manually in UI - s3-quest-bls-dataset-neelima
        bucket = s3.Bucket.from_bucket_name(self,"ExistingBucket","s3-quest-bls-dataset-neelima")

        # SQS queue
        queue = sqs.Queue(self,"PopulationQueue",visibility_timeout=Duration.seconds(300),)

        # Ingest Lambda (Part 1 + Part 2)
        ingest_fn = _lambda.Function(
            self,
            "IngestLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="lambda_handlers.ingest_lambda.handler",
            code=_lambda.Code.from_asset("..",exclude=[
                "infra/cdk.out/**",
                "infra/.venv/**",
                ".venv/**",
                "cdk.out/**",
                "**/__pycache__/**",
                ".git/**",
                ".idea/**",
                "*.ipynb",
                "**/*.rst",
            ],),
            timeout=Duration.minutes(10),
            memory_size=512,
            environment={
                "BUCKET": "s3-quest-bls-dataset-neelima",
                "BLS_PREFIX": "bls/pr/",
                "POP_PREFIX": "demographics",
            },
        )

        bucket.grant_read_write(ingest_fn)

        # Run daily
        events.Rule(
            self,
            "DailyIngestSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(ingest_fn)],
        )

        # Analytics Lambda (Part 3)
        analytics_fn = _lambda.Function(
            self,
            "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_8,
            handler="lambda_handlers.analytics_lambda.handler",
            code=_lambda.Code.from_asset("..",exclude=[
                "infra/cdk.out/**",
                "infra/.venv/**",
                ".venv/**",
                "cdk.out/**",
                "**/__pycache__/**",
                ".git/**",
                ".idea/**",
                "*.ipynb",
                "**/*.rst",
            ],),
            timeout=Duration.minutes(5),
            memory_size=1024,
            environment={
                "BUCKET": "s3-quest-bls-dataset-neelima",
                "BLS_KEY": "bls/pr/pr.data.0.Current",
                "TARGET_SERIES_ID": "PRS30006032",
                "TARGET_PERIOD": "Q01",
                "START_YEAR":"2013",
                "END_YEAR":"2018",
                "POP_PREFIX": "demographics",
            },
        )

        bucket.grant_read(analytics_fn)
        queue.grant_consume_messages(analytics_fn)

        # SQS -> Lambda trigger
        analytics_fn.add_event_source(
            lambda_event_sources.SqsEventSource(queue, batch_size=1)
        )

        # S3 -> SQS notification when JSON written
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="demographics"),
        )
