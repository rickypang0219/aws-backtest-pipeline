import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";
import { CommonResources } from "../../common_resources";
import * as lambdaEventSources from "aws-cdk-lib/aws-lambda-event-sources";
import * as sqs from "aws-cdk-lib/aws-sqs";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";

export interface HyblockProps extends cdk.StackProps {
  readonly common: CommonResources;
}

export class HyblockDataCollection extends Construct {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: HyblockProps) {
    super(scope, id);

    const dataBucket = props.common.dataBucket;

    const hyblockStatusTable = new dynamodb.Table(this, "HyblockStatusTable", {
      tableName: "HyblockStatusTable",
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "PK", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "SK", type: dynamodb.AttributeType.STRING },
    });

    const unprocessedEventQueue = new sqs.Queue(this, "UnprocessedEventQueue", {
      queueName: "UnprocessedEventQueue",
      retentionPeriod: cdk.Duration.days(14),
    });

    const eventQueue = new sqs.Queue(this, "EventQueue", {
      queueName: "EventQueue",
      visibilityTimeout: cdk.Duration.seconds(45),
      retentionPeriod: cdk.Duration.days(14),
      deadLetterQueue: {
        queue: unprocessedEventQueue,
        maxReceiveCount: 3,
      },
    });

    const unprocessedBufferQueue = new sqs.Queue(
      this,
      "UnprocessedBufferQueue",
      {
        queueName: "UnprocessedBufferQueue",
        retentionPeriod: cdk.Duration.days(14),
      },
    );

    const bufferQueue = new sqs.Queue(this, "BufferQueue", {
      queueName: "BufferQueue",
      retentionPeriod: cdk.Duration.days(14),
      visibilityTimeout: cdk.Duration.seconds(600),
      deadLetterQueue: {
        queue: unprocessedBufferQueue,
        maxReceiveCount: 5,
      },
    });

    const hyblockHeatmapTasksGenerator = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapTasksGeneration",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.hyblock_liq_heatmap.lambdas.incremental_events_generator",
          ],
        }),
        environment: {
          SQS_QUEUE_URL: eventQueue.queueUrl,
        },
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.seconds(30),
        memorySize: 512,
        description:
          "Generates a list of daite string for download tasks for the Step Function map.",
      },
    );
    hyblockStatusTable.grantReadWriteData(hyblockHeatmapTasksGenerator);
    eventQueue.grantSendMessages(hyblockHeatmapTasksGenerator);

    hyblockHeatmapTasksGenerator.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["ssm:GetParameter", "ssm:GetParameters"],
        resources: [
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-id`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-secret`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-api-key`,
        ],
      }),
    );

    const hyblockHeatmapInitEventFunction = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapInitEventFunction",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.hyblock_liq_heatmap.lambdas.init_pipeline_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.seconds(45),
        memorySize: 512,
        description: "Start the Pipeline.",
        environment: {
          EVENT_LAMBDA_ARN: hyblockHeatmapTasksGenerator.functionArn,
        },
      },
    );
    hyblockStatusTable.grantReadWriteData(hyblockHeatmapInitEventFunction);
    hyblockHeatmapInitEventFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["ssm:GetParameter", "ssm:GetParameters"],
        resources: [
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-id`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-secret`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-api-key`,
        ],
      }),
    );
    hyblockHeatmapInitEventFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["lambda:InvokeFunction"],
        resources: [hyblockHeatmapTasksGenerator.functionArn],
      }),
    );

    const hyblockHeatmapFetcherFunction = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapFetcherFunction",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.hyblock_liq_heatmap.lambda_sqs_fetcher.sqs_consume_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        reservedConcurrentExecutions: 2,
        timeout: cdk.Duration.seconds(30),
        memorySize: 512,
        environment: {
          AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
          S3_BUCKET: dataBucket.bucketName,
          SQS_QUEUE_URL: eventQueue.queueUrl,
          BUFFER_QUEUE_URL: bufferQueue.queueUrl,
        },
        description: "Fetch Daily Heatmap using Lambda",
      },
    );
    const sqsSource = new lambdaEventSources.SqsEventSource(eventQueue, {
      batchSize: 1,
      enabled: true,
    });
    hyblockHeatmapFetcherFunction.addEventSource(sqsSource);

    dataBucket.grantReadWrite(hyblockHeatmapFetcherFunction);
    bufferQueue.grantSendMessages(hyblockHeatmapFetcherFunction);

    hyblockHeatmapFetcherFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["ssm:GetParameter", "ssm:GetParameters"],
        resources: [
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-id`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-client-secret`,
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-api-key`,
        ],
      }),
    );

    const hyblockHeatmapBatchUploadFunction = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapBatchUploadFunction",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.hyblock_liq_heatmap.sqs_batch_upload.sqs_buffer_queue_consume_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        reservedConcurrentExecutions: 1,
        timeout: cdk.Duration.minutes(5),
        memorySize: 10240,
        environment: {
          AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
          S3_BUCKET: dataBucket.bucketName,
          BUFFER_QUEUE_URL: bufferQueue.queueUrl,
          EVENT_QUEUE_URL: eventQueue.queueUrl,
          INIT_PIPELINE_ARN: hyblockHeatmapInitEventFunction.functionArn,
        },
        description: "Consume Buffer Queue events and Batch Uploads to S3",
      },
    );
    eventQueue.grant(
      hyblockHeatmapBatchUploadFunction,
      "sqs:GetQueueAttributes",
    );
    bufferQueue.grantConsumeMessages(hyblockHeatmapBatchUploadFunction);
    props.common.dataBucket.grantPut(hyblockHeatmapBatchUploadFunction);
    hyblockHeatmapBatchUploadFunction.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ["lambda:InvokeFunction"],
        resources: [hyblockHeatmapInitEventFunction.functionArn],
      }),
    );

    const consumeBufferQueueRule = new events.Rule(
      this,
      "HyblockHeatmapBatchUploadSchedule",
      {
        schedule: events.Schedule.cron({
          minute: "0/15",
          hour: "*",
          day: "*",
          month: "*",
          year: "*",
        }),
        enabled: false,
      },
    );

    consumeBufferQueueRule.addTarget(
      new targets.LambdaFunction(hyblockHeatmapBatchUploadFunction),
    );

    const hyblockHeatmapCleanDlqFunction = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapCleanDlqFunction",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.hyblock_liq_heatmap.dlq_handler.put_dead_events_to_event_queue_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(5),
        memorySize: 4096,
        environment: {
          AWS_ACCOUNT_ID: cdk.Aws.ACCOUNT_ID,
          EVENT_QUEUE_URL: eventQueue.queueUrl,
          UNPROCESSED_EVENT_QUEUE_URL: unprocessedEventQueue.queueUrl,
        },
        description: "Put the dead events from DLQ to Event Queue",
      },
    );
    unprocessedEventQueue.grantConsumeMessages(hyblockHeatmapCleanDlqFunction);
    eventQueue.grantSendMessages(hyblockHeatmapCleanDlqFunction);

    const consumeDlqEventRule = new events.Rule(
      this,
      "HyblockHeatmapCleanDlqSchedule",
      {
        schedule: events.Schedule.cron({
          minute: "0",
          hour: "0",
          day: "*",
          month: "*",
          year: "*",
        }),
      },
    );
    consumeDlqEventRule.addTarget(
      new targets.LambdaFunction(hyblockHeatmapCleanDlqFunction),
    );
  }
}
