import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";
import { CommonResources } from "../../common_resources";
import { Platform } from "aws-cdk-lib/aws-ecr-assets";

export interface HyblockProps extends cdk.StackProps {
  readonly common: CommonResources;
}

export class HyblockDataCollection extends Construct {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: HyblockProps) {
    super(scope, id);

    const dataBucket = props.common.dataBucket;

    // Task Generator Lambda
    const hyblockHeatmapTasksGenerator = new lambda.DockerImageFunction(
      this,
      "HyblockHeatmapTasksGeneration",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: ["data_processing.hyblock_liq_heatmap.task_gen_handler.handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(1),
        memorySize: 512,
        description:
          "Generates a list of daily timestamps for download tasks for the Step Function map.",
      },
    );

    // ECS Task Status Checker Lambda
    const checkTaskStatus = new lambda.DockerImageFunction(
      this,
      "CheckTaskStatus",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: ["data_processing.hyblock_liq_heatmap.status_handler.handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(5),
        memorySize: 256,
        description: "Checks the status of ECS tasks for a batch.",
      },
    );

    // ECS Cluster
    const cluster = new ecs.Cluster(this, "HyblockCluster", {
      vpc: props.common.vpc,
      clusterName: "HyblockCluster",
      enableFargateCapacityProviders: true,
    });

    cluster.addDefaultCapacityProviderStrategy([
      { capacityProvider: "FARGATE_SPOT", weight: 1, base: 0 },
    ]);

    // Security Group for Fargate Tasks
    const fargateSecurityGroup = new ec2.SecurityGroup(
      this,
      "FargateTaskSecurityGroup",
      {
        vpc: props.common.vpc,
        description:
          "Security group for Hyblock ECS Fargate tasks in public subnet",
        allowAllOutbound: false,
      },
    );

    fargateSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      "Allow outbound HTTPS to API and general internet",
    );

    // Fargate Task Definition
    const dataTaskDefinition = new ecs.FargateTaskDefinition(
      this,
      "DataTaskDefinition",
      {
        memoryLimitMiB: 4096,
        cpu: 1024,
        runtimePlatform: {
          cpuArchitecture: ecs.CpuArchitecture.ARM64,
          operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        },
        executionRole: new iam.Role(this, "DataTaskExecutionRole", {
          assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
          managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName(
              "service-role/AmazonECSTaskExecutionRolePolicy",
            ),
          ],
        }),
        taskRole: new iam.Role(this, "DataTaskRole", {
          assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        }),
      },
    );

    dataTaskDefinition.taskRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        resources: [dataBucket.bucketArn, `${dataBucket.bucketArn}/*`],
        effect: iam.Effect.ALLOW,
      }),
    );

    dataTaskDefinition.taskRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["ssm:GetParameter", "ssm:GetParameters"],
        resources: [
          `arn:aws:ssm:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:parameter/hyblock-*`,
        ],
        effect: iam.Effect.ALLOW,
      }),
    );

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _dataContainer = dataTaskDefinition.addContainer("DataContainer", {
      image: ecs.ContainerImage.fromAsset("./src/etl_pipeline", {
        file: "Dockerfile.int_trades_ecs",
        platform: Platform.LINUX_ARM64,
      }),
      memoryLimitMiB: 4096,
      cpu: 1024,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: "DataProcessing",
        logRetention: logs.RetentionDays.ONE_DAY,
      }),
      command: ["data_processing.hyblock_liq_heatmap.ecs_fetcher"],
      environment: {
        POLARS_TEMP_DIR: "/tmp",
      },
    });

    // IAM Permissions for Step Functions
    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "lambda:InvokeFunction",
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
        ],
        resources: [
          hyblockHeatmapTasksGenerator.functionArn,
          checkTaskStatus.functionArn,
          dataTaskDefinition.taskDefinitionArn,
          `${dataTaskDefinition.taskDefinitionArn}:*`,
        ],
        effect: iam.Effect.ALLOW,
      }),
    );

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        resources: [
          dataTaskDefinition.executionRole!.roleArn,
          dataTaskDefinition.taskRole!.roleArn,
        ],
        effect: iam.Effect.ALLOW,
      }),
    );

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["ecs:DescribeCapacityProviders"],
        resources: [cluster.clusterArn],
        effect: iam.Effect.ALLOW,
      }),
    );

    // IAM Permissions for CheckTaskStatus Lambda
    checkTaskStatus.addToRolePolicy(
      new iam.PolicyStatement({
        actions: ["ecs:DescribeTasks"],
        resources: ["*"],
        effect: iam.Effect.ALLOW,
      }),
    );

    // Step 1: Invoke Task Generator Lambda
    const generateTasks = new tasks.LambdaInvoke(this, "GenerateTasks", {
      lambdaFunction: hyblockHeatmapTasksGenerator,
      resultPath: "$.GenerateTasksResult",
      retryOnServiceExceptions: true,
    });

    // Step 2: Batch dates into groups of 4
    const batchItems = new sfn.Pass(this, "BatchItems", {
      parameters: {
        "batchedTasks.$":
          "States.ArrayPartition($.GenerateTasksResult.Payload.daily_ts, 4)",
        "symbol.$": "$.GenerateTasksResult.Payload.symbol",
        "exchange.$": "$.GenerateTasksResult.Payload.exchange",
        "lookback.$": "$.GenerateTasksResult.Payload.lookback",
        "scaling.$": "$.GenerateTasksResult.Payload.scaling",
        "leverage.$": "$.GenerateTasksResult.Payload.leverage",
      },
      resultPath: "$.BatchResult",
    });

    // Step 3: Launch ECS task for a single date
    const processDataTask = new tasks.CallAwsService(this, "ProcessDataTask", {
      service: "ecs",
      action: "runTask",
      iamResources: ["*"],
      parameters: {
        Cluster: cluster.clusterArn,
        TaskDefinition: dataTaskDefinition.taskDefinitionArn,
        CapacityProviderStrategy: [
          {
            CapacityProvider: "FARGATE_SPOT",
            Weight: 1,
          },
        ],
        NetworkConfiguration: {
          AwsvpcConfiguration: {
            Subnets: props.common.vpc.publicSubnets.map((s) => s.subnetId),
            SecurityGroups: [fargateSecurityGroup.securityGroupId],
            AssignPublicIp: "ENABLED",
          },
        },
        Overrides: {
          ContainerOverrides: [
            {
              Name: "DataContainer",
              Environment: [
                { Name: "SYMBOL", Value: sfn.JsonPath.stringAt("$.symbol") },
                {
                  Name: "EXCHANGE",
                  Value: sfn.JsonPath.stringAt("$.exchange"),
                },
                {
                  Name: "LOOKBACK",
                  Value: sfn.JsonPath.stringAt("$.lookback"),
                },
                { Name: "SCALING", Value: sfn.JsonPath.stringAt("$.scaling") },
                {
                  Name: "LEVERAGE",
                  Value: sfn.JsonPath.stringAt("$.leverage"),
                },
                { Name: "DATE", Value: sfn.JsonPath.stringAt("$.date") },
                { Name: "S3_BUCKET", Value: dataBucket.bucketName },
              ],
            },
          ],
        },
      },
      resultPath: "$.ProcessDataResult",
    });

    // Step 4: Check for runTask failures or empty tasks
    const checkRunTaskResult = new sfn.Choice(this, "CheckRunTaskResult")
      .when(
        sfn.Condition.isPresent("$.ProcessDataResult.failures"),
        new sfn.Fail(this, "RunTaskFailed", {
          cause: "ECS runTask returned failures",
          error: "RunTaskFailed",
        }),
      )
      .when(
        sfn.Condition.or(
          sfn.Condition.not(
            sfn.Condition.isPresent("$.ProcessDataResult.tasks"),
          ),
          sfn.Condition.numberEquals("$.ProcessDataResult.tasks.length", 0),
        ),
        new sfn.Fail(this, "NoTasksLaunched", {
          cause: "No ECS tasks were launched",
          error: "NoTasksLaunched",
        }),
      )
      .otherwise(new sfn.Pass(this, "TasksLaunched"));

    // Step 5: Check ECS task status
    const checkTaskStatusTask = new tasks.LambdaInvoke(
      this,
      "CheckTaskStatusTask",
      {
        lambdaFunction: checkTaskStatus,
        payload: sfn.TaskInput.fromObject({
          clusterArn: cluster.clusterArn,
          "taskArns.$": "$.ProcessDataResult.tasks[*].taskArn",
        }),
        resultPath: "$.TaskStatusResult",
      },
    );

    // Step 6: Wait state for polling
    const waitForTasks = new sfn.Wait(this, "WaitForTasks", {
      time: sfn.WaitTime.duration(cdk.Duration.seconds(30)),
    });

    // Step 7: Choice state to check if tasks are complete
    const checkTasksComplete = new sfn.Choice(this, "CheckTasksComplete")
      .when(
        sfn.Condition.stringEquals(
          "$.TaskStatusResult.Payload.status",
          "COMPLETE",
        ),
        new sfn.Pass(this, "TasksComplete"),
      )
      .otherwise(waitForTasks);

    // Error handling for ECS tasks
    const processDataTaskFailed = new sfn.Fail(this, "ProcessDataTaskFailed", {
      cause: "Data processing failed",
      error: "EtlTaskFailed",
    });

    processDataTask.addCatch(processDataTaskFailed, {
      errors: ["States.TaskFailed", "ECS.AmazonECSException", "States.Timeout"],
      resultPath: "$.ErrorInfo",
    });

    processDataTask.addCatch(processDataTaskFailed, {
      errors: ["States.ALL"],
      resultPath: "$.ErrorInfo",
    });

    processDataTask.addRetry({
      maxAttempts: 1,
      backoffRate: 2,
      interval: cdk.Duration.seconds(30),
      errors: ["States.ALL"],
    });

    // Inner Map state: Process up to 4 dates in a batch
    const processDatesInBatch = new sfn.Map(this, "ProcessDatesInBatch", {
      maxConcurrency: 4, // Up to 4 concurrent ECS tasks per batch
      itemsPath: sfn.JsonPath.stringAt("$.batchDates"),
      parameters: {
        "symbol.$": "$.symbol",
        "exchange.$": "$.exchange",
        "lookback.$": "$.lookback",
        "scaling.$": "$.scaling",
        "leverage.$": "$.leverage",
        "date.$": "$$.Map.Item.Value",
      },
      resultPath: "$.BatchMapResult",
    });

    processDatesInBatch.iterator(
      processDataTask
        .next(checkRunTaskResult)
        .next(checkTaskStatusTask)
        .next(waitForTasks)
        .next(checkTasksComplete),
    );

    // Outer Map state: Process batches sequentially
    const processBatches = new sfn.Map(this, "ProcessBatches", {
      maxConcurrency: 1, // Process one batch at a time
      itemsPath: sfn.JsonPath.stringAt("$.BatchResult.batchedTasks"),
      parameters: {
        "symbol.$": "$.BatchResult.symbol",
        "exchange.$": "$.BatchResult.exchange",
        "lookback.$": "$.BatchResult.lookback",
        "scaling.$": "$.BatchResult.scaling",
        "leverage.$": "$.BatchResult.leverage",
        "batchDates.$": "$$.Map.Item.Value",
      },
      resultPath: "$.MapResult",
      outputPath: "$.MapResult",
    });

    processBatches.iterator(processDatesInBatch);

    // Define the state machine
    const definition = generateTasks.next(batchItems).next(processBatches);

    this.stateMachine = new sfn.StateMachine(
      this,
      "DataIngestionStateMachine",
      {
        definition,
        role: props.common.sfnRole,
        timeout: cdk.Duration.days(2), // 2-day timeout
        logs: {
          destination: new logs.LogGroup(this, "StateMachineLogGroup", {
            retention: logs.RetentionDays.ONE_DAY,
          }),
          level: sfn.LogLevel.ALL,
        },
      },
    );

    // Output the state machine ARN
    new cdk.CfnOutput(this, "StateMachineArn", {
      value: this.stateMachine.stateMachineArn,
      description: "ARN of the Data Ingestion State Machine",
    });
  }
}
