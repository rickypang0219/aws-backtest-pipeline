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

export interface IntTradesProps extends cdk.StackProps {
  readonly common: CommonResources;
}

export class DataIngestionTask extends Construct {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: IntTradesProps) {
    super(scope, id);

    const dataBucket = props.common.dataBucket;

    const intTradeTasksGenerator = new lambda.DockerImageFunction(
      this,
      "IntTradeTasksGenerator",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: ["data_processing.int_trades.handlers.generate_tasks_handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(1),
        memorySize: 512,
        environment: {
          BUCKET_NAME: dataBucket.bucketName,
        },
        description:
          "Generates a list of monthly download tasks for the Step Function map.",
      },
    );

    const intTradesParquetsConcatTask = new lambda.DockerImageFunction(
      this,
      "IntTradesParquetsConcatTask",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.int_trades.concat_parquets_handler.concat_parquets_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(5),
        memorySize: 4096,
        environment: {
          BUCKET_NAME: dataBucket.bucketName,
        },
        description: "Concatenate all the resampled parquet files in S3 ",
      },
    );
    dataBucket.grantPut(intTradesParquetsConcatTask);
    dataBucket.grantReadWrite(intTradesParquetsConcatTask);

    // New Lambda function to check if all tasks are STOPPED
    const checkTasksStatusLambda = new lambda.DockerImageFunction(
      this,
      "CheckTasksStatusLambda",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/etl_pipeline", {
          cmd: [
            "data_processing.int_trades.handlers.check_tasks_status_handler",
          ],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(1),
        memorySize: 128,
        description: "Checks if all ECS tasks have a LastStatus of STOPPED",
      },
    );

    const cluster = new ecs.Cluster(this, "IntTradeCluster", {
      vpc: props.common.vpc,
      clusterName: "IntTradeProcessingCluster",
      enableFargateCapacityProviders: true,
    });

    cluster.addDefaultCapacityProviderStrategy([
      { capacityProvider: "FARGATE_SPOT", weight: 1, base: 0 },
    ]);

    const fargateSecurityGroup = new ec2.SecurityGroup(
      this,
      "FargateTaskSecurityGroup",
      {
        vpc: props.common.vpc,
        description:
          "Security group for IntTrade ECS Fargate tasks in public subnet",
        allowAllOutbound: false,
      },
    );

    fargateSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      "Allow outbound HTTPS to S3 (via Endpoint) and general internet",
    );

    const dataTaskDefinition = new ecs.FargateTaskDefinition(
      this,
      "DataTaskDefinition",
      {
        memoryLimitMiB: 16384,
        cpu: 2048,
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

    const dataContainer = dataTaskDefinition.addContainer("DataContainer", {
      image: ecs.ContainerImage.fromAsset("./src/etl_pipeline", {
        file: "Dockerfile.int_trades_ecs",
        platform: Platform.LINUX_ARM64,
      }),
      memoryLimitMiB: 16384,
      cpu: 2048,
      logging: ecs.LogDrivers.awsLogs({
        streamPrefix: "DataProcessing",
        logRetention: logs.RetentionDays.ONE_DAY,
      }),
      command: ["data_processing.int_trades.ecs_task"],
      environment: {
        BUCKET_NAME: dataBucket.bucketName,
        POLARS_TEMP_DIR: "/tmp",
      },
    });

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "lambda:InvokeFunction",
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
        ],
        resources: [
          intTradeTasksGenerator.functionArn,
          intTradesParquetsConcatTask.functionArn,
          checkTasksStatusLambda.functionArn,
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

    const generateTasks = new tasks.LambdaInvoke(this, "GenerateTasks", {
      lambdaFunction: intTradeTasksGenerator,
      resultPath: "$.GenerateTasksResult",
      retryOnServiceExceptions: true,
    });

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["ecs:DescribeCapacityProviders"],
        resources: [cluster.clusterArn],
        effect: iam.Effect.ALLOW,
      }),
    );

    const processDataTask = new tasks.EcsRunTask(this, "ProcessDataTask", {
      cluster,
      taskDefinition: dataTaskDefinition,
      launchTarget: new tasks.EcsFargateLaunchTarget(),
      assignPublicIp: true,
      subnets: { subnetType: ec2.SubnetType.PUBLIC },
      securityGroups: [fargateSecurityGroup],
      containerOverrides: [
        {
          containerDefinition: dataContainer,
          environment: [
            {
              name: "COIN_NAME",
              value: sfn.JsonPath.stringAt("$.coin_name"),
            },
            {
              name: "ZIP_URL",
              value: sfn.JsonPath.stringAt("$.zip_url"),
            },
            {
              name: "ZIP_PATH",
              value: sfn.JsonPath.stringAt("$.zip_path"),
            },
          ],
        },
      ],
      resultPath: "$.ProcessDataResult",
      integrationPattern: sfn.IntegrationPattern.RUN_JOB,
    });

    const processDataTaskFailed = new sfn.Fail(this, "ProcessDataTaskFailed", {
      cause: "CSV to Parquet conversion failed",
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
      maxAttempts: 3,
      backoffRate: 2,
      interval: cdk.Duration.seconds(30),
      errors: ["States.ALL"],
    });

    const processTasks = new sfn.Map(this, "ProcessTasks", {
      maxConcurrency: 10,
      itemsPath: sfn.JsonPath.stringAt("$.GenerateTasksResult.Payload.tasks"),
      parameters: {
        "coin_name.$": "$.GenerateTasksResult.Payload.coin_name",
        "zip_url.$": "$$.Map.Item.Value.zip_url",
        "s3_bucket.$": "$$.Map.Item.Value.s3_bucket",
        "zip_path.$": "$$.Map.Item.Value.zip_path",
      },
      resultPath: "$.MapResult",
      outputPath: "$.MapResult",
    });

    processTasks.iterator(processDataTask);

    const concatParquetsTask = new tasks.LambdaInvoke(
      this,
      "ConcatParquetsTask",
      {
        lambdaFunction: intTradesParquetsConcatTask,
        payload: sfn.TaskInput.fromObject({
          "coin_name.$": "$.GenerateTasksResult.Payload.coin_name",
        }),
        resultPath: "$.ConcatParquetsResult",
        retryOnServiceExceptions: true,
      },
    );

    const definition = generateTasks
      .next(processTasks)
      .next(concatParquetsTask);

    this.stateMachine = new sfn.StateMachine(
      this,
      "DataIngestionStateMachine",
      {
        definition,
        role: props.common.sfnRole,
        timeout: cdk.Duration.hours(1),
        logs: {
          destination: new logs.LogGroup(this, "StateMachineLogGroup", {
            retention: logs.RetentionDays.ONE_DAY,
          }),
          level: sfn.LogLevel.ALL,
        },
      },
    );
  }
}
