import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as ecs from "aws-cdk-lib/aws-ecs";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { Construct } from "constructs";
import { CommonResources } from "../common_resources";
import { Platform } from "aws-cdk-lib/aws-ecr-assets";

export interface IntTradesProps extends cdk.StackProps {
  readonly common: CommonResources;
}

export class IntTradesBacktest extends Construct {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: IntTradesProps) {
    super(scope, id);

    const dataBucket = props.common.dataBucket;

    const taskGenerator = new lambda.DockerImageFunction(
      this,
      "TaskGenerator",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/backtest", {
          cmd: ["backtest_unit.int_trades.handlers.assign_tasks_handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(1),
        memorySize: 512,
        description:
          "Generates a list of factor tasks for the Step Function map.",
      },
    );

    // FIX #1: Set the default capacity provider strategy directly on the cluster.
    const cluster = new ecs.Cluster(this, "BacktestCluster", {
      vpc: props.common.vpc,
      clusterName: "IntTradeBacktestCluster",
      enableFargateCapacityProviders: true,
    });

    cluster.addDefaultCapacityProviderStrategy([
      {
        capacityProvider: "FARGATE_SPOT",
        weight: 1,
        base: 0,
      },
    ]);

    const fargateSecurityGroup = new ec2.SecurityGroup(
      this,
      "FargateSecurityGroup",
      {
        vpc: props.common.vpc,
        description:
          "Security group for Backtest ECS Fargate tasks in public subnet",
        allowAllOutbound: false,
      },
    );

    fargateSecurityGroup.addEgressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      "Allow outbound HTTPS to S3 and general internet",
    );

    const backtestTaskDefinition = new ecs.FargateTaskDefinition(
      this,
      "BacktestTaskDefinition",
      {
        memoryLimitMiB: 16384,
        cpu: 4096,
        runtimePlatform: {
          cpuArchitecture: ecs.CpuArchitecture.ARM64,
          operatingSystemFamily: ecs.OperatingSystemFamily.LINUX,
        },
        executionRole: new iam.Role(this, "BacktestExecutionRole", {
          assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
          managedPolicies: [
            iam.ManagedPolicy.fromAwsManagedPolicyName(
              "service-role/AmazonECSTaskExecutionRolePolicy",
            ),
          ],
        }),
        taskRole: new iam.Role(this, "BacktestTaskRole", {
          assumedBy: new iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        }),
      },
    );

    backtestTaskDefinition.taskRole.addToPrincipalPolicy(
      new iam.PolicyStatement({
        actions: ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
        resources: [dataBucket.bucketArn, `${dataBucket.bucketArn}/*`],
        effect: iam.Effect.ALLOW,
      }),
    );

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const _container = backtestTaskDefinition.addContainer(
      "BacktestContainer",
      {
        image: ecs.ContainerImage.fromAsset("./src/backtest", {
          file: "Dockerfile.int_trades_bt",
          platform: Platform.LINUX_ARM64,
        }),
        memoryLimitMiB: 16384,
        cpu: 4096,
        logging: ecs.LogDrivers.awsLogs({
          streamPrefix: "Backtest",
          logRetention: logs.RetentionDays.ONE_DAY,
        }),
        command: ["backtest_unit.int_trades.bt_ecs"],
        environment: {
          BUCKET_NAME: dataBucket.bucketName,
          POLARS_TEMP_DIR: "/tmp",
        },
      },
    );

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "lambda:InvokeFunction",
          "ecs:RunTask",
          "ecs:StopTask",
          "ecs:DescribeTasks",
        ],
        resources: [
          taskGenerator.functionArn,
          backtestTaskDefinition.taskDefinitionArn,
          `${backtestTaskDefinition.taskDefinitionArn}:*`,
        ],
        effect: iam.Effect.ALLOW,
      }),
    );

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["iam:PassRole"],
        resources: [
          backtestTaskDefinition.executionRole!.roleArn,
          backtestTaskDefinition.taskRole!.roleArn,
        ],
        effect: iam.Effect.ALLOW,
      }),
    );

    const generateTasks = new tasks.LambdaInvoke(this, "GenerateTasks", {
      lambdaFunction: taskGenerator,
      outputPath: "$.Payload",
      retryOnServiceExceptions: true,
    });

    const lambdaFailState = new sfn.Fail(this, "LambdaFailed", {
      cause: "Task generation failed",
      error: "TaskGenerationFailed",
    });

    generateTasks.addCatch(lambdaFailState, {
      errors: ["States.ALL"],
      resultPath: "$.Error",
    });

    // FIX #2: Remove the now-unnecessary CfnClusterCapacityProviderAssociations block.
    // const clusterCapacityProviders = ...

    props.common.sfnRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["ecs:DescribeCapacityProviders"],
        resources: [cluster.clusterArn],
        effect: iam.Effect.ALLOW,
      }),
    );

    // FIX #3: Replace 'EcsRunTask' with the lower-level 'CallAwsService'.
    // This allows you to explicitly specify the capacity provider strategy.
    const backtestTask = new tasks.CallAwsService(this, "RunBacktestTasks", {
      service: "ecs",
      action: "runTask",
      iamResources: ["*"],
      parameters: {
        // The keys within the 'parameters' object must be PascalCase
        Cluster: cluster.clusterArn,
        TaskDefinition: backtestTaskDefinition.taskDefinitionArn,
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
              Name: "BacktestContainer",
              Environment: [
                {
                  Name: "COIN_NAME",
                  Value: sfn.TaskInput.fromJsonPathAt("$.coin_name").value,
                },
                {
                  Name: "MODEL_NAME",
                  Value: sfn.TaskInput.fromJsonPathAt("$.model_name").value,
                },
                {
                  Name: "FACTOR_NAME",
                  Value: sfn.TaskInput.fromJsonPathAt("$.factor").value,
                },
                {
                  Name: "S3_KEY",
                  Value: sfn.TaskInput.fromJsonPathAt("$.s3_key").value,
                },
                {
                  Name: "TEST_RATIO",
                  Value: sfn.TaskInput.fromJsonPathAt(
                    "States.Format('{}', $.test_ratio)",
                  ).value,
                },
              ],
            },
          ],
        },
      },
      resultPath: "$.BacktestResult",
    });
    const failState = new sfn.Fail(this, "BacktestFailed", {
      cause: "Backtest task failed",
      error: "BacktestTaskFailed",
    });

    backtestTask.addCatch(failState, {
      errors: ["States.ALL"],
      resultPath: "$.Error",
    });

    backtestTask.addRetry({
      maxAttempts: 3,
      backoffRate: 2,
      interval: cdk.Duration.seconds(30),
      errors: ["States.ALL"],
    });

    const mapState = new sfn.Map(this, "MapTasks", {
      itemsPath: "$.tasks",
      parameters: {
        "factor.$": "$$.Map.Item.Value",
        "coin_name.$": "$.coin_name",
        "model_name.$": "$.model_name",
        "s3_key.$": "$.s3_key",
        "test_ratio.$": "$.test_ratio",
      },
      outputPath: "$",
    }).iterator(backtestTask);

    const definition = generateTasks.next(mapState);

    this.stateMachine = new sfn.StateMachine(this, "BacktestStateMachine", {
      definition,
      role: props.common.sfnRole,
      timeout: cdk.Duration.hours(2),
      logs: {
        destination: new logs.LogGroup(this, "StateMachineLogGroup", {
          retention: logs.RetentionDays.ONE_WEEK,
        }),
        level: sfn.LogLevel.ALL,
      },
    });
  }
}
