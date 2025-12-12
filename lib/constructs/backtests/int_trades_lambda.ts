import * as cdk from "aws-cdk-lib";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as sfn from "aws-cdk-lib/aws-stepfunctions";
import * as tasks from "aws-cdk-lib/aws-stepfunctions-tasks";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import { Construct } from "constructs";
import { CommonResources } from "../common_resources";

export interface IntTradesProps extends cdk.StackProps {
  readonly common: CommonResources;
}

export class IntTradesBacktest extends Construct {
  public readonly stateMachine: sfn.StateMachine;

  constructor(scope: Construct, id: string, props: IntTradesProps) {
    super(scope, id);

    // Create a dedicated IAM role for the state machine
    const stateMachineRole = new iam.Role(this, "BacktestStateMachineRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
      description: "Role for IntTradesBacktest State Machine",
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole",
        ),
      ],
    });

    // Lambda function to generate parameters
    const paramsGenerator = new lambda.DockerImageFunction(
      this,
      "ParamsGenerator",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/backtest", {
          cmd: ["backtest_unit.int_trades.handlers.assign_params_handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(1),
        memorySize: 512,
        description: "Generates a list of Params for the Step Function map.",
      },
    );

    // Lambda function for backtest handling
    const backtestHandler = new lambda.DockerImageFunction(
      this,
      "BacktestHandler",
      {
        code: lambda.DockerImageCode.fromImageAsset("./src/backtest", {
          cmd: ["backtest_unit.int_trades.bt_handler.backtest_handler"],
        }),
        architecture: lambda.Architecture.ARM_64,
        timeout: cdk.Duration.minutes(5),
        memorySize: 2048,
        description:
          "Backtest Handler that calculates Sharpe ratio for particular set of Param.",
        environment: {
          BUCKET_NAME: props.common.dataBucket.bucketName,
        },
      },
    );

    // Grant S3 read permissions to backtestHandler
    props.common.dataBucket.grantRead(backtestHandler);

    // Grant Step Functions permissions to the state machine role
    stateMachineRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["lambda:InvokeFunction"],
        resources: [paramsGenerator.functionArn, backtestHandler.functionArn],
        effect: iam.Effect.ALLOW,
      }),
    );

    // Add permissions for Map state to start executions
    stateMachineRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["states:StartExecution"],
        resources: [
          `arn:aws:states:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:stateMachine:BacktestStateMachine-${id}`,
        ],
      }),
    );
    stateMachineRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ["states:DescribeExecution", "states:StopExecution"],
        resources: [
          `arn:aws:states:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:execution:BacktestStateMachine-${id}:*`,
        ],
      }),
    );

    // Step Functions Definition
    const generateParams = new tasks.LambdaInvoke(this, "GenerateParams", {
      lambdaFunction: paramsGenerator,
      payload: sfn.TaskInput.fromObject({
        "model_name.$": "$.model_name",
        "s3_key.$": "$.s3_key",
        "coin_name.$": "$.coin_name",
        "factor_name.$": "$.factor_name",
        "train_ratio.$": "$.train_ratio",
      }),
      outputPath: "$.Payload",
      retryOnServiceExceptions: true,
      stateName: "GenerateParamsState",
    });

    // Error handling for params generator Lambda
    const paramsFailState = new sfn.Fail(this, "ParamsFailed", {
      cause: "Parameter generation failed",
      error: "ParamsGenerationFailed",
      stateName: "ParamsFailureState",
    });

    generateParams.addCatch(paramsFailState, {
      errors: ["States.ALL"],
      resultPath: "$.Error",
    });

    // Map state to iterate over parameter pairs
    const mapState = new sfn.Map(this, "MapBacktests", {
      itemsPath: sfn.JsonPath.stringAt("$.params"),
      parameters: {
        "param.$": "$$.Map.Item.Value",
        "s3_key.$": "$.s3_key",
        "coin_name.$": "$.coin_name",
        "factor_name.$": "$.factor_name",
        "train_ratio.$": "$.train_ratio",
        "model_name.$": "$.model_name",
      },
      maxConcurrency: 25,
      outputPath: "$",
      stateName: "MapBacktestsState",
    });

    // Backtest Lambda task
    const backtestTask = new tasks.LambdaInvoke(this, "RunBacktest", {
      lambdaFunction: backtestHandler,
      payload: sfn.TaskInput.fromObject({
        "x_param.$": "$.param[0]", // Rolling window
        "y_param.$": "$.param[1]", // Threshold
        "s3_key.$": "$.s3_key",
        "coin_name.$": "$.coin_name",
        "factor_name.$": "$.factor_name",
        "train_ratio.$": "$.train_ratio",
        "model_name.$": "$.model_name",
      }),
      resultPath: "$.BacktestResult",
      stateName: "RunBacktestState",
    });

    // Error handling for backtest Lambda
    const backtestFailState = new sfn.Fail(this, "BacktestFailed", {
      cause: "Backtest task failed",
      error: "BacktestTaskFailed",
      stateName: "BacktestFailureState",
    });

    backtestTask.addCatch(backtestFailState, {
      errors: ["States.ALL"],
      resultPath: "$.Error",
    });

    // Build the Step Function workflow
    mapState.iterator(backtestTask);

    const definition = generateParams.next(mapState);

    // State Machine
    this.stateMachine = new sfn.StateMachine(this, "BacktestStateMachine", {
      definition,
      role: stateMachineRole,
      timeout: cdk.Duration.hours(2),
      stateMachineName: `BacktestStateMachine-${id}`,
      logs: {
        destination: new logs.LogGroup(this, "StateMachineLogGroup", {
          retention: logs.RetentionDays.ONE_WEEK,
        }),
        level: sfn.LogLevel.ALL,
      },
    });

    // Output the State Machine ARN
    new cdk.CfnOutput(this, "StateMachineArn", {
      value: this.stateMachine.stateMachineArn,
      description: "ARN of the Backtest Step Function",
    });
  }
}
