import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as glue from "aws-cdk-lib/aws-glue";
import * as cdk from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as ec2 from "aws-cdk-lib/aws-ec2";

export class CommonResources extends Construct {
  public readonly dataBucket: s3.Bucket;
  public readonly glueScriptsBucket: s3.Bucket;
  public readonly sfnRole: iam.Role;
  public readonly glueRole: iam.Role;
  public readonly glueDatabase: glue.CfnDatabase;
  public readonly glueCrawler: glue.CfnCrawler;
  public readonly vpc: ec2.IVpc;
  public readonly s3GatewayEndpoint: ec2.GatewayVpcEndpoint;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    this.dataBucket = new s3.Bucket(this, "EtlDataStorageBucket", {
      bucketName: `data-bucket-v2-${cdk.Aws.ACCOUNT_ID}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    this.glueScriptsBucket = new s3.Bucket(this, "GlueScriptsBucket", {
      bucketName: `glue-script-bucket-v2-${cdk.Aws.ACCOUNT_ID}`,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    this.glueRole = new iam.Role(this, "GlueServiceRole", {
      assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSGlueServiceRole",
        ),
      ],
    });

    this.dataBucket.grantReadWrite(this.glueRole);
    this.glueScriptsBucket.grantReadWrite(this.glueRole);

    this.glueRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
          "iam:PassRole",
        ],
        resources: [
          `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/crawlers:*`,
          `arn:aws:logs:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:log-group:/aws-glue/jobs:*`,
          this.glueRole.roleArn,
        ],
        conditions: {
          StringEquals: {
            "iam:PassedToService": "glue.amazonaws.com",
          },
        },
      }),
    );

    this.sfnRole = new iam.Role(this, "SfnExecutionRole", {
      assumedBy: new iam.ServicePrincipal("states.amazonaws.com"),
      description:
        "Role for Step Functions to invoke Lambdas and start executions",
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName(
          "service-role/AWSLambdaBasicExecutionRole",
        ),
      ],
    });

    this.glueDatabase = new glue.CfnDatabase(this, "EtlGlueDatabase", {
      catalogId: cdk.Aws.ACCOUNT_ID,
      databaseInput: {
        name: "etl_glue_database",
        description: "Glue database for ETL processed data",
      },
    });

    this.glueCrawler = new glue.CfnCrawler(this, "ParquetDataCrawler", {
      name: "parquet-data-crawler",
      role: this.glueRole.roleArn,
      databaseName: this.glueDatabase.ref,
      targets: {
        s3Targets: [
          {
            path: this.dataBucket.s3UrlForObject(),
          },
        ],
      },
      schemaChangePolicy: {
        deleteBehavior: "LOG",
        updateBehavior: "UPDATE_IN_DATABASE",
      },
      tablePrefix: "parquet_",
      configuration: JSON.stringify({
        Version: 1.0,
        Grouping: {
          TableGroupingPolicy: "CombineCompatibleSchemas",
        },
        CrawlerOutput: {
          Partitions: {
            AddOrUpdateBehavior: "InheritFromTable",
          },
        },
      }),
    });

    this.vpc = new ec2.Vpc(this, "EtlVPC", {
      maxAzs: 2,
      subnetConfiguration: [
        {
          cidrMask: 24,
          name: "Public",
          subnetType: ec2.SubnetType.PUBLIC,
        },
      ],
      natGateways: 0,
    });

    this.s3GatewayEndpoint = this.vpc.addGatewayEndpoint("S3GatewayEndpoint", {
      service: ec2.GatewayVpcEndpointAwsService.S3,
    });

    new cdk.CfnOutput(this, "EtlBucketNameOutput", {
      value: this.dataBucket.bucketName,
      description: "The name of the S3 bucket for ETL data storage",
    });

    new cdk.CfnOutput(this, "EtlBucketArnOutput", {
      value: this.dataBucket.bucketArn,
      description: "The ARN of the S3 bucket for ETL data storage",
    });

    new cdk.CfnOutput(this, "GlueScriptsBucketNameOutput", {
      value: this.glueScriptsBucket.bucketName,
      description: "S3 bucket for Glue job scripts",
    });

    new cdk.CfnOutput(this, "GlueDatabaseNameOutput", {
      value: this.glueDatabase.ref,
      description: "Name of the AWS Glue Database",
    });

    new cdk.CfnOutput(this, "GlueCrawlerNameOutput", {
      value: this.glueCrawler.name!,
      description: "Name of the AWS Glue Crawler",
    });
  }
}
