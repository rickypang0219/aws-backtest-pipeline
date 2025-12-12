import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as iam from "aws-cdk-lib/aws-iam";
import * as glueAplha from "@aws-cdk/aws-glue-alpha";

export interface BatchProcessingJobProps {
  readonly glueRole: iam.IRole;
  readonly glueScriptsBucket: s3.IBucket;
  readonly dataBucket: s3.IBucket;
  readonly jobName: string;
  readonly pysparkScript: string;
}

export class BatchProcessingJob extends Construct {
  constructor(scope: Construct, id: string, props: BatchProcessingJobProps) {
    super(scope, id);

    const { glueRole, glueScriptsBucket, dataBucket, jobName, pysparkScript } =
      props;

    new glueAplha.PySparkEtlJob(this, "PySparkETLJob", {
      jobName: jobName,
      role: glueRole,
      script: glueAplha.Code.fromBucket(glueScriptsBucket, pysparkScript),
      glueVersion: glueAplha.GlueVersion.V4_0,
      continuousLogging: { enabled: true },
      workerType: glueAplha.WorkerType.G_2X,
      numberOfWorkers: 3,
      maxConcurrentRuns: 4,
      timeout: cdk.Duration.hours(2),
      defaultArguments: {
        "--job-bookmark-option": "job-bookmark-disable", // Job bookmark option (can be 'job-bookmark-enable')
        "--enable-metrics": "true", // Enable metrics for the job
        "--TempDir": `s3://${glueScriptsBucket.bucketName}/temporary/`, // Temporary directory for Glue
        "--S3_SOURCE_PATH": `s3://${dataBucket.bucketName}/BTCUSDT/raw_data/`, // S3 path for source data
        "--S3_TARGET_PATH": `s3://${dataBucket.bucketName}/BTCUSDT/processed_data/`, // S3 path for target processed data
      },
    });

    new cdk.CfnOutput(this, "GlueJobName", {
      value: "test_batch_processing",
      description: "The name of the Glue ETL job",
    });
  }
}
