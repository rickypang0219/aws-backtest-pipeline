import { Stack, StackProps } from "aws-cdk-lib";
import { Construct } from "constructs";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";

import { CommonResources } from "./constructs/common_resources";
import { DataIngestionTask } from "./constructs/data_collection/int_trades/int_trades_data_pipeline";
import { IntTradesBacktest } from "./constructs/backtests/int_trades_ecs";

import { HyblockDataCollection } from "./constructs/data_collection/hyblock/hyblock_lambda";

export class AwsDataEtlStack extends Stack {
  readonly common: CommonResources;

  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);
    this.common = new CommonResources(this, "CommonResources");

    new s3deploy.BucketDeployment(this, "DeployGlueScript", {
      sources: [
        s3deploy.Source.asset("src/etl_pipeline/data_processing/glue_scripts/"),
      ],
      destinationBucket: this.common.glueScriptsBucket,
      destinationKeyPrefix: "scripts",
      retainOnDelete: false,
    });

    new DataIngestionTask(this, "TradeDataIngestionWorkflow", {
      common: this.common,
    });

    new HyblockDataCollection(this, "HyblockHeatmapCollectionFlow", {
      common: this.common,
    });

    new IntTradesBacktest(this, "IntTradeBacktest", {
      common: this.common,
    });
  }
}
