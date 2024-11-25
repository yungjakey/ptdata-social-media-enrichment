import asyncio
from typing import Literal  # Add this import at the top

from omegaconf import DictConfig, OmegaConf
from path import Path
from pydantic import BaseModel

from src.aws import AWSClient, AWSConfig
from src.azure import OpenAIClient, OpenAIConfig
from src.common import LoggerConfig
from src.models import Builder


def build_model(
    kind: Literal["source", "target"],
    acl: AWSClient,
    acfg: AWSConfig,
    ocfg: OpenAIConfig,
) -> BaseModel:
    schema = {}

    # Get tables for the specified kind (source or target)
    tables = getattr(acfg.db, kind)
    for table in tables:
        table_meta = acl.get_table(table.database, table.table)
        schema.update(table_meta.schema)

    # Get the name and prompt based on kind
    name = "Input" if kind == "source" else "Output"
    prompt = getattr(ocfg.prompts, kind)

    return (
        Builder(name)
        .from_schema(schema)
        .with_primary_keys(acfg.db.primary)
        .with_prompt(prompt)
        .build()
    )


def main(config: DictConfig):
    logging = config.logging
    lcfg = LoggerConfig.from_config(**logging)
    lcfg.configure()

    acfg: AWSConfig = AWSConfig.from_config(**config.aws)
    ocfg: OpenAIConfig = OpenAIConfig.from_config(**config.openai)

    with AWSClient(acfg) as acl:
        # Build source and target models
        source_model = build_model("source", acl, acfg, ocfg)
        target_model = build_model("target", acl, acfg, ocfg)

        # Get latest data from source tables
        dfs = []
        for source in acfg.db.source:
            # Use the database client's parameterized query execution
            df = acl.execute_query(
                query_params={
                    "database": source.database,
                    "table": source.table,
                    "limit": 1000,
                    "order_by": "date_key",
                },
                database=source.database,
                query_template="SELECT * FROM {database}.{table} ORDER BY {order_by} DESC LIMIT {limit}",
            )
            dfs.append(df)

        # Join data on primary keys
        df = dfs[0]
        for other in dfs[1:]:
            df = df.merge(other, on=acfg.db.primary, how="inner")
            df.drop_duplicates(subset=acfg.db.primary, inplace=True)

        # Process through Azure OpenAI
        with OpenAIClient(ocfg) as ocl:
            results = asyncio.run(
                ocl.process_batch(df.to_dict("records"), source_model, target_model)
            )

            # Split results into dimensions and facts based on data types
            for df, target in zip(results, acfg.db.target, strict=False):
                # Create dimension and fact dataframes
                non_numerical_cols = df.select_dtypes(exclude=["number"]).columns.tolist()
                dims_df = df[non_numerical_cols] if non_numerical_cols else None
                if dims_df and not dims_df.empty:
                    placeholders = ", ".join(["%s"] * len(dims_df.columns))
                    acl.execute_query(
                        query_params={
                            "database": target.database,
                            "table": f"dim_{target.postfix}",
                            "placeholders": placeholders,
                            "values": dims_df.values.tolist(),
                        },
                        database=target.database,
                        query_template="INSERT INTO {database}.{table} VALUES ({placeholders})",
                    )

                # Write facts
                numerical_cols = df.select_dtypes(include=["number"]).columns.tolist()
                facts_df = df[numerical_cols] if numerical_cols else None
                if facts_df and not facts_df.empty:
                    placeholders = ", ".join(["%s"] * len(facts_df.columns))
                    acl.execute_query(
                        query_params={
                            "database": target.database,
                            "table": f"fact_{target.postfix}",
                            "placeholders": placeholders,
                            "values": facts_df.values.tolist(),
                        },
                        database=target.database,
                        query_template="INSERT INTO {database}.{table} VALUES ({placeholders})",
                    )
    return


if __name__ == "__main__":
    config = DictConfig(OmegaConf.load(f"{Path(__file__)}/config.yaml"))
    main(config)
