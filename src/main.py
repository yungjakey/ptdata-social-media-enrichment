"""Main entry point for social media analytics."""

import logging

import pandas as pd
from omegaconf import DictConfig, OmegaConf
from path import Path
from pydantic import BaseModel

from src.aws import AWSClient, AWSConfig
from src.azure import OpenAIConfig
from src.common import Directions, LoggerConfig
from src.models import Builder

logger = logging.getLogger(__name__)


def load_config(config_path: str | None = None) -> OmegaConf:
    """Load configuration from file or command line."""

    if not config_path:
        config_path = f"{Path(__file__).parent.parent}/config/default.yaml"
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    return OmegaConf.load(path)


def build_models(
    acl: AWSClient, acfg: AWSConfig, ocfg: OpenAIConfig
) -> tuple[BaseModel, BaseModel]:
    """Build source and target models from table schemas."""
    # Get schemas for source and target tables
    schemas = {kind.name: {} for kind in Directions}
    for kind in Directions:
        tables = getattr(acfg.db, kind.value)
        for table in tables:
            table_meta = acl.get_table(table.database, f"{table.table}")
            schemas[kind.name].update(table_meta)

    inpt = (
        Builder(name="Input Model")
        .from_schema(schemas["Input"])
        .with_primary_keys(acfg.db.primary)
        .with_openai_config(ocfg)
        .build()
    )

    outpt = (
        Builder(name="Output Model")
        .from_schema(schemas["Output"])
        .with_primary_keys(acfg.db.primary)
        .build()
    )

    return inpt, outpt


def process_source_data(acl: AWSClient, acfg: AWSConfig) -> pd.DataFrame | None:
    """Fetch and process source data."""
    dfs = []
    for source in acfg.db.source:
        query_config = source.query
        if not query_config:
            logger.warning("No query config for table %s.%s", source.database, source.table)
            continue

        # Format query template with parameters
        query = query_config.template.format(
            database=source.database, table=source.table, **(query_config.params or {})
        )

        # Execute query and get results
        df = acl.run(
            query=query,
            database=source.database,
        )

        # Convert results to DataFrame
        if df and "ResultSet" in df and "Rows" in df["ResultSet"]:
            rows = df["ResultSet"]["Rows"]
            if rows:
                columns = [col["VarCharValue"] for col in rows[0]["Data"]]
                data = [
                    [
                        row["VarCharValue"] if "VarCharValue" in row else ""
                        for row in data_row["Data"]
                    ]
                    for data_row in rows[1:]
                ]
                source_df = pd.DataFrame(data, columns=columns)
                dfs.append(source_df)

    if not dfs:
        logger.warning("No data to process")
        return None

    df = (
        pd.concat(dfs, axis=1)
        .loc[:, ~pd.concat(dfs, axis=1).columns.duplicated()]
        .drop_duplicates(subset=acfg.db.primary or None)
    )

    if df.empty:
        logger.warning("No data to process after deduplication")
        return None

    return df


# def process_target_data(acl: AWSClient, results: list[pd.DataFrame], acfg: AWSConfig):
#     """Process and write target data."""
#     for df, target in zip(results, acfg.db.target, strict=False):
#         query_config = target.query
#         if not query_config:
#             logger.warning("No query config for table %s.%s", target.database, target.table)
#             continue

#         dims_df = df.select_dtypes(exclude=["number"])
#         facts_df = df.select_dtypes(include=["number"])

#         if not dims_df.empty:
#             write_data(acl, dims_df, target, query_config.dimensions.template)
#         if not facts_df.empty:
#             write_data(acl, facts_df, target, query_config.facts.template)


# def write_data(acl: AWSClient, df: pd.DataFrame, target: Directions, template: str):
#     """Write data to target table."""
#     columns = ", ".join(df.columns)
#     placeholders = ", ".join(["%s"] * len(df.columns))
#     acl.run(
#         query=f"INSERT INTO {target.database}.{target.table} ({columns}) VALUES ({placeholders})",
#         database=target.database,
#         params=df.to_dict("records"),
#         query_template=template,
#     )


def main(config: DictConfig | None = None):
    """Main entry point."""
    config = load_config(config) if config is None else config

    LoggerConfig.from_config(**config.logging).configure()

    acfg: AWSConfig = AWSConfig.from_config(**config.aws)
    ocfg: OpenAIConfig = OpenAIConfig.from_config(**config.openai)

    with AWSClient(acfg) as acl:
        source_model, target_model = build_models(acl, acfg, ocfg)
        df = process_source_data(acl, acfg)

        # if df is not None:
        #     with OpenAIClient(ocfg) as ocl:
        #         results = asyncio.run(
        #             ocl.process_batch(df.to_dict("records"), source_model, target_model)
        #         )
        #         process_target_data(acl, results, acfg)


if __name__ == "__main__":
    main()
