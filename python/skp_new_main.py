from functools import reduce, partial
from pyspark.sql import SparkSession
from typing import (
    List,
    Optional,
    Union,
    Callable
)
from common.hive_utils import create_database
from common.arg_parsers import PartitionDates, BooleanParser
from jobs.reqresp.skp.skp_configs import DB
from jobs.reqresp.router import get_schema
import jobs.reqresp.skp.skp_utils as skpu
import jobs.reqresp.skp.skp_mapping as skpm

datasets: List[skpu.Dataset] = [
    {
        "StagingPrefix": "master/staging/skp_da_response",
        "Consumer": "da",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RESP_POST_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_post_skp_rebalance_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RESP_GET_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_get_skp_rebalance_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/profiles/.*/evaluate$',
                "Schema": partial(get_schema, skpm.RESP_GET_SKP_PROFILE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_get_skp_profile_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/skp_pas_response",
        "Consumer": "pas",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RESP_POST_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_post_skp_rebalance_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RESP_GET_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_get_skp_rebalance_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/profiles/.*/evaluate$',
                "Schema": partial(get_schema, skpm.RESP_GET_SKP_PROFILE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_get_skp_profile_process_set]
            }
        ]
    }
]


def run_job(
    spark: SparkSession,
    *,
    bucket: str,
    dates: Optional[str] = None,
    default_date: Union[bool, str] = True,
    write_options: Optional[str] = None,
    cache_options: Optional[bool] = True,
    use_discovery: Union[bool, str] = True
) -> None:
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 1000)
    if not use_discovery:
        spark.conf.set("spark.sql.caseSensitive", "true")
    spark.sparkContext.setCheckpointDir("hdfs:///home/hadoop/app/spark-checkpoint/skp")
    create_database(spark, DB)
    spark.sql(f"use {DB}")
    for date in PartitionDates(dates, use_default=BooleanParser(default_date)).join_dates():
        date_process: Callable[[skpu.Dataset], None] = partial(
            skpu.process,
            spark=spark,
            db=DB,
            bucket=bucket, 
            date=date,
            write_options=write_options,
            cache_options=cache_options,
            discovery_layer=BooleanParser(use_discovery)
        )
        reduce(lambda x, y: date_process(y), datasets, None)
