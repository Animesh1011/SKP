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
import jobs.reqresp.skp.skp_schemas as skps
import jobs.reqresp.skp.skp_utils as skpu
import jobs.reqresp.skp.skp_mapping as skpm

datasets: List[skpu.Dataset] = [
    {
        "StagingPrefix": "master/staging/skp_da_request",
        "Consumer": "da",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.REQ_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/skp_da_response",
        "Consumer": "da",
        "Endpoints": [
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RES_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/profiles/.*/evaluate$',
                "Schema": partial(get_schema, skpm.RES_SKP_PROFILE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            }
        ]
    },
     {
        "StagingPrefix": "master/staging/skp_pas_request",
        "Consumer": "pas",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.REQ_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/skp_pas_response",
        "Consumer": "pas",
        "Endpoints": [
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/clients/rebalance-hold$',
                "Schema": partial(get_schema, skpm.RES_SKP_REBALANCE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            },
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/profiles/.*/evaluate$',
                "Schema": partial(get_schema, skpm.RES_SKP_PROFILE_SCHEMA_MAP),
                "ProcessSets": [skpu.resp_process_set]
            }
        ]
    }
]

def run_job(
    spark: SparkSession,
    *,
    bucket: str,
    dates: Optional[str] = None,
    default_date: Union[bool, str] = True
) -> None:
    spark.conf.set("spark.sql.parquet.writeLegacyFormat", "true")
    spark.conf.set("spark.sql.caseSensitive", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 500)
    spark.sparkContext.setCheckpointDir("hdfs:///home/hadoop/app/spark-checkpoint/skp")
    create_database(spark, DB)
    spark.sql(f"use {DB}")
    for date in PartitionDates(dates, use_default=BooleanParser(default_date)).to_datetimes():
        date_process: Callable[[skpu.Dataset], None] = partial(skpu.process, spark=spark, db=DB, bucket=bucket, date=date)
        reduce(lambda x, y: date_process(y), datasets, None)

