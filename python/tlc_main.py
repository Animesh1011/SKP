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
from jobs.reqresp.tlc.tlc_configs import DB
from jobs.reqresp.router import get_schema
import jobs.reqresp.tlc.tlc_schemas as tlcs
import jobs.reqresp.tlc.tlc_utils as tlcu
import jobs.reqresp.tlc.tlc_mappings as tlcm

datasets: List[tlcu.Dataset] = [
    {
        "StagingPrefix": "master/staging/tlc_da_response",
        "Consumer": "da",
        "Endpoints": [
            {
                "Pattern": r'.*vanguard\.com/tax-loss-harvesting-calculator$',
                "Schema": partial(get_schema, tlcm.RESP_TLC_SCHEMA_MAP),
                "ProcessSets": [tlcu.resp_tlc_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/tlc_da_request",
        "Consumer": "da",
        "Endpoints": [
            {
                "Pattern": r'.*vanguard\.com/tax-loss-harvesting-calculator$',
                "Schema": partial(get_schema, tlcm.REQ_TLC_SCHEMA_MAP),
                "ProcessSets": [tlcu.req_tlc_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/tlc_pas_response",
        "Consumer": "pas",
        "Endpoints": [
            {
                "Pattern": r'.*vanguard\.com/tax-loss-harvesting-calculator$',
                "Schema": partial(get_schema, tlcm.RESP_TLC_SCHEMA_MAP),
                "ProcessSets": [tlcu.resp_tlc_process_set]
            }
        ]
    },
     {
        "StagingPrefix": "master/staging/tlc_pas_request",
        "Consumer": "pas",
        "Endpoints": [
            {
                "Pattern": r'.*vanguard\.com/tax-loss-harvesting-calculator$',
                "Schema": partial(get_schema, tlcm.REQ_TLC_SCHEMA_MAP),
                "ProcessSets": [tlcu.req_tlc_process_set]
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
    spark.sparkContext.setCheckpointDir("hdfs:///home/hadoop/app/spark-checkpoint/tlc")
    create_database(spark, DB)
    spark.sql(f"use {DB}")
    for date in PartitionDates(dates, use_default=BooleanParser(default_date)).join_dates():
        date_process: Callable[[tlcu.Dataset], None] = partial(
            tlcu.process,
            spark=spark,
            db=DB,
            bucket=bucket, 
            date=date,
            write_options=write_options,
            cache_options=cache_options,
            discovery_layer=BooleanParser(use_discovery)
        )
        reduce(lambda x, y: date_process(y), datasets, None)
