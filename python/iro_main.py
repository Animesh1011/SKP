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
from jobs.reqresp.iro.iro_configs import DB
from jobs.reqresp.router import get_schema
import jobs.reqresp.iro.iro_schemas as iros
import jobs.reqresp.iro.iro_utils as irou
import jobs.reqresp.iro.iro_mappings as irom

datasets: List[irou.Dataset] = [
    {
        "StagingPrefix": "master/staging/iro_pas_request",
        "Consumer": "pas",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/calculate$',
                "Schema": partial(get_schema, irom.REQ_IRO_SCHEMA_MAP),
                "ProcessSets": [irou.req_iro_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/iro_pas_response",
        "Consumer": "pas",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/calculate$',
                "Schema": partial(get_schema, irom.RESP_IRO_SCHEMA_MAP),
                "ProcessSets": [irou.resp_iro_process_set]
            }
        ]
    },
    
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
    spark.sparkContext.setCheckpointDir("hdfs:///home/hadoop/app/spark-checkpoint/iro")
    create_database(spark, DB)
    spark.sql(f"use {DB}")
    for date in PartitionDates(dates, use_default=BooleanParser(default_date)).join_dates():
        date_process: Callable[[irou.Dataset], None] = partial(
            irou.process,
            spark=spark,
            db=DB,
            bucket=bucket, 
            date=date,
            write_options=write_options,
            cache_options=cache_options,
            discovery_layer=BooleanParser(use_discovery)
        )
        reduce(lambda x, y: date_process(y), datasets, None)
