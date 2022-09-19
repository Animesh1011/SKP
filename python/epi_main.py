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
from jobs.reqresp.epi.epi_configs import DB
from jobs.reqresp.router import get_schema
import jobs.reqresp.epi.epi_utils as epiu
import jobs.reqresp.epi.epi_mappings as epim

datasets: List[epiu.Dataset] = [
    {
        "StagingPrefix": "master/staging/epi_pas_response",
        "Consumer": "pas",
        "consumer_id": "VGI-US-PERSONAL_ADVISOR_SERVICES",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/profiles$',
                "Schema": partial(get_schema, epim.RESP_POST_SCHEMA_MAP),
                "ProcessSets": [epiu.resp_post_process_set]
            },
            
            {
                "httpMethod": "GET",
                "Pattern": r'.*vanguard\.com/profiles/.+',
                "Schema": partial(get_schema, epim.RESP_GET_SCHEMA_MAP),
                "ProcessSets": [epiu.resp_get_process_set]
            },
            {
                "httpMethod": "PUT",
                "Pattern": r'.*vanguard\.com/profiles/.+',
                "Schema": partial(get_schema, epim.RESP_PUT_SCHEMA_MAP),
                "ProcessSets": [epiu.resp_put_process_set]
            }
        ]
    },
    {
        "StagingPrefix": "master/staging/epi_pas_request",
        "Consumer": "pas",
        "consumer_id": "VGI-US-PERSONAL_ADVISOR_SERVICES",
        "Endpoints": [
            {
                "httpMethod": "POST",
                "Pattern": r'.*vanguard\.com/profiles$',
                "Schema": partial(get_schema, epim.REQ_POST_SCHEMA_MAP),
                "ProcessSets": [epiu.req_post_process_set]
            },
            
            {
                "httpMethod": "PUT",
                "Pattern": r'.*vanguard\.com/profiles/.+',
                "Schema": partial(get_schema, epim.REQ_PUT_SCHEMA_MAP),
                "ProcessSets": [epiu.req_put_process_set]
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
    spark.sparkContext.setCheckpointDir("hdfs:///home/hadoop/app/spark-checkpoint/epi")
    create_database(spark, DB)
    spark.sql(f"use {DB}")
    for date in PartitionDates(dates, use_default=BooleanParser(default_date)).join_dates():
        date_process: Callable[[epiu.Dataset], None] = partial(
            epiu.process, 
            spark=spark, 
            db=DB, 
            bucket=bucket, 
            date=date,
            write_options=write_options,
            cache_options=cache_options,
            discovery_layer=BooleanParser(use_discovery)
        )
        reduce(lambda x, y: date_process(y), datasets, None)