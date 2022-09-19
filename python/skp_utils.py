from typing import (
    Dict,
    List,
    Optional,
    Union,
    Callable,
    Any
)
import decimal
from functools import reduce, partial
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from common.functional import compose_functions
from jobs.reqresp.pipeline import ProcessSet, ProcessPipeline
from jobs.reqresp.router import Router
import jobs.reqresp.reqresp_utils as rru
import jobs.reqresp.functions as rrf
import jobs.reqresp.skp.skp_configs as skpc
import jobs.reqresp.skp.skp_mapping as skpm
import jobs.reqresp.skp.skp_functions as skpf

Endpoint = Dict[str, Union[str, Callable, List[Callable]]]
Dataset = Dict[str, Union[str, List[Endpoint]]]

def resp_process_set(date: datetime, consumer: str) -> ProcessSet:
    pset: ProcessSet = ProcessSet()
    dly_details_router: Router = Router(skpm.RES_DLY_DETAILS_FUNC_MAP, date)
    pset.add(
        dly_details_router,
        f'{consumer}_resp_dly_details',
        skpc.RESP_DLY_DETAILS,
        nncolumn='skipClient'    
    )
    dly_rebalance_router: Router = Router(skpm.REQ_DLY_REBALANCE_FUNC_MAP, date)
    pset.add(
        dly_rebalance_router,
        f'{consumer}_req_dly_rebalance',
        skpc.REQ_DLY_REBALANCE,
        nncolumn='lastUpdatedBy' 
    )
    dly_fees_router: Router = Router(skpm.RESP_DLY_FEES_FUNC_MAP, date)
    pset.add(
        dly_fees_router,
        f'{consumer}_resp_dly_fees',
        skpc.RESP_DLY_FEES,
        nncolumn='lastUpdatedBy' 
    )
    return pset

def process(dataset: Dataset, *, spark: SparkSession, db: str, bucket: str, date: datetime) -> None:
    consumer: str = dataset['Consumer']
    base_prefix: str = f'{skpc.BASE_PREFIX}/{consumer}'
    str_date: str = date.strftime('%Y-%m-%d')
    raw_df: Optional[DataFrame] = rru.get_input_df(spark, bucket, f"{dataset['StagingPrefix']}/{str_date}")
    if raw_df:
        preprocessed_df: DataFrame = rru.base_preprocess(raw_df).cache()
        rru.create_base_table(
            preprocessed_df,
            spark,
            db,
            bucket,
            base_prefix,
            f'{consumer}_base',
            str_date
        )
        for endpoint in dataset['Endpoints']:
            match_df: DataFrame = rrf.match_column(preprocessed_df, 'uri', endpoint['Pattern'])
            # if not match_df.rdd.isEmpty():
            prepared_df: DataFrame = rru.body_preprocess(
                spark,
                match_df,
                endpoint['Schema'](date),
                booleanfields=skpc.BOOLEAN_FIELDS,
                strfields=skpc.STRING_FIELDS,
                datefields=skpc.DATETIME_FIELDS
            )
            checkpoint_df: DataFrame = prepared_df.repartition(1000, "correlationId").checkpoint(eager=True)
            for process_set in endpoint['ProcessSets']:
                pipeline: ProcessPipeline = ProcessPipeline(
                    process_set(date, consumer),
                    spark=spark,
                    db=db,
                    bucket=bucket,
                    base_prefix=base_prefix,
                    date=str_date,
                    coalesce=20
                )
                print(f"Running {str(pipeline)} for {str_date}")
                pipeline.run(checkpoint_df)
    else:
        print(f"No data found for {dataset['StagingPrefix']} for {str_date}")
