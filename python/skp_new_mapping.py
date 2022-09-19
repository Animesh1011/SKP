from typing import List
from datetime import datetime
from common.schema_utils import create_schema
from jobs.reqresp.router import RouterMap, SchemaMap
import jobs.reqresp.functions as rrf
import jobs.reqresp.skp.skp_schemas as skps
import jobs.reqresp.skp.skp_functions as skpf
import jobs.reqresp.skp.skp_configs as skpc

############################## RESPONSE ##############################

RESP_POST_SKP_REBALANCE_SCHEMA_MAP: SchemaMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)): create_schema(
        skps.RESP_POST_SKP_REBALANCE_VER1,
        inf_dt=True,
        scl=2
    )
}


RESP_GET_SKP_REBALANCE_SCHEMA_MAP: SchemaMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)): create_schema(
        skps.RESP_GET_SKP_REBALANCE_VER1,
        inf_dt=True,
        scl=2
    )
}

RESP_GET_SKP_PROFILE_SCHEMA_MAP: SchemaMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)): create_schema(
        skps.RESP_SKP_PROFILE_VER1,
        inf_dt=True,
        scl=2
    )
}

RESP_SKP_REBALANCE_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": lambda x : x
    }
}

RESP_GET_SKP_PROFILE_FUNC_MAP: RouterMap = {
    (datetime(2020, 7, 1), datetime(9999, 12, 31)) : {
        "function": skpf.skp_profile
    }
}