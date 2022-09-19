from typing import List
from datetime import datetime
from common.schema_utils import create_schema
from jobs.reqresp.router import RouterMap, SchemaMap
import jobs.reqresp.functions as rrf
import jobs.reqresp.skp.skp_schemas as skps
import jobs.reqresp.skp.skp_functions as skpf
import jobs.reqresp.skp.skp_configs as skpc

RES_SKP_REBALANCE_SCHEMA_MAP: SchemaMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): create_schema(
        skps.RESP_SKP_REBALANCE_VAR1,
        inf_dt=True,
        scl=2
    )
}

RES_SKP_PROFILE_SCHEMA_MAP: SchemaMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): create_schema(
        skps.RESP_SKP_PROFILE_VAR1,
        inf_dt=True,
        scl=2
    )
}

REQ_SKP_REBALANCE_SCHEMA_MAP: SchemaMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): create_schema(
        skps.RESP_SKP_REBALANCE_VAR1,
        inf_dt=True,
        scl=2
    )
}


REQ_SKP_CLIENT_REBALANCE_SCHEMA_MAP: SchemaMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): create_schema(
        skps.REQ_SKP_REBALANCE_VAR1,
        inf_dt=True,
        scl=2
    )
}

REQ_DLY_REBALANCE_FUNC_MAP: RouterMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): {
        "function": skpf.dly_rebalance,
        "args": tuple(x for x in skpc.REQ_DLY_REBALANCE if x not in skpc.BASE_COLUMNS)
    }
}

RESP_DLY_FEES_FUNC_MAP: RouterMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): {
        "function": skpf.dly_rebalance,
        "args": tuple(x for x in skpc.RESP_DLY_FEES if x not in skpc.BASE_COLUMNS)
    }
}

RES_DLY_DETAILS_FUNC_MAP: RouterMap = {
    (datetime(1970, 1, 1), datetime(9999, 12, 30)): {
        "function": skpf.dly_details,
        "args": ('skipClient')              ### is this correct
    }
}
