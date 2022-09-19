from typing import List
from datetime import datetime
from common.schema_utils import create_schema
from jobs.reqresp.router import RouterMap, SchemaMap
import jobs.reqresp.functions as rrf
import jobs.reqresp.iro.iro_schemas as iros
import jobs.reqresp.iro.iro_functions as irof
import jobs.reqresp.iro.iro_configs as iroc

############################## REQUEST ##############################

REQ_IRO_SCHEMA_MAP: SchemaMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)): create_schema(
        iros.REQ_CALCULATE_IRO,
        inf_dt=True,
        scl=2
    )
}

RESP_IRO_SCHEMA_MAP: SchemaMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)): create_schema(
        iros.RESP_CALCULATE_IRO,
        inf_dt=True,
        scl=2
    )
}


REQ_REBALANCEREQUEST_REQ_FUNC_MAP: RouterMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function": irof.rebalancerequest_req,
        "args": tuple(x for x in iroc.REBALANCEREQUEST_REQ if x not in iroc.BASE_COLUMNS)
    }
}

REQ_GOALS_CONF_REQ_FUNC_MAP: RouterMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function": irof.goals_conf_req,
        "args": tuple(x for x in iroc.GOALS_CONF_REQ if x not in iroc.BASE_COLUMNS)
    }
}

RESP_GOALS_RESP_FUNC_MAP: RouterMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function":irof.goals_resp,
        "args": tuple(x for x in iroc.GOALS_RESP if x not in iroc.BASE_COLUMNS)
    }
}

RESP_ACCOUNTS_RESP_FUNC_MAP: RouterMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function": irof.accounts_resp,
        "args": tuple(x for x in iroc.ACCOUNTS_RESP if x not in iroc.BASE_COLUMNS)
    }
}

RESP_POSITIONS_RESP_FUNC_MAP: RouterMap = {
    (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function": irof.positions_resp,
        "args": tuple(x for x in iroc.POSITIONS_RESP if x not in iroc.BASE_COLUMNS)
    }
}

RESP_CONFIGURATION_RESP_FUNC_MAP: RouterMap = {
     (datetime(2022, 1, 21), datetime(9999, 12, 31)) : {
        "function": irof.configuration_resp,
        "args": tuple(x for x in iroc.CONFIGURATION_RESP if x not in iroc.BASE_COLUMNS)
    }
}
