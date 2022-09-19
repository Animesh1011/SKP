from typing import List
from jobs.reqresp import BASE_COLUMNS

#BASE_COLUMNS = ["profileId", "correlationId", "sessionId", "consumerId", "req_resp_ts"]

DB: str = "ead_skpmaster"

BASE_PREFIX:str = "master/skp"

BOOLEAN_FIELDS: List[str] = [
    "holdRebalance",
    "skipClient"
]

DATETIME_FIELDS: List[str] = [
    "lastUpdated"
]

SKP_POST_REBALANCE_COLUMNS : List[str] = BASE_COLUMNS + [
    "holdRebalance",
    "lastUpdatedBy",
    "expirationDate",
    "lastUpdated"
]

SKP_GET_REBALANCE_COLUMNS : List[str] = BASE_COLUMNS + [
    "holdRebalance",
    "lastUpdatedBy",
    "expirationDate",
    "lastUpdated"
]

SKP_PROFILE_COLUMNS : List[str] = BASE_COLUMNS + [
    "applicationCode",
    "message",
    "eligiblePortfolioManagementTriggers",
    "skipClient"
]