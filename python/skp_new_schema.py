from typing import (Dict,Any)

################## RESPONSE ##########################

RESP_POST_SKP_REBALANCE_VER1: Dict[str,Any] ={
  "holdRebalance": True,
  "lastUpdatedBy": "456789123",
  "expirationDate": "2020-05-16",
  "lastUpdated": "2020-05-01T13:00:00Z"
}

RESP_GET_SKP_REBALANCE_VER1: Dict[str,Any] ={
  "holdRebalance": True,
  "lastUpdatedBy": "456789123",
  "expirationDate": "2020-05-16",
  "lastUpdated": "2020-05-01T13:00:00Z"
}

RESP_SKP_PROFILE_VER1: Dict[str,Any] ={
  "details": [
    {
      "applicationCode": "ISF",
      "message": "SETTLEMENT_PENDING"
    }
  ],
  "eligiblePortfolioManagementTriggers": [
    "FEES"
  ],
  "skipClient": True
}