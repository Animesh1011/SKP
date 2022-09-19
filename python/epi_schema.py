from typing import (
    Dict,
    Any
)
from decimal import Decimal

#get/put/post - exact schema
BASE_SCHEMA_VER1: Dict[str, Any] = {
  "shortTermTaxRate": Decimal("23.4"),
  "nextRebalanceDate": "2017-11-05",
  "taxBracket": "HIGH",
  "accounts": [
    {
      "accountId": "479-943_abc-XYZ",
      "defaultSecurityPositionInstructions": [
        {
          "securityId": "GOOG",
          "securityIdType": "VANGUARD_FUND_ID",
          "positionInstruction": "FORCED_HOLD"
        }
      ],
      "newSpendingPosition": {
        "securityId": "0585",
        "securityIdType": "VANGUARD_FUND_ID",
        "positionInstruction": "REBAL_TO_TARGET"
      },
      "positions": [
        {
          "positionId": "111_abc-XYZ",
          "positionInstruction": "EVALUATE"
        }
      ],
      "accountRecordKeeper": "VANGUARD_INSTITUTIONAL"
    }
  ],
  "goals": [
    {
      "goalId": "275-abc_XYZ",
      "alternateSecurities": [
        {
          "securityId": "0608",
          "securityIdType": "VANGUARD_FUND_ID",
          "subAssetClass": "US_EQUITY",
          "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
          "lastUpdatedUserId": "UserAbc123"
        }
      ],
      "spendingFund": {
        "targetAmount": Decimal("2000.00"),
        "ceilingAmount": Decimal("2500.00"),
        "floorAmount": Decimal("1500.00"),
        "accountId": "479-943_abc-XYZ"
      },
      "subAssetClassOverrides": [
        {
          "subAssetClass": "US_EQUITY",
          "target": 40,
          "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
          "lastUpdatedUserId": "UserAbc123"
        }
      ],
      "tilts": {
        "bond": {
          "tilt": "CORPORATE_OVERWEIGHT",
          "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
          "lastUpdatedUserId": "UserAbc123"
        },
        "fund": {
          "tilt": "ETF",
          "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
          "lastUpdatedUserId": "UserAbc123"
        },
        "equity": {
          "tilt": "ACTIVE_MULTI",
          "activeEquityTarget": 60,
          "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
          "lastUpdatedUserId": "UserAbc123"
        }
      }
    }
  ],
  "version": 1,
  "lastUpdatedDateTime": "2020-07-21T03:38:29Z",
  "lastSavedDateTime": "2020-07-21T03:38:29Z",
  "lastUpdatedUserId": "UserAbc123",
  "profileId": "id-123_ABC"
}
#post - /profiles
REQ_CREATE_PROFILE_PORTFOLIO_VER1: Dict[str, Any] = {
  **BASE_SCHEMA_VER1
}

#post - /profiles
RESP_CREATE_PROFILE_PORTFOLIO_VER1: Dict[str, Any] = {
  **BASE_SCHEMA_VER1
}

#get - /profiles/{profileId}
RESP_RETRIEVE_PROFILE_PORTFOLIO_VER1: Dict[str, Any] = {
  **BASE_SCHEMA_VER1
}

#put - /profiles/{profileId}
REQ_UPDATE_PROFILE_PORTFOLIO_VER1: Dict[str, Any] = {
  **BASE_SCHEMA_VER1
}

#put - /profiles/{profileId}
RESP_UPDATE_PROFILE_PORTFOLIO_VER1: Dict[str, Any] = {
  **BASE_SCHEMA_VER1
}


