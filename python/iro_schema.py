from typing import (Any, Dict)

from decimal import Decimal


############################### REQUEST ###############################

<<<<<<< HEAD
BASE_SCHEMA_REQ: Dict[str, Any] = {
=======
BASE_SCHEMA_REQ: Dict[str,Any] = {
>>>>>>> b85b06e55cb906076fd383d4212629d2e0af9625
     "goals": [
    {
      "configuration": {
        "accountIdsExcludedFromRecommendations": [
          "string"
        ],
        "feeInvoices": [
          {
            "feeAmount": Decimal("137.29"),
            "feeCollectionEntity": "VAI",
            "invoiceId": "93d4d4b2-97a1-43d2-b8d2-b5c9b2984066",
            "payingAccounts": [
              {
                "payingAccountId": "383660320012734",
                "sweepAccountPositionId": "569374608152347"
              }
            ]
          }
        ],
        "includeRecommendationsForHypoManaged": True,
        "includeRecommendationsForPreManaged": True,
        "oneTimeWithdrawalAccountId": "709260622094716",
        "oneTimeWithdrawalAmount":Decimal("10000"),
        "oneTimeWithdrawalGoalId": "3ddf1a50-78f5-11ea-bc55-0242ac130003",
        "rebalanceType": "FULL",
        "shouldSellAllExistingRetailPositions": True,
        "withdrawalStatement": {
          "accountId": "922908769",
          "amount": {
            "currencyCode": "EUR",
            "value": Decimal("922908769")
          },
          "goalId": "72656028194936",
          "purpose": "HEALTHCARE",
          "statementId": "abcdef12-e89b-12d3-a456-426614174000",
          "sweepPositionId": "284003126032853",
          "withdrawalDate": "2021-03-19",
          "withdrawalDueDate": "2021-03-29",
          "withdrawalType":     "REQUIRED_MINIMUM_DISTRIBUTION"
        }
      },
      "goalId": "72656028194936"
    }
  ],
  "includePreviousDayBalance": True,
  "processTriggeredBy": "ACTION",
  "processType": "VPA",
  "rebalanceRequestedUserId": "UTKU",
  "runType": "BATCH"
}
