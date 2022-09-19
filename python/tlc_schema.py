from typing import Any, Dict

from decimal import Decimal


############################### REQUEST ###############################

REQ_TLC_VER1: Dict[str,Any] = {
    "clientUid": "6106691000",
    "financialSecurities": [
      {
        "accountPositionId": "12345678-0025",
        "costBasisMethod": "SPEC_ID",
        "securityId": "VTI",
        "securityIdType": "VANGUARD_FUND_ID",
        "unrealizedTaxLots": [
          {
            "averageCostPerShare": Decimal("26.5"),
            "covered": True,
            "gainOrLossAmount": Decimal("-75"),
            "hasPartialCost": True,
            "id": "B53458671",
            "lotType": "UNREALIZED",
            "quantity": Decimal("50"),
            "securityId": "123456789",
            "term": "LONG_TERM",
            "tradeDateTime": "2017-01-25T00:00:00Z",
            "tradeType": "BUY"
          }
        ]
      }
    ],
     "historicalTransactions": [
      {
        "accountPositionId": "123456789",
        "securityId": "922908769",
        "securityIdType": "VANGUARD_FUND_ID",
        "tradeDate": "2020-01-23",
        "tradeStatus": "HISTORICAL",
        "tradeType": "BUY"
      }
    ],
    "realizedTaxLots": [
      {
        "accountPositionId": "123456789",
        "gainOrLossAmount": Decimal("-75"),
        "id": "B53458671",
        "lotType": "UNREALIZED",
        "securityId": "VFIAX",
        "securityIdType": "VANGUARD_FUND_ID",
        "tradeDateTime": "2017-01-25T00:00:00Z",
        "tradeType": "BUY"
      }
    ]
  }


############################### RESPONSE ###############################

RESP_TLC_VER1: Dict[str,Any] = {
  "aggregateMinimumHarvestableMarketValue": "500.0",
  "clientUid": "6106691000",
  "harvestOpportunities": [
    {
      "buyTransactions": [
        {
          "accountPositionId": "123456789",
          "alternateShareClasses": [
            {
              "securityId": "123456789",
              "tradeType": "BUY",
              "washSaleAccountPositionId": "123456789",
              "washSaleExpirationDate": "2020-01-23",
              "washSaleImminent": "true"
            }
          ],
          "securityId": "922908769",
          "tradeType": "BUY",
          "washSaleExpirationDate": "2020-01-23",
          "washSaleImminent": "true"
        }
      ],
         "opportunityCodes": [
        "HARVESTABLE"
      ],
      "opportunityDate": {
        "year": 2022,
        "month": 1,
        "day": 24
        },
      "sellTransaction": {
        "accountPositionId": "123456789",
        "alternateShareClasses": [
          {
            "securityId": "123456789",
            "tradeType": "BUY",
            "washSaleAccountPositionId": "123456789",
            "washSaleExpirationDate": "2020-01-23",
            "washSaleImminent": "true"
          }
        ],
        "costBasisMethod": "FIFO",
        "securityId": "922908769",
        "taxLots": [
          {
            "averageCostPerShare": Decimal("26.5"),
            "covered": True,
            "gainOrLossAmount": Decimal("-75"),
            "hasPartialCost": True,
            "id": "B53458671",
            "lotType": "UNREALIZED",
            "quantity": Decimal("50"),
            "securityId": "123456789",
            "term": "LONG_TERM",
            "tradeDateTimeString": "2017-01-25T00:00:00Z",
            "tradeDateTime": "",
            "tradeType": "BUY"
          }
        ],
        "tradeType": "BUY",
        "washSaleExpirationDate": "2020-01-23",
        "washSaleImminent": "true"
      }
    }
  ]
}
