```json
{
  "openapi": "3.0.3",
  "info": {
    "title": "Create a credit card transaction",
    "version": "1",
    "description": "Creates a new credit card transaction."
  },
  "servers": [
    {
      "url": "https://api.intacct.com/ia/api/v1",
      "x-try-it": "sage-intacct-api"
    }
  ],
  "paths": {
    "/objects/cash-management/credit-card-txn": {
      "post": {
        "summary": "Create a credit card transaction",
        "description": "Creates a new credit card transaction.",
        "tags": [
          "Cash_Management_Credit card transactions"
        ],
        "operationId": "create-cash-management-credit-card-txn",
        "requestBody": {
          "description": "",
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "description": "Transaction record containing charges made to credit and debit cards.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned unique key for the credit card transaction.",
                    "readOnly": true,
                    "example": "1001"
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the transaction.",
                    "readOnly": true,
                    "example": "1001"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the transaction.",
                    "readOnly": true,
                    "example": "/objects/cash-management/credit-card-txn/1001"
                  },
                  "txnDate": {
                    "type": "string",
                    "format": "date",
                    "example": "2021-11-30",
                    "description": "Date of the transaction."
                  },
                  "creditCardAccount": {
                    "type": "object",
                    "description": "Credit card account used for this transaction.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the credit card account.",
                        "example": "3"
                      },
                      "id": {
                        "type": "string",
                        "description": "Unique identifier for the credit card account.",
                        "example": "amex-card-1"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the credit card account.",
                        "readOnly": true,
                        "example": "/objects/cash-management/credit-card-account/3"
                      }
                    },
                    "required": [
                      "id"
                    ]
                  },
                  "referenceNumber": {
                    "type": "string",
                    "description": "Reference number for the transaction, such as the transaction number from the credit card statement.",
                    "example": "Ref--cc12"
                  },
                  "payee": {
                    "type": "string",
                    "description": "Name of the payee for the transaction.",
                    "example": "Vend-1"
                  },
                  "description": {
                    "type": "string",
                    "description": "Brief description of the purpose of the transaction.",
                    "example": "Meals"
                  },
                  "attachment": {
                    "type": "object",
                    "description": "Supporting document attached to this transaction.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the attachment.",
                        "example": "1"
                      },
                      "id": {
                        "type": "string",
                        "description": "Unique identifier for the attachment.",
                        "example": "1"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the attachment.",
                        "readOnly": true,
                        "example": "/objects/company-config/attachment/1"
                      }
                    }
                  },
                  "currency": {
                    "type": "object",
                    "description": "Currency details for the transaction.",
                    "properties": {
                      "baseCurrency": {
                        "type": "string",
                        "description": "The base currency for the entity or company.",
                        "example": "USD"
                      },
                      "txnCurrency": {
                        "type": "string",
                        "description": "For multi-currency companies, the currency in which the transaction was incurred.",
                        "example": "GBP"
                      },
                      "exchangeRate": {
                        "type": "object",
                        "title": "exchangeRate",
                        "description": "Exchange rate used to calculate the base amount for this transaction.",
                        "properties": {
                          "date": {
                            "type": "string",
                            "format": "date",
                            "example": "2021-01-23",
                            "description": "Date of the exchange rate used to calculate the base amount from the transaction amount."
                          },
                          "rate": {
                            "type": "number",
                            "description": "Exchange rate used to calculate the base amount from the transaction amount.",
                            "example": 1.0789
                          },
                          "typeId": {
                            "type": "string",
                            "description": "Exchange rate type used to calculate the base amount from the transaction amount.",
                            "example": "Intacct Daily Rate"
                          }
                        }
                      }
                    },
                    "required": [
                      "txnCurrency"
                    ]
                  },
                  "totalEntered": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "Total amount entered for the transaction in the company's base currency.",
                    "readOnly": true,
                    "example": "1500.01"
                  },
                  "txnTotalEntered": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "For multi-currency companies, the total amount entered in the transaction currency.",
                    "readOnly": true,
                    "example": "2500.10"
                  },
                  "totalPaid": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "Total amount paid for the transaction in the company's base currency.",
                    "readOnly": true,
                    "example": "101.00"
                  },
                  "txnTotalPaid": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "For multi-currency companies, the total amount paid in the transaction currency.",
                    "readOnly": true,
                    "example": "200.10"
                  },
                  "whenPaid": {
                    "type": "string",
                    "format": "date",
                    "readOnly": true,
                    "example": "2021-01-23",
                    "description": "Date the transaction was paid."
                  },
                  "state": {
                    "type": "string",
                    "description": "Current state of the credit card transaction.\nWhen a credit card transaction has been issued and is in a `posted` state, it can be reversed. When a credit card transaction is reversed, the original `posted` transaction immediately enters the `reversal` state and transitions to the `reversed` state once the reversal date is reached.\nA reversal transaction is also created in the `reversal` state when you reverse a credit card transaction. The state of the reversal transaction does not change.",
                    "enum": [
                      "reversed",
                      "reversal",
                      "posted",
                      "paid",
                      "partiallyPaid",
                      "selected",
                      "noValue"
                    ],
                    "readOnly": true,
                    "example": "posted"
                  },
                  "reconciliationState": {
                    "type": "string",
                    "description": "Indicates if this transaction has been cleared or matched for card account reconciliation.",
                    "example": "cleared",
                    "enum": [
                      "cleared",
                      "uncleared",
                      "matched"
                    ],
                    "default": "uncleared"
                  },
                  "clearingDate": {
                    "type": "string",
                    "format": "date",
                    "description": "Date when the transaction was cleared as part of the reconciliation process.",
                    "readOnly": true,
                    "example": "2024-01-23",
                    "nullable": true
                  },
                  "totalDue": {
                    "type": "string",
                    "readOnly": true,
                    "format": "decimal-precision-2",
                    "description": "Total amount due for the transaction in the company's base currency.",
                    "example": "1600.00"
                  },
                  "txnTotalDue": {
                    "type": "string",
                    "readOnly": true,
                    "format": "decimal-precision-2",
                    "description": "Total amount due in the transaction currency.",
                    "example": "1000.45"
                  },
                  "totalSelected": {
                    "type": "string",
                    "readOnly": true,
                    "format": "decimal-precision-2",
                    "description": "Total amount selected for the transaction in the company's base currency.",
                    "example": "1234.67"
                  },
                  "txnTotalSelected": {
                    "type": "string",
                    "readOnly": true,
                    "format": "decimal-precision-2",
                    "description": "Total amount selected in the transaction currency.",
                    "example": "1234.56"
                  },
                  "isInclusiveTax": {
                    "type": "boolean",
                    "description": "Indicates whether the transaction is tax inclusive.",
                    "example": true,
                    "default": false
                  },
                  "transactionSource": {
                    "type": "string",
                    "description": "Source of the transaction. For transactions auto-created from a bank feed, this value is `bank`, otherwise the value is null.",
                    "readOnly": true,
                    "enum": [
                      null,
                      "bank"
                    ],
                    "nullable": true,
                    "default": null,
                    "example": "bank"
                  },
                  "reversedBy": {
                    "type": "object",
                    "description": "For transactions with a state of `reversed`, a reference to the transaction that reversed this transaction.",
                    "readOnly": true,
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the reversing transaction.",
                        "readOnly": true,
                        "example": "221"
                      },
                      "id": {
                        "type": "string",
                        "description": "Unique identifier for the reversing transaction.",
                        "readOnly": true,
                        "example": "221"
                      },
                      "reversalDate": {
                        "type": "string",
                        "format": "date",
                        "readOnly": true,
                        "example": "2021-01-23",
                        "description": "Date the transaction was reversed."
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the reversing transaction.",
                        "readOnly": true,
                        "example": "/objects/cash-management/credit-card-txn/221"
                      }
                    }
                  },
                  "reversalOf": {
                    "type": "object",
                    "description": "For transactions with a state of `reversal`, a reference to the transaction that was reversed by this transaction.",
                    "readOnly": true,
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the reversed transaction.",
                        "readOnly": true,
                        "example": "125"
                      },
                      "id": {
                        "type": "string",
                        "description": "Unique identifier for the reversed transaction.",
                        "readOnly": true,
                        "example": "125"
                      },
                      "txnDate": {
                        "type": "string",
                        "format": "date",
                        "readOnly": true,
                        "example": "2021-10-22",
                        "description": "Date of the reversed transaction."
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the reversed transaction.",
                        "readOnly": true,
                        "example": "/objects/cash-management/credit-card-txn/125"
                      }
                    }
                  },
                  "taxSolution": {
                    "type": "object",
                    "description": "Tax solution used to calculate and capture taxes on this transaction.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the tax solution.",
                        "example": "4"
                      },
                      "id": {
                        "type": "string",
                        "description": "Unique identifier for the tax solution.",
                        "example": "Australia GST"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the tax solution.",
                        "readOnly": true,
                        "example": "/objects/tax/tax-solution/4"
                      }
                    }
                  },
                  "lines": {
                    "type": "array",
                    "description": "Line items for the credit card transaction.",
                    "items": {
                      "$ref": "#/components/schemas/objects.cash-management.credit-card-txn-line",
                      "required": [
                        "txnAmount",
                        "glAccount"
                      ]
                    }
                  },
                  "audit": {
                    "type": "object",
                    "readOnly": true,
                    "properties": {
                      "createdDateTime": {
                        "description": "Date and time when the credit card transaction was created.",
                        "type": "string",
                        "format": "date-time",
                        "readOnly": true,
                        "example": "2025-10-01T11:28:12Z"
                      },
                      "modifiedDateTime": {
                        "description": "Date and time when the record was last modified.",
                        "type": "string",
                        "format": "date-time",
                        "readOnly": true,
                        "example": "2025-09-14T21:23:42Z"
                      },
                      "createdBy": {
                        "description": "This field has been deprecated. Use the `createdByUser` field instead.",
                        "type": "string",
                        "readOnly": true,
                        "nullable": true,
                        "deprecated": true
                      },
                      "modifiedBy": {
                        "description": "This field has been deprecated. Use the `modifiedByUser` field instead.",
                        "type": "string",
                        "readOnly": true,
                        "nullable": true,
                        "deprecated": true
                      },
                      "createdByUser": {
                        "type": "object",
                        "description": "The user who created the object.",
                        "readOnly": true,
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the user.",
                            "readOnly": true,
                            "nullable": true,
                            "example": "436"
                          },
                          "id": {
                            "type": "string",
                            "description": "User login ID.",
                            "readOnly": true,
                            "nullable": true,
                            "example": "JohnDoe"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "description": "URL endpoint for the user.",
                            "example": "/objects/company-config/user/436"
                          }
                        }
                      },
                      "modifiedByUser": {
                        "type": "object",
                        "description": "The user who last modified this object.",
                        "readOnly": true,
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the user.",
                            "readOnly": true,
                            "nullable": true,
                            "example": "3086"
                          },
                          "id": {
                            "type": "string",
                            "description": "User login ID.",
                            "readOnly": true,
                            "nullable": true,
                            "example": "JaneDoe"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "description": "URL endpoint for the user.",
                            "example": "/objects/company-config/user/3086"
                          }
                        }
                      }
                    }
                  },
                  "entity": {
                    "$ref": "#/components/schemas/entity-ref"
                  }
                },
                "required": [
                  "txnDate"
                ]
              },
              "examples": {
                "Create a credit card transaction": {
                  "value": {
                    "txnDate": "2025-04-11",
                    "creditCardAccount": {
                      "id": "A0000001"
                    },
                    "referenceNumber": "CCTxn-001",
                    "payee": "Vendor-AAA",
                    "description": "Travel expenses",
                    "isInclusiveTax": true,
                    "currency": {
                      "baseCurrency": "USD",
                      "txnCurrency": "CAD",
                      "exchangeRate": {
                        "date": "2023-12-04",
                        "typeId": "Intacct Daily Rate",
                        "rate": 0.7306
                      }
                    },
                    "taxSolution": {
                      "key": "5",
                      "id": "Canadian Sales Tax - SYS"
                    },
                    "attachment": {
                      "id": "111",
                      "key": "2"
                    },
                    "lines": [
                      {
                        "glAccount": {
                          "key": "326"
                        },
                        "totalTxnAmount": "500.00",
                        "dimensions": {
                          "department": {
                            "id": "PS"
                          },
                          "location": {
                            "id": "7"
                          },
                          "project": {
                            "id": "QSF - BTI"
                          },
                          "customer": {
                            "id": "BTI"
                          },
                          "vendor": {
                            "id": "210"
                          },
                          "employee": {
                            "id": "12"
                          },
                          "item": {
                            "id": "DELL"
                          },
                          "class": {
                            "id": "WSD"
                          }
                        },
                        "description": "Travel expenses - BTI",
                        "taxEntries": [
                          {
                            "purchasingTaxDetail": {
                              "key": "65"
                            }
                          }
                        ],
                        "isBillable": false,
                        "isBilled": false
                      }
                    ]
                  }
                }
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Created",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "title": "New credit card transaction",
                  "properties": {
                    "ia::result": {
                      "$ref": "#/components/schemas/object-reference"
                    },
                    "ia::meta": {
                      "$ref": "#/components/schemas/metadata"
                    }
                  }
                },
                "examples": {
                  "Reference to new credit card transaction": {
                    "value": {
                      "ia::result": {
                        "id": "12",
                        "key": "12",
                        "href": "/objects/cash-management/credit-card-txn/12"
                      },
                      "ia::meta": {
                        "totalCount": 1,
                        "totalSuccess": 1,
                        "totalError": 0
                      }
                    }
                  }
                }
              }
            }
          },
          "400": {
            "$ref": "#/components/responses/400error"
          }
        },
        "security": [
          {
            "OAuth2": []
          }
        ],
        "x-codeSamples": [
          {
            "lang": "shell_curl",
            "name": "Create a credit card transaction",
            "source": "curl --request POST \\\n  --url https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn \\\n  --header 'Content-Type: application/json' \\\n  --data '{\"txnDate\":\"2025-04-11\",\"creditCardAccount\":{\"id\":\"A0000001\"},\"referenceNumber\":\"CCTxn-001\",\"payee\":\"Vendor-AAA\",\"description\":\"Travel expenses\",\"isInclusiveTax\":true,\"currency\":{\"baseCurrency\":\"USD\",\"txnCurrency\":\"CAD\",\"exchangeRate\":{\"date\":\"2023-12-04\",\"typeId\":\"Intacct Daily Rate\",\"rate\":0.7306}},\"taxSolution\":{\"key\":\"5\",\"id\":\"Canadian Sales Tax - SYS\"},\"attachment\":{\"id\":\"111\",\"key\":\"2\"},\"lines\":[{\"glAccount\":{\"key\":\"326\"},\"totalTxnAmount\":\"500.00\",\"dimensions\":{\"department\":{\"id\":\"PS\"},\"location\":{\"id\":\"7\"},\"project\":{\"id\":\"QSF - BTI\"},\"customer\":{\"id\":\"BTI\"},\"vendor\":{\"id\":\"210\"},\"employee\":{\"id\":\"12\"},\"item\":{\"id\":\"DELL\"},\"class\":{\"id\":\"WSD\"}},\"description\":\"Travel expenses - BTI\",\"taxEntries\":[{\"purchasingTaxDetail\":{\"key\":\"65\"}}],\"isBillable\":false,\"isBilled\":false}]}'",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_httpie",
            "name": "Create a credit card transaction",
            "source": "echo '{\"txnDate\":\"2025-04-11\",\"creditCardAccount\":{\"id\":\"A0000001\"},\"referenceNumber\":\"CCTxn-001\",\"payee\":\"Vendor-AAA\",\"description\":\"Travel expenses\",\"isInclusiveTax\":true,\"currency\":{\"baseCurrency\":\"USD\",\"txnCurrency\":\"CAD\",\"exchangeRate\":{\"date\":\"2023-12-04\",\"typeId\":\"Intacct Daily Rate\",\"rate\":0.7306}},\"taxSolution\":{\"key\":\"5\",\"id\":\"Canadian Sales Tax - SYS\"},\"attachment\":{\"id\":\"111\",\"key\":\"2\"},\"lines\":[{\"glAccount\":{\"key\":\"326\"},\"totalTxnAmount\":\"500.00\",\"dimensions\":{\"department\":{\"id\":\"PS\"},\"location\":{\"id\":\"7\"},\"project\":{\"id\":\"QSF - BTI\"},\"customer\":{\"id\":\"BTI\"},\"vendor\":{\"id\":\"210\"},\"employee\":{\"id\":\"12\"},\"item\":{\"id\":\"DELL\"},\"class\":{\"id\":\"WSD\"}},\"description\":\"Travel expenses - BTI\",\"taxEntries\":[{\"purchasingTaxDetail\":{\"key\":\"65\"}}],\"isBillable\":false,\"isBilled\":false}]}' |  \\\n  http POST https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn \\\n  Content-Type:application/json",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_wget",
            "name": "Create a credit card transaction",
            "source": "wget --quiet \\\n  --method POST \\\n  --header 'Content-Type: application/json' \\\n  --body-data '{\"txnDate\":\"2025-04-11\",\"creditCardAccount\":{\"id\":\"A0000001\"},\"referenceNumber\":\"CCTxn-001\",\"payee\":\"Vendor-AAA\",\"description\":\"Travel expenses\",\"isInclusiveTax\":true,\"currency\":{\"baseCurrency\":\"USD\",\"txnCurrency\":\"CAD\",\"exchangeRate\":{\"date\":\"2023-12-04\",\"typeId\":\"Intacct Daily Rate\",\"rate\":0.7306}},\"taxSolution\":{\"key\":\"5\",\"id\":\"Canadian Sales Tax - SYS\"},\"attachment\":{\"id\":\"111\",\"key\":\"2\"},\"lines\":[{\"glAccount\":{\"key\":\"326\"},\"totalTxnAmount\":\"500.00\",\"dimensions\":{\"department\":{\"id\":\"PS\"},\"location\":{\"id\":\"7\"},\"project\":{\"id\":\"QSF - BTI\"},\"customer\":{\"id\":\"BTI\"},\"vendor\":{\"id\":\"210\"},\"employee\":{\"id\":\"12\"},\"item\":{\"id\":\"DELL\"},\"class\":{\"id\":\"WSD\"}},\"description\":\"Travel expenses - BTI\",\"taxEntries\":[{\"purchasingTaxDetail\":{\"key\":\"65\"}}],\"isBillable\":false,\"isBilled\":false}]}' \\\n  --output-document \\\n  - https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_xhr",
            "name": "Create a credit card transaction",
            "source": "const data = JSON.stringify({\n  \"txnDate\": \"2025-04-11\",\n  \"creditCardAccount\": {\n    \"id\": \"A0000001\"\n  },\n  \"referenceNumber\": \"CCTxn-001\",\n  \"payee\": \"Vendor-AAA\",\n  \"description\": \"Travel expenses\",\n  \"isInclusiveTax\": true,\n  \"currency\": {\n    \"baseCurrency\": \"USD\",\n    \"txnCurrency\": \"CAD\",\n    \"exchangeRate\": {\n      \"date\": \"2023-12-04\",\n      \"typeId\": \"Intacct Daily Rate\",\n      \"rate\": 0.7306\n    }\n  },\n  \"taxSolution\": {\n    \"key\": \"5\",\n    \"id\": \"Canadian Sales Tax - SYS\"\n  },\n  \"attachment\": {\n    \"id\": \"111\",\n    \"key\": \"2\"\n  },\n  \"lines\": [\n    {\n      \"glAccount\": {\n        \"key\": \"326\"\n      },\n      \"totalTxnAmount\": \"500.00\",\n      \"dimensions\": {\n        \"department\": {\n          \"id\": \"PS\"\n        },\n        \"location\": {\n          \"id\": \"7\"\n        },\n        \"project\": {\n          \"id\": \"QSF - BTI\"\n        },\n        \"customer\": {\n          \"id\": \"BTI\"\n        },\n        \"vendor\": {\n          \"id\": \"210\"\n        },\n        \"employee\": {\n          \"id\": \"12\"\n        },\n        \"item\": {\n          \"id\": \"DELL\"\n        },\n        \"class\": {\n          \"id\": \"WSD\"\n        }\n      },\n      \"description\": \"Travel expenses - BTI\",\n      \"taxEntries\": [\n        {\n          \"purchasingTaxDetail\": {\n            \"key\": \"65\"\n          }\n        }\n      ],\n      \"isBillable\": false,\n      \"isBilled\": false\n    }\n  ]\n});\n\nconst xhr = new XMLHttpRequest();\nxhr.withCredentials = true;\n\nxhr.addEventListener(\"readystatechange\", function () {\n  if (this.readyState === this.DONE) {\n    console.log(this.responseText);\n  }\n});\n\nxhr.open(\"POST\", \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn\");\nxhr.setRequestHeader(\"Content-Type\", \"application/json\");\n\nxhr.send(data);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_jquery",
            "name": "Create a credit card transaction",
            "source": "const settings = {\n  \"async\": true,\n  \"crossDomain\": true,\n  \"url\": \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn\",\n  \"method\": \"POST\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  },\n  \"processData\": false,\n  \"data\": \"{\\\"txnDate\\\":\\\"2025-04-11\\\",\\\"creditCardAccount\\\":{\\\"id\\\":\\\"A0000001\\\"},\\\"referenceNumber\\\":\\\"CCTxn-001\\\",\\\"payee\\\":\\\"Vendor-AAA\\\",\\\"description\\\":\\\"Travel expenses\\\",\\\"isInclusiveTax\\\":true,\\\"currency\\\":{\\\"baseCurrency\\\":\\\"USD\\\",\\\"txnCurrency\\\":\\\"CAD\\\",\\\"exchangeRate\\\":{\\\"date\\\":\\\"2023-12-04\\\",\\\"typeId\\\":\\\"Intacct Daily Rate\\\",\\\"rate\\\":0.7306}},\\\"taxSolution\\\":{\\\"key\\\":\\\"5\\\",\\\"id\\\":\\\"Canadian Sales Tax - SYS\\\"},\\\"attachment\\\":{\\\"id\\\":\\\"111\\\",\\\"key\\\":\\\"2\\\"},\\\"lines\\\":[{\\\"glAccount\\\":{\\\"key\\\":\\\"326\\\"},\\\"totalTxnAmount\\\":\\\"500.00\\\",\\\"dimensions\\\":{\\\"department\\\":{\\\"id\\\":\\\"PS\\\"},\\\"location\\\":{\\\"id\\\":\\\"7\\\"},\\\"project\\\":{\\\"id\\\":\\\"QSF - BTI\\\"},\\\"customer\\\":{\\\"id\\\":\\\"BTI\\\"},\\\"vendor\\\":{\\\"id\\\":\\\"210\\\"},\\\"employee\\\":{\\\"id\\\":\\\"12\\\"},\\\"item\\\":{\\\"id\\\":\\\"DELL\\\"},\\\"class\\\":{\\\"id\\\":\\\"WSD\\\"}},\\\"description\\\":\\\"Travel expenses - BTI\\\",\\\"taxEntries\\\":[{\\\"purchasingTaxDetail\\\":{\\\"key\\\":\\\"65\\\"}}],\\\"isBillable\\\":false,\\\"isBilled\\\":false}]}\"\n};\n\n$.ajax(settings).done(function (response) {\n  console.log(response);\n});",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "node_native",
            "name": "Create a credit card transaction",
            "source": "const http = require(\"https\");\n\nconst options = {\n  \"method\": \"POST\",\n  \"hostname\": \"api.intacct.com\",\n  \"port\": null,\n  \"path\": \"/ia/api/v1/objects/cash-management/credit-card-txn\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  }\n};\n\nconst req = http.request(options, function (res) {\n  const chunks = [];\n\n  res.on(\"data\", function (chunk) {\n    chunks.push(chunk);\n  });\n\n  res.on(\"end\", function () {\n    const body = Buffer.concat(chunks);\n    console.log(body.toString());\n  });\n});\n\nreq.write(JSON.stringify({\n  txnDate: '2025-04-11',\n  creditCardAccount: {id: 'A0000001'},\n  referenceNumber: 'CCTxn-001',\n  payee: 'Vendor-AAA',\n  description: 'Travel expenses',\n  isInclusiveTax: true,\n  currency: {\n    baseCurrency: 'USD',\n    txnCurrency: 'CAD',\n    exchangeRate: {date: '2023-12-04', typeId: 'Intacct Daily Rate', rate: 0.7306}\n  },\n  taxSolution: {key: '5', id: 'Canadian Sales Tax - SYS'},\n  attachment: {id: '111', key: '2'},\n  lines: [\n    {\n      glAccount: {key: '326'},\n      totalTxnAmount: '500.00',\n      dimensions: {\n        department: {id: 'PS'},\n        location: {id: '7'},\n        project: {id: 'QSF - BTI'},\n        customer: {id: 'BTI'},\n        vendor: {id: '210'},\n        employee: {id: '12'},\n        item: {id: 'DELL'},\n        class: {id: 'WSD'}\n      },\n      description: 'Travel expenses - BTI',\n      taxEntries: [{purchasingTaxDetail: {key: '65'}}],\n      isBillable: false,\n      isBilled: false\n    }\n  ]\n}));\nreq.end();",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_httpclient",
            "name": "Create a credit card transaction",
            "source": "var client = new HttpClient();\nvar request = new HttpRequestMessage\n{\n    Method = HttpMethod.Post,\n    RequestUri = new Uri(\"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn\"),\n    Content = new StringContent(\"{\\\"txnDate\\\":\\\"2025-04-11\\\",\\\"creditCardAccount\\\":{\\\"id\\\":\\\"A0000001\\\"},\\\"referenceNumber\\\":\\\"CCTxn-001\\\",\\\"payee\\\":\\\"Vendor-AAA\\\",\\\"description\\\":\\\"Travel expenses\\\",\\\"isInclusiveTax\\\":true,\\\"currency\\\":{\\\"baseCurrency\\\":\\\"USD\\\",\\\"txnCurrency\\\":\\\"CAD\\\",\\\"exchangeRate\\\":{\\\"date\\\":\\\"2023-12-04\\\",\\\"typeId\\\":\\\"Intacct Daily Rate\\\",\\\"rate\\\":0.7306}},\\\"taxSolution\\\":{\\\"key\\\":\\\"5\\\",\\\"id\\\":\\\"Canadian Sales Tax - SYS\\\"},\\\"attachment\\\":{\\\"id\\\":\\\"111\\\",\\\"key\\\":\\\"2\\\"},\\\"lines\\\":[{\\\"glAccount\\\":{\\\"key\\\":\\\"326\\\"},\\\"totalTxnAmount\\\":\\\"500.00\\\",\\\"dimensions\\\":{\\\"department\\\":{\\\"id\\\":\\\"PS\\\"},\\\"location\\\":{\\\"id\\\":\\\"7\\\"},\\\"project\\\":{\\\"id\\\":\\\"QSF - BTI\\\"},\\\"customer\\\":{\\\"id\\\":\\\"BTI\\\"},\\\"vendor\\\":{\\\"id\\\":\\\"210\\\"},\\\"employee\\\":{\\\"id\\\":\\\"12\\\"},\\\"item\\\":{\\\"id\\\":\\\"DELL\\\"},\\\"class\\\":{\\\"id\\\":\\\"WSD\\\"}},\\\"description\\\":\\\"Travel expenses - BTI\\\",\\\"taxEntries\\\":[{\\\"purchasingTaxDetail\\\":{\\\"key\\\":\\\"65\\\"}}],\\\"isBillable\\\":false,\\\"isBilled\\\":false}]}\")\n    {\n        Headers =\n        {\n            ContentType = new MediaTypeHeaderValue(\"application/json\")\n        }\n    }\n};\nusing (var response = await client.SendAsync(request))\n{\n    response.EnsureSuccessStatusCode();\n    var body = await response.Content.ReadAsStringAsync();\n    Console.WriteLine(body);\n}",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_restsharp",
            "name": "Create a credit card transaction",
            "source": "var client = new RestClient(\"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn\");\nvar request = new RestRequest(Method.POST);\nrequest.AddHeader(\"Content-Type\", \"application/json\");\nrequest.AddParameter(\"application/json\", \"{\\\"txnDate\\\":\\\"2025-04-11\\\",\\\"creditCardAccount\\\":{\\\"id\\\":\\\"A0000001\\\"},\\\"referenceNumber\\\":\\\"CCTxn-001\\\",\\\"payee\\\":\\\"Vendor-AAA\\\",\\\"description\\\":\\\"Travel expenses\\\",\\\"isInclusiveTax\\\":true,\\\"currency\\\":{\\\"baseCurrency\\\":\\\"USD\\\",\\\"txnCurrency\\\":\\\"CAD\\\",\\\"exchangeRate\\\":{\\\"date\\\":\\\"2023-12-04\\\",\\\"typeId\\\":\\\"Intacct Daily Rate\\\",\\\"rate\\\":0.7306}},\\\"taxSolution\\\":{\\\"key\\\":\\\"5\\\",\\\"id\\\":\\\"Canadian Sales Tax - SYS\\\"},\\\"attachment\\\":{\\\"id\\\":\\\"111\\\",\\\"key\\\":\\\"2\\\"},\\\"lines\\\":[{\\\"glAccount\\\":{\\\"key\\\":\\\"326\\\"},\\\"totalTxnAmount\\\":\\\"500.00\\\",\\\"dimensions\\\":{\\\"department\\\":{\\\"id\\\":\\\"PS\\\"},\\\"location\\\":{\\\"id\\\":\\\"7\\\"},\\\"project\\\":{\\\"id\\\":\\\"QSF - BTI\\\"},\\\"customer\\\":{\\\"id\\\":\\\"BTI\\\"},\\\"vendor\\\":{\\\"id\\\":\\\"210\\\"},\\\"employee\\\":{\\\"id\\\":\\\"12\\\"},\\\"item\\\":{\\\"id\\\":\\\"DELL\\\"},\\\"class\\\":{\\\"id\\\":\\\"WSD\\\"}},\\\"description\\\":\\\"Travel expenses - BTI\\\",\\\"taxEntries\\\":[{\\\"purchasingTaxDetail\\\":{\\\"key\\\":\\\"65\\\"}}],\\\"isBillable\\\":false,\\\"isBilled\\\":false}]}\", ParameterType.RequestBody);\nIRestResponse response = client.Execute(request);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_python3",
            "name": "Create a credit card transaction",
            "source": "import http.client\n\nconn = http.client.HTTPSConnection(\"api.intacct.com\")\n\npayload = \"{\\\"txnDate\\\":\\\"2025-04-11\\\",\\\"creditCardAccount\\\":{\\\"id\\\":\\\"A0000001\\\"},\\\"referenceNumber\\\":\\\"CCTxn-001\\\",\\\"payee\\\":\\\"Vendor-AAA\\\",\\\"description\\\":\\\"Travel expenses\\\",\\\"isInclusiveTax\\\":true,\\\"currency\\\":{\\\"baseCurrency\\\":\\\"USD\\\",\\\"txnCurrency\\\":\\\"CAD\\\",\\\"exchangeRate\\\":{\\\"date\\\":\\\"2023-12-04\\\",\\\"typeId\\\":\\\"Intacct Daily Rate\\\",\\\"rate\\\":0.7306}},\\\"taxSolution\\\":{\\\"key\\\":\\\"5\\\",\\\"id\\\":\\\"Canadian Sales Tax - SYS\\\"},\\\"attachment\\\":{\\\"id\\\":\\\"111\\\",\\\"key\\\":\\\"2\\\"},\\\"lines\\\":[{\\\"glAccount\\\":{\\\"key\\\":\\\"326\\\"},\\\"totalTxnAmount\\\":\\\"500.00\\\",\\\"dimensions\\\":{\\\"department\\\":{\\\"id\\\":\\\"PS\\\"},\\\"location\\\":{\\\"id\\\":\\\"7\\\"},\\\"project\\\":{\\\"id\\\":\\\"QSF - BTI\\\"},\\\"customer\\\":{\\\"id\\\":\\\"BTI\\\"},\\\"vendor\\\":{\\\"id\\\":\\\"210\\\"},\\\"employee\\\":{\\\"id\\\":\\\"12\\\"},\\\"item\\\":{\\\"id\\\":\\\"DELL\\\"},\\\"class\\\":{\\\"id\\\":\\\"WSD\\\"}},\\\"description\\\":\\\"Travel expenses - BTI\\\",\\\"taxEntries\\\":[{\\\"purchasingTaxDetail\\\":{\\\"key\\\":\\\"65\\\"}}],\\\"isBillable\\\":false,\\\"isBilled\\\":false}]}\"\n\nheaders = { 'Content-Type': \"application/json\" }\n\nconn.request(\"POST\", \"/ia/api/v1/objects/cash-management/credit-card-txn\", payload, headers)\n\nres = conn.getresponse()\ndata = res.read()\n\nprint(data.decode(\"utf-8\"))",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_requests",
            "name": "Create a credit card transaction",
            "source": "import requests\n\nurl = \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-txn\"\n\npayload = {\n    \"txnDate\": \"2025-04-11\",\n    \"creditCardAccount\": {\"id\": \"A0000001\"},\n    \"referenceNumber\": \"CCTxn-001\",\n    \"payee\": \"Vendor-AAA\",\n    \"description\": \"Travel expenses\",\n    \"isInclusiveTax\": True,\n    \"currency\": {\n        \"baseCurrency\": \"USD\",\n        \"txnCurrency\": \"CAD\",\n        \"exchangeRate\": {\n            \"date\": \"2023-12-04\",\n            \"typeId\": \"Intacct Daily Rate\",\n            \"rate\": 0.7306\n        }\n    },\n    \"taxSolution\": {\n        \"key\": \"5\",\n        \"id\": \"Canadian Sales Tax - SYS\"\n    },\n    \"attachment\": {\n        \"id\": \"111\",\n        \"key\": \"2\"\n    },\n    \"lines\": [\n        {\n            \"glAccount\": {\"key\": \"326\"},\n            \"totalTxnAmount\": \"500.00\",\n            \"dimensions\": {\n                \"department\": {\"id\": \"PS\"},\n                \"location\": {\"id\": \"7\"},\n                \"project\": {\"id\": \"QSF - BTI\"},\n                \"customer\": {\"id\": \"BTI\"},\n                \"vendor\": {\"id\": \"210\"},\n                \"employee\": {\"id\": \"12\"},\n                \"item\": {\"id\": \"DELL\"},\n                \"class\": {\"id\": \"WSD\"}\n            },\n            \"description\": \"Travel expenses - BTI\",\n            \"taxEntries\": [{\"purchasingTaxDetail\": {\"key\": \"65\"}}],\n            \"isBillable\": False,\n            \"isBilled\": False\n        }\n    ]\n}\nheaders = {\"Content-Type\": \"application/json\"}\n\nresponse = requests.request(\"POST\", url, json=payload, headers=headers)\n\nprint(response.text)",
            "mimeType": "application/json",
            "isAutoGenerated": true
          }
        ]
      }
    }
  },
  "components": {
    "schemas": {
      "objects.cash-management.credit-card-txn": {
        "type": "object",
        "description": "Transaction record containing charges made to credit and debit cards.",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned unique key for the credit card transaction.",
            "readOnly": true,
            "example": "1001"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the transaction.",
            "readOnly": true,
            "example": "1001"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the transaction.",
            "readOnly": true,
            "example": "/objects/cash-management/credit-card-txn/1001"
          },
          "txnDate": {
            "type": "string",
            "format": "date",
            "example": "2021-11-30",
            "description": "Date of the transaction."
          },
          "creditCardAccount": {
            "type": "object",
            "description": "Credit card account used for this transaction.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the credit card account.",
                "example": "3"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the credit card account.",
                "example": "amex-card-1"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the credit card account.",
                "readOnly": true,
                "example": "/objects/cash-management/credit-card-account/3"
              }
            }
          },
          "referenceNumber": {
            "type": "string",
            "description": "Reference number for the transaction, such as the transaction number from the credit card statement.",
            "example": "Ref--cc12"
          },
          "payee": {
            "type": "string",
            "description": "Name of the payee for the transaction.",
            "example": "Vend-1"
          },
          "description": {
            "type": "string",
            "description": "Brief description of the purpose of the transaction.",
            "example": "Meals"
          },
          "attachment": {
            "type": "object",
            "description": "Supporting document attached to this transaction.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the attachment.",
                "example": "1"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the attachment.",
                "example": "1"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the attachment.",
                "readOnly": true,
                "example": "/objects/company-config/attachment/1"
              }
            }
          },
          "currency": {
            "type": "object",
            "description": "Currency details for the transaction.",
            "properties": {
              "baseCurrency": {
                "type": "string",
                "description": "The base currency for the entity or company.",
                "example": "USD"
              },
              "txnCurrency": {
                "type": "string",
                "description": "For multi-currency companies, the currency in which the transaction was incurred.",
                "example": "GBP"
              },
              "exchangeRate": {
                "type": "object",
                "title": "exchangeRate",
                "description": "Exchange rate used to calculate the base amount for this transaction.",
                "properties": {
                  "date": {
                    "type": "string",
                    "format": "date",
                    "example": "2021-01-23",
                    "description": "Date of the exchange rate used to calculate the base amount from the transaction amount."
                  },
                  "rate": {
                    "type": "number",
                    "description": "Exchange rate used to calculate the base amount from the transaction amount.",
                    "example": 1.0789
                  },
                  "typeId": {
                    "type": "string",
                    "description": "Exchange rate type used to calculate the base amount from the transaction amount.",
                    "example": "Intacct Daily Rate"
                  }
                }
              }
            }
          },
          "totalEntered": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "Total amount entered for the transaction in the company's base currency.",
            "readOnly": true,
            "example": "1500.01"
          },
          "txnTotalEntered": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "For multi-currency companies, the total amount entered in the transaction currency.",
            "readOnly": true,
            "example": "2500.10"
          },
          "totalPaid": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "Total amount paid for the transaction in the company's base currency.",
            "readOnly": true,
            "example": "101.00"
          },
          "txnTotalPaid": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "For multi-currency companies, the total amount paid in the transaction currency.",
            "readOnly": true,
            "example": "200.10"
          },
          "whenPaid": {
            "type": "string",
            "format": "date",
            "readOnly": true,
            "example": "2021-01-23",
            "description": "Date the transaction was paid."
          },
          "state": {
            "type": "string",
            "description": "Current state of the credit card transaction.\nWhen a credit card transaction has been issued and is in a `posted` state, it can be reversed. When a credit card transaction is reversed, the original `posted` transaction immediately enters the `reversal` state and transitions to the `reversed` state once the reversal date is reached.\nA reversal transaction is also created in the `reversal` state when you reverse a credit card transaction. The state of the reversal transaction does not change.",
            "enum": [
              "reversed",
              "reversal",
              "posted",
              "paid",
              "partiallyPaid",
              "selected",
              "noValue"
            ],
            "readOnly": true,
            "example": "posted"
          },
          "reconciliationState": {
            "type": "string",
            "description": "Indicates if this transaction has been cleared or matched for card account reconciliation.",
            "example": "cleared",
            "enum": [
              "cleared",
              "uncleared",
              "matched"
            ],
            "default": "uncleared"
          },
          "clearingDate": {
            "type": "string",
            "format": "date",
            "description": "Date when the transaction was cleared as part of the reconciliation process.",
            "readOnly": true,
            "example": "2024-01-23",
            "nullable": true
          },
          "totalDue": {
            "type": "string",
            "readOnly": true,
            "format": "decimal-precision-2",
            "description": "Total amount due for the transaction in the company's base currency.",
            "example": "1600.00"
          },
          "txnTotalDue": {
            "type": "string",
            "readOnly": true,
            "format": "decimal-precision-2",
            "description": "Total amount due in the transaction currency.",
            "example": "1000.45"
          },
          "totalSelected": {
            "type": "string",
            "readOnly": true,
            "format": "decimal-precision-2",
            "description": "Total amount selected for the transaction in the company's base currency.",
            "example": "1234.67"
          },
          "txnTotalSelected": {
            "type": "string",
            "readOnly": true,
            "format": "decimal-precision-2",
            "description": "Total amount selected in the transaction currency.",
            "example": "1234.56"
          },
          "isInclusiveTax": {
            "type": "boolean",
            "description": "Indicates whether the transaction is tax inclusive.",
            "example": true,
            "default": false
          },
          "transactionSource": {
            "type": "string",
            "description": "Source of the transaction. For transactions auto-created from a bank feed, this value is `bank`, otherwise the value is null.",
            "readOnly": true,
            "enum": [
              null,
              "bank"
            ],
            "nullable": true,
            "default": null,
            "example": "bank"
          },
          "reversedBy": {
            "type": "object",
            "description": "For transactions with a state of `reversed`, a reference to the transaction that reversed this transaction.",
            "readOnly": true,
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the reversing transaction.",
                "readOnly": true,
                "example": "221"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the reversing transaction.",
                "readOnly": true,
                "example": "221"
              },
              "reversalDate": {
                "type": "string",
                "format": "date",
                "readOnly": true,
                "example": "2021-01-23",
                "description": "Date the transaction was reversed."
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the reversing transaction.",
                "readOnly": true,
                "example": "/objects/cash-management/credit-card-txn/221"
              }
            }
          },
          "reversalOf": {
            "type": "object",
            "description": "For transactions with a state of `reversal`, a reference to the transaction that was reversed by this transaction.",
            "readOnly": true,
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the reversed transaction.",
                "readOnly": true,
                "example": "125"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the reversed transaction.",
                "readOnly": true,
                "example": "125"
              },
              "txnDate": {
                "type": "string",
                "format": "date",
                "readOnly": true,
                "example": "2021-10-22",
                "description": "Date of the reversed transaction."
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the reversed transaction.",
                "readOnly": true,
                "example": "/objects/cash-management/credit-card-txn/125"
              }
            }
          },
          "taxSolution": {
            "type": "object",
            "description": "Tax solution used to calculate and capture taxes on this transaction.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the tax solution.",
                "example": "4"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the tax solution.",
                "example": "Australia GST"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the tax solution.",
                "readOnly": true,
                "example": "/objects/tax/tax-solution/4"
              }
            }
          },
          "lines": {
            "type": "array",
            "description": "Line items for the credit card transaction.",
            "items": {
              "$ref": "#/components/schemas/objects.cash-management.credit-card-txn-line"
            }
          },
          "audit": {
            "type": "object",
            "readOnly": true,
            "properties": {
              "createdDateTime": {
                "description": "Date and time when the credit card transaction was created.",
                "type": "string",
                "format": "date-time",
                "readOnly": true,
                "example": "2025-10-01T11:28:12Z"
              },
              "modifiedDateTime": {
                "description": "Date and time when the record was last modified.",
                "type": "string",
                "format": "date-time",
                "readOnly": true,
                "example": "2025-09-14T21:23:42Z"
              },
              "createdBy": {
                "description": "This field has been deprecated. Use the `createdByUser` field instead.",
                "type": "string",
                "readOnly": true,
                "nullable": true,
                "deprecated": true
              },
              "modifiedBy": {
                "description": "This field has been deprecated. Use the `modifiedByUser` field instead.",
                "type": "string",
                "readOnly": true,
                "nullable": true,
                "deprecated": true
              },
              "createdByUser": {
                "type": "object",
                "description": "The user who created the object.",
                "readOnly": true,
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the user.",
                    "readOnly": true,
                    "nullable": true,
                    "example": "436"
                  },
                  "id": {
                    "type": "string",
                    "description": "User login ID.",
                    "readOnly": true,
                    "nullable": true,
                    "example": "JohnDoe"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "description": "URL endpoint for the user.",
                    "example": "/objects/company-config/user/436"
                  }
                }
              },
              "modifiedByUser": {
                "type": "object",
                "description": "The user who last modified this object.",
                "readOnly": true,
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the user.",
                    "readOnly": true,
                    "nullable": true,
                    "example": "3086"
                  },
                  "id": {
                    "type": "string",
                    "description": "User login ID.",
                    "readOnly": true,
                    "nullable": true,
                    "example": "JaneDoe"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "description": "URL endpoint for the user.",
                    "example": "/objects/company-config/user/3086"
                  }
                }
              }
            }
          },
          "entity": {
            "$ref": "#/components/schemas/entity-ref"
          }
        }
      },
      "cash-management-credit-card-txnRequiredProperties": {
        "type": "object",
        "required": [
          "txnDate"
        ],
        "properties": {
          "creditCardAccount": {
            "required": [
              "id"
            ]
          },
          "currency": {
            "required": [
              "txnCurrency"
            ]
          },
          "lines": {
            "type": "array",
            "items": {
              "required": [
                "txnAmount",
                "glAccount"
              ]
            }
          }
        }
      },
      "object-reference": {
        "type": "object",
        "description": "Reference to created or updated object.",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the object.",
            "example": "12345"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the object.",
            "example": "ID123"
          },
          "href": {
            "type": "string",
            "readOnly": true,
            "description": "URL endpoint for the object.",
            "example": "/objects/<application>/<name>/12345"
          }
        }
      },
      "metadata": {
        "description": "Metadata for the response.",
        "type": "object",
        "properties": {
          "totalCount": {
            "type": "integer",
            "description": "Total count.",
            "readOnly": true,
            "example": 3
          },
          "totalSuccess": {
            "type": "integer",
            "description": "Total success.",
            "readOnly": true,
            "example": 2
          },
          "totalError": {
            "type": "integer",
            "description": "Total errors.",
            "readOnly": true,
            "example": 1
          }
        }
      },
      "objects.cash-management.credit-card-txn-line": {
        "type": "object",
        "description": "Line items in a credit card transaction represent charges applied to credit and debit cards.",
        "properties": {
          "id": {
            "type": "string",
            "description": "Unique identifier for the credit card transaction line item.",
            "readOnly": true,
            "example": "62"
          },
          "key": {
            "type": "string",
            "description": "System-assigned unique key for the line item.",
            "readOnly": true,
            "example": "62"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the line item.",
            "readOnly": true,
            "example": "/objects/cash-management/credit-card-txn-line/62"
          },
          "amount": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "Amount for the line item in your company's base currency, which is calculated based on the exchange rate defined in the header.",
            "readOnly": true,
            "example": "3000.54"
          },
          "totalTxnAmount": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "For multi-currency companies, amount of the line item.",
            "example": "100.99"
          },
          "txnAmount": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "For multi-currency companies, the amount of the line item in the transaction currency.",
            "example": "2003.00"
          },
          "currency": {
            "type": "object",
            "description": "Currency details for the credit card transaction.",
            "readOnly": true,
            "properties": {
              "baseCurrency": {
                "type": "string",
                "description": "Base currency for the entity or company.",
                "readOnly": true,
                "example": "USD"
              },
              "txnCurrency": {
                "type": "string",
                "description": "For multi-currency companies, the currency for the line item.",
                "readOnly": true,
                "example": "GBP"
              },
              "exchangeRate": {
                "type": "object",
                "title": "exchangeRate",
                "readOnly": true,
                "description": "Exchange rate used to calculate the base amount for this line item.",
                "properties": {
                  "date": {
                    "type": "string",
                    "format": "date",
                    "example": "2021-01-23",
                    "description": "Date of the exchange rate used to calculate the base amount from the transaction amount.",
                    "readOnly": true
                  },
                  "rate": {
                    "type": "number",
                    "description": "Exchange rate used to calculate the base amount from the transaction amount.",
                    "readOnly": true,
                    "example": 1.0789
                  }
                }
              }
            }
          },
          "description": {
            "type": "string",
            "description": "Additional information about the line item.",
            "example": "Entertainment charges"
          },
          "lineNumber": {
            "type": "integer",
            "description": "Line number for the line item.",
            "readOnly": true,
            "example": 1
          },
          "totalPaid": {
            "type": "string",
            "format": "decimal-precision-2",
            "description": "Total paid for the line item in the company's base currency.",
            "readOnly": true,
            "example": "777.10"
          },
          "txnTotalPaid": {
            "type": "string",
            "readOnly": true,
            "format": "decimal-precision-2",
            "description": "In multi-currency companies, total paid for the line item in the transaction currency.",
            "example": "1000.99"
          },
          "isBillable": {
            "type": "boolean",
            "description": "Indicates whether a line item is billable.",
            "default": false,
            "example": true
          },
          "isBilled": {
            "type": "boolean",
            "description": "Indicates whether a line item is already billed.",
            "default": false,
            "example": false
          },
          "accountLabel": {
            "type": "object",
            "description": "Label for the accounts payable account associated with the line item.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the account label.",
                "example": "14"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the account label.",
                "example": "Entertainment"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the account label.",
                "readOnly": true,
                "example": "/objects/accounts-payable/account-label/14"
              }
            }
          },
          "glAccount": {
            "$ref": "#/components/schemas/gl-account-ref"
          },
          "taxDetail": {
            "type": "object",
            "description": "Specific tax information that applies to the line item.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the tax detail.",
                "example": "5"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the tax detail.",
                "example": "CATAXDETAIL"
              },
              "taxRate": {
                "type": "number",
                "description": "Percentage rate of tax used for the line item.",
                "example": 1.0299
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the tax detail.",
                "readOnly": true,
                "example": "/objects/tax/purchasing-tax-detail/5"
              }
            }
          },
          "dimensions": {
            "type": "object",
            "properties": {
              "location": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the location.",
                    "example": "4",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier of the location.",
                    "example": "AU",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name of the location.",
                    "readOnly": true,
                    "example": "Australia",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the location.",
                    "readOnly": true,
                    "example": "/objects/company-config/location/4"
                  }
                },
                "description": "Standard Sage Intacct dimension that allows you to create a hierarchy of locations to reflect how your company is organized.",
                "title": "location"
              },
              "department": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the department.",
                    "example": "9",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier of the department.",
                    "example": "01",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name of the department.",
                    "readOnly": true,
                    "example": "Accounting",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the department.",
                    "readOnly": true,
                    "example": "/objects/company-config/department/9"
                  }
                },
                "description": "Standard Sage Intacct dimension that allows you to create a hierarchy of departments to reflect how your company is organized.",
                "title": "department"
              },
              "employee": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the employee.",
                    "example": "10",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the employee.",
                    "example": "EMP-10",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the employee.",
                    "readOnly": true,
                    "example": "Thomas, Glenn",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the employee.",
                    "example": "/objects/company-config/employee/10",
                    "readOnly": true
                  }
                }
              },
              "project": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the project.",
                    "example": "2",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the project.",
                    "example": "NET-XML30-2",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the project.",
                    "readOnly": true,
                    "example": "Talcomp training",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the project.",
                    "readOnly": true,
                    "example": "/objects/projects/project/2"
                  }
                }
              },
              "customer": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the customer.",
                    "example": "13",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the customer.",
                    "example": "CUST-13",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the customer.",
                    "readOnly": true,
                    "example": "Jack In the Box",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the customer.",
                    "readOnly": true,
                    "example": "/objects/accounts-receivable/customer/13"
                  }
                }
              },
              "vendor": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the vendor.",
                    "example": "357",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the vendor.",
                    "example": "1605212096809",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the vendor.",
                    "readOnly": true,
                    "example": "GenLab",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the vendor.",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/vendor/357"
                  }
                }
              },
              "item": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the item.",
                    "example": "13",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the item.",
                    "example": "Case 13",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the item.",
                    "readOnly": true,
                    "example": "Platform pack",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the item.",
                    "readOnly": true,
                    "example": "/objects/inventory-control/item/13"
                  }
                }
              },
              "warehouse": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the warehouse.",
                    "example": "6",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the warehouse.",
                    "example": "WH01",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the warehouse.",
                    "readOnly": true,
                    "example": "WH01",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the warehouse.",
                    "readOnly": true,
                    "example": "/objects/inventory-control/warehouse/6"
                  }
                }
              },
              "class": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the class.",
                    "example": "731",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the class.",
                    "example": "REST_CLS_001",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the class.",
                    "readOnly": true,
                    "example": "Enterprises",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the class.",
                    "readOnly": true,
                    "example": "/objects/company-config/class/731"
                  }
                }
              },
              "task": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the task.",
                    "example": "1",
                    "nullable": true
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the task.",
                    "example": "1",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the task.",
                    "readOnly": true,
                    "example": "Project Task",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the task.",
                    "readOnly": true,
                    "example": "/objects/projects/task/1"
                  }
                }
              },
              "costType": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the cost type.",
                    "example": "2",
                    "nullable": true
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the cost type.",
                    "example": "2",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the cost type.",
                    "readOnly": true,
                    "example": "Project Expense",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the cost type.",
                    "readOnly": true,
                    "example": "/objects/construction/cost-type/2"
                  }
                }
              },
              "asset": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the asset.",
                    "example": "A001",
                    "nullable": true
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the asset.",
                    "example": "1",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the asset.",
                    "readOnly": true,
                    "example": "Laptop",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the asset.",
                    "readOnly": true,
                    "example": "/objects/fixed-assets/asset/1"
                  }
                }
              },
              "contract": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the contract.",
                    "example": "CON-0045-1",
                    "nullable": true
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the contract.",
                    "example": "12",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the contract.",
                    "readOnly": true,
                    "example": "ACME Widgets - Service",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the contract.",
                    "readOnly": true,
                    "example": "/objects/contracts/contract/12"
                  }
                }
              },
              "affiliateEntity": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the affiliate entity.",
                    "example": "23",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the affiliate entity.",
                    "example": "AFF-23",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the affiliate entity.",
                    "readOnly": true,
                    "example": "/objects/affiliate-entity/23"
                  },
                  "name": {
                    "type": "string",
                    "readOnly": true,
                    "description": "Name for the affiliate entity.",
                    "example": "100-USA",
                    "nullable": true
                  }
                }
              },
              "loanAccount": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the loan account.",
                    "example": "LN001",
                    "nullable": true
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the loan account.",
                    "example": "852",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the loan account.",
                    "readOnly": true,
                    "example": "Business Loan",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the loan account.",
                    "readOnly": true,
                    "example": "/objects/loan-management/loan-account/852"
                  }
                }
              },
              "workOrder": {
                "description": "Work order associated with the dimension.",
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the work order.",
                    "example": "18",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the work order.",
                    "example": "WO-0017",
                    "nullable": true
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the work order.",
                    "readOnly": true,
                    "example": "WO-India",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the work order.",
                    "readOnly": true,
                    "example": "/objects/construction/work-order/18"
                  }
                }
              }
            }
          },
          "baseLocation": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "Base location key.",
                "example": "4"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the location.",
                "example": "US"
              },
              "name": {
                "type": "string",
                "description": "Name for the location.",
                "readOnly": true,
                "example": "United States of America"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the location.",
                "example": "/objects/company-config/location/1",
                "readOnly": true
              }
            },
            "description": "Base location for the line item in multi-entity companies."
          },
          "creditCardTxn": {
            "type": "object",
            "description": "Credit card transaction that contains the line items.",
            "readOnly": true,
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the credit card transaction.",
                "readOnly": true,
                "example": "100"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the credit card transaction.",
                "readOnly": true,
                "example": "100"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the credit card transaction.",
                "readOnly": true,
                "example": "/objects/cash-management/credit-card-txn/100"
              }
            }
          },
          "audit": {
            "$ref": "#/components/schemas/audit.s1",
            "readOnly": true
          },
          "taxEntries": {
            "type": "array",
            "description": "Tax Entries of the credit card transaction line item.",
            "items": {
              "$ref": "#/components/schemas/objects.cash-management.credit-card-txn-tax-entry"
            }
          }
        }
      },
      "audit.s1": {
        "type": "object",
        "readOnly": true,
        "properties": {
          "createdDateTime": {
            "description": "Date and time when the record was created.",
            "type": "string",
            "format": "date-time",
            "readOnly": true,
            "example": "2025-05-16T15:34:35Z"
          },
          "modifiedDateTime": {
            "description": "Date and time when the record was last modified.",
            "type": "string",
            "format": "date-time",
            "readOnly": true,
            "example": "2025-09-14T21:23:42Z"
          },
          "createdBy": {
            "description": "This field has been deprecated. Use the `createdByUser` field instead.",
            "type": "string",
            "readOnly": true,
            "nullable": true,
            "deprecated": true
          },
          "modifiedBy": {
            "description": "This field has been deprecated. Use the `modifiedByUser` field instead.",
            "type": "string",
            "readOnly": true,
            "nullable": true,
            "deprecated": true
          },
          "createdByUser": {
            "type": "object",
            "description": "The user who created the object.",
            "readOnly": true,
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the user.",
                "readOnly": true,
                "nullable": true,
                "example": "436"
              },
              "id": {
                "type": "string",
                "description": "User login ID.",
                "readOnly": true,
                "nullable": true,
                "example": "JohnDoe"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "description": "URL endpoint for the user.",
                "example": "/objects/company-config/user/436"
              }
            }
          },
          "modifiedByUser": {
            "type": "object",
            "description": "The user who last modified this object.",
            "readOnly": true,
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the user.",
                "readOnly": true,
                "nullable": true,
                "example": "3086"
              },
              "id": {
                "type": "string",
                "description": "User login ID.",
                "readOnly": true,
                "nullable": true,
                "example": "JaneDoe"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "description": "URL endpoint for the user.",
                "example": "/objects/company-config/user/3086"
              }
            }
          }
        }
      },
      "entity-ref": {
        "type": "object",
        "description": "The entity that the object is associated with. Objects created at the top level do not have an entity reference so the `key`, `id`, and `name` properties will be `null`.",
        "readOnly": true,
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the entity.",
            "readOnly": true,
            "nullable": true,
            "example": "46"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the entity.",
            "readOnly": true,
            "nullable": true,
            "example": "CORP"
          },
          "name": {
            "type": "string",
            "description": "Name for the entity.",
            "readOnly": true,
            "nullable": true,
            "example": "Corp"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the entity.",
            "readOnly": true,
            "example": "/objects/company-config/entity/46"
          }
        }
      },
      "gl-account-ref": {
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the GL account.",
            "example": "144"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the GL account.",
            "example": "1112"
          },
          "name": {
            "type": "string",
            "description": "Name for the GL account.",
            "readOnly": true,
            "example": "Employee Advances"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the GL account.",
            "readOnly": true,
            "example": "/objects/general-ledger/account/144"
          }
        }
      },
      "dimension-ref": {
        "type": "object",
        "properties": {
          "location": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the location.",
                "example": "22",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the location.",
                "example": "LOC-22",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the location.",
                "readOnly": true,
                "example": "California",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the location.",
                "readOnly": true,
                "example": "/objects/company-config/location/22"
              }
            }
          },
          "department": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the department.",
                "example": "11",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the department.",
                "example": "DEP-11",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the department.",
                "readOnly": true,
                "example": "Sales and Marketing",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the department.",
                "readOnly": true,
                "example": "/objects/company-config/department/11"
              }
            }
          },
          "employee": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the employee.",
                "example": "10",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the employee.",
                "example": "EMP-10",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the employee.",
                "readOnly": true,
                "example": "Thomas, Glenn",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the employee.",
                "example": "/objects/company-config/employee/10",
                "readOnly": true
              }
            }
          },
          "project": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the project.",
                "example": "2",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the project.",
                "example": "NET-XML30-2",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the project.",
                "readOnly": true,
                "example": "Talcomp training",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the project.",
                "readOnly": true,
                "example": "/objects/projects/project/2"
              }
            }
          },
          "customer": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the customer.",
                "example": "13",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the customer.",
                "example": "CUST-13",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the customer.",
                "readOnly": true,
                "example": "Jack In the Box",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the customer.",
                "readOnly": true,
                "example": "/objects/accounts-receivable/customer/13"
              }
            }
          },
          "vendor": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the vendor.",
                "example": "357",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the vendor.",
                "example": "1605212096809",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the vendor.",
                "readOnly": true,
                "example": "GenLab",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the vendor.",
                "readOnly": true,
                "example": "/objects/accounts-payable/vendor/357"
              }
            }
          },
          "item": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the item.",
                "example": "13",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the item.",
                "example": "Case 13",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the item.",
                "readOnly": true,
                "example": "Platform pack",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the item.",
                "readOnly": true,
                "example": "/objects/inventory-control/item/13"
              }
            }
          },
          "warehouse": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the warehouse.",
                "example": "6",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the warehouse.",
                "example": "WH01",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the warehouse.",
                "readOnly": true,
                "example": "WH01",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the warehouse.",
                "readOnly": true,
                "example": "/objects/inventory-control/warehouse/6"
              }
            }
          },
          "class": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the class.",
                "example": "731",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the class.",
                "example": "REST_CLS_001",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the class.",
                "readOnly": true,
                "example": "Enterprises",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the class.",
                "readOnly": true,
                "example": "/objects/company-config/class/731"
              }
            }
          },
          "task": {
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the task.",
                "example": "1",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "System-assigned key for the task.",
                "example": "1",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the task.",
                "readOnly": true,
                "example": "Project Task",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the task.",
                "readOnly": true,
                "example": "/objects/projects/task/1"
              }
            }
          },
          "costType": {
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the cost type.",
                "example": "2",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "System-assigned key for the cost type.",
                "example": "2",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the cost type.",
                "readOnly": true,
                "example": "Project Expense",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the cost type.",
                "readOnly": true,
                "example": "/objects/construction/cost-type/2"
              }
            }
          },
          "asset": {
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the asset.",
                "example": "A001",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "System-assigned key for the asset.",
                "example": "1",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the asset.",
                "readOnly": true,
                "example": "Laptop",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the asset.",
                "readOnly": true,
                "example": "/objects/fixed-assets/asset/1"
              }
            }
          },
          "contract": {
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the contract.",
                "example": "CON-0045-1",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "System-assigned key for the contract.",
                "example": "12",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the contract.",
                "readOnly": true,
                "example": "ACME Widgets - Service",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the contract.",
                "readOnly": true,
                "example": "/objects/contracts/contract/12"
              }
            }
          },
          "affiliateEntity": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the affiliate entity.",
                "example": "23",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the affiliate entity.",
                "example": "AFF-23",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the affiliate entity.",
                "readOnly": true,
                "example": "/objects/affiliate-entity/23"
              },
              "name": {
                "type": "string",
                "readOnly": true,
                "description": "Name for the affiliate entity.",
                "example": "100-USA",
                "nullable": true
              }
            }
          },
          "loanAccount": {
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the loan account.",
                "example": "LN001",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "System-assigned key for the loan account.",
                "example": "852",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the loan account.",
                "readOnly": true,
                "example": "Business Loan",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the loan account.",
                "readOnly": true,
                "example": "/objects/loan-management/loan-account/852"
              }
            }
          },
          "workOrder": {
            "description": "Work order associated with the dimension.",
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the work order.",
                "example": "18",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the work order.",
                "example": "WO-0017",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the work order.",
                "readOnly": true,
                "example": "WO-India",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the work order.",
                "readOnly": true,
                "example": "/objects/construction/work-order/18"
              }
            }
          }
        }
      },
      "location-ref": {
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the location.",
            "example": "1"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the location.",
            "example": "US"
          },
          "name": {
            "type": "string",
            "description": "Name for the location.",
            "readOnly": true,
            "example": "United States of America"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the location.",
            "example": "/objects/company-config/location/1",
            "readOnly": true
          }
        }
      },
      "objects.cash-management.credit-card-txn-tax-entry": {
        "title": "Tax entries",
        "description": "Tax entry details for credit card transaction line items.",
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the tax entry.",
            "example": "7149",
            "readOnly": true
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the tax entry.",
            "example": "7149",
            "readOnly": true
          },
          "baseTaxAmount": {
            "type": "string",
            "description": "Base tax amount.",
            "format": "decimal-precision-2",
            "example": "100.00"
          },
          "txnTaxAmount": {
            "type": "string",
            "description": "Transaction tax amount. For a PATCH request, set to `null` if you want Sage Intacct to recalculate the amount, or set to the value you want if you don't want the system to recalculate.",
            "format": "decimal-precision-2",
            "example": "100.00"
          },
          "taxRate": {
            "type": "number",
            "description": "Tax rate.",
            "example": 1.0299
          },
          "purchasingTaxDetail": {
            "type": "object",
            "description": "Specifies tax information for individual line items in credit card transactions, including entries relating to accounts receivable, accounts payable and purchasing.\n",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned unique key for the purchasing tax detail.",
                "example": "1"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier of the purchasing tax detail.",
                "example": "Alaska Tax Detail"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint of the purchasing tax detail.",
                "readOnly": true,
                "example": "/objects/tax/purchasing-tax-detail/1"
              }
            }
          },
          "creditCardTxnLine": {
            "title": "creditCardTxnLine",
            "description": "Credit card transaction line item to which this tax information applies.",
            "type": "object",
            "readOnly": true,
            "properties": {
              "id": {
                "type": "string",
                "description": "Unique identifier for the credit card transaction line item.",
                "example": "100",
                "readOnly": true
              },
              "key": {
                "type": "string",
                "description": "Unique key for the credit card transaction line item.",
                "example": "100",
                "readOnly": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the credit card transaction line item.",
                "readOnly": true,
                "example": "/objects/cash-management/credit-card-txn-line/100"
              }
            }
          }
        }
      },
      "error-response": {
        "type": "object",
        "description": "Error response",
        "properties": {
          "ia::result": {
            "type": "object",
            "properties": {
              "ia::error": {
                "type": "object",
                "properties": {
                  "code": {
                    "type": "string",
                    "example": "invalidRequest"
                  },
                  "message": {
                    "type": "string",
                    "example": "Payload contains errors"
                  },
                  "supportId": {
                    "type": "string",
                    "example": "sQrM9%7EYdh5oDEWVb80mrn9xuHjoAAAABBQ"
                  },
                  "errorId": {
                    "type": "string",
                    "example": "REST-1064"
                  },
                  "additionalInfo": {
                    "type": "object",
                    "properties": {
                      "messageId": {
                        "type": "string",
                        "example": "IA.PAYLOAD_CONTAINS_ERRORS"
                      },
                      "placeholders": {
                        "type": "object",
                        "example": {}
                      },
                      "propertySet": {
                        "type": "object",
                        "example": {}
                      }
                    }
                  },
                  "details": {
                    "type": "array",
                    "items": {
                      "type": "object",
                      "properties": {
                        "code": {
                          "type": "string",
                          "example": "invalidRequest"
                        },
                        "message": {
                          "type": "string",
                          "example": "/newDate is not a valid field"
                        },
                        "errorId": {
                          "type": "string",
                          "example": "REST-1043"
                        },
                        "target": {
                          "type": "string",
                          "example": "/newDate"
                        },
                        "additionalInfo": {
                          "type": "object",
                          "properties": {
                            "messageId": {
                              "type": "string",
                              "example": "IA.NOT_A_VALID_FIELD"
                            },
                            "placeholders": {
                              "type": "object",
                              "example": {}
                            },
                            "propertySet": {
                              "type": "object",
                              "example": {}
                            }
                          }
                        }
                      }
                    }
                  }
                }
              },
              "ia::meta": {
                "$ref": "#/components/schemas/metadata"
              }
            }
          }
        },
        "example": {
          "ia::result": {
            "ia::error": {
              "code": "invalidRequest",
              "message": "A POST request requires a payload",
              "errorId": "REST-1028",
              "additionalInfo": {
                "messageId": "IA.REQUEST_REQUIRES_A_PAYLOAD",
                "placeholders": {
                  "OPERATION": "POST"
                },
                "propertySet": {}
              },
              "supportId": "Kxi78%7EZuyXBDEGVHD2UmO1phYXDQAAAAo"
            }
          },
          "ia::meta": {
            "totalCount": 1,
            "totalSuccess": 0,
            "totalError": 1
          }
        }
      }
    },
    "responses": {
      "400error": {
        "description": "Bad Request",
        "content": {
          "application/json": {
            "schema": {
              "$ref": "#/components/schemas/error-response"
            },
            "examples": {
              "Response example": {
                "value": {
                  "ia::result": {
                    "ia::error": {
                      "code": "invalidRequest",
                      "message": "A POST request requires a payload",
                      "errorId": "REST-1028",
                      "additionalInfo": {
                        "messageId": "IA.REQUEST_REQUIRES_A_PAYLOAD",
                        "placeholders": {
                          "OPERATION": "POST"
                        },
                        "propertySet": {}
                      },
                      "supportId": "Kxi78%7EZuyXBDEGVHD2UmO1phYXDQAAAAo"
                    }
                  },
                  "ia::meta": {
                    "totalCount": 1,
                    "totalSuccess": 0,
                    "totalError": 1
                  }
                }
              }
            }
          }
        }
      }
    },
    "securitySchemes": {
      "OAuth2": {
        "description": "Sage Intacct OAuth 2.0 authorization code flow",
        "type": "oauth2",
        "flows": {
          "authorizationCode": {
            "authorizationUrl": "https://api.intacct.com/ia/api/v1/oauth2/authorize",
            "tokenUrl": "https://api.intacct.com/ia/api/v1/oauth2/token",
            "refreshUrl": "https://api.intacct.com/ia/api/v1/oauth2/token",
            "scopes": {}
          }
        }
      }
    }
  }
}
```
