```json
{
  "openapi": "3.0.3",
  "info": {
    "title": "Create a savings account",
    "version": "1",
    "description": "Create a new savings account."
  },
  "servers": [
    {
      "url": "https://api.intacct.com/ia/api/v1",
      "x-try-it": "sage-intacct-api"
    }
  ],
  "paths": {
    "/objects/cash-management/savings-account": {
      "post": {
        "summary": "Create a savings account",
        "description": "Create a new savings account.",
        "tags": [
          "Cash_Management_Savings accounts"
        ],
        "operationId": "post-objects-savings-account",
        "requestBody": {
          "description": "",
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the savings account.",
                    "readOnly": true,
                    "example": "34"
                  },
                  "id": {
                    "type": "string",
                    "description": "Name or other unique identifier for the savings account. The account ID cannot be modified.",
                    "example": "SBI"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL for the savings account.",
                    "readOnly": true,
                    "example": "/objects/cash-management/savings-account/34"
                  },
                  "bankAccountDetails": {
                    "type": "object",
                    "description": "Bank account details",
                    "properties": {
                      "accountNumber": {
                        "type": "string",
                        "description": "Bank account number for this savings account.",
                        "example": "4356789400"
                      },
                      "bankName": {
                        "type": "string",
                        "description": "Name of the bank for this savings account.",
                        "example": "Bank of the West"
                      },
                      "routingNumber": {
                        "type": "string",
                        "description": "Routing number for this savings account.",
                        "example": "123456791"
                      },
                      "branchId": {
                        "type": "string",
                        "description": "Bank branch ID for this savings account.",
                        "example": "123456791"
                      },
                      "phoneNumber": {
                        "type": "string",
                        "description": "Phone number of the bank branch.",
                        "example": "555-927-6200"
                      },
                      "currency": {
                        "type": "string",
                        "description": "The currency for this account. The default is the base currency for the company or entity. If this account is with a foreign bank, the currency should match the country.",
                        "example": "USD"
                      },
                      "bankAddress": {
                        "type": "object",
                        "properties": {
                          "city": {
                            "type": "string",
                            "description": "City where the bank is located.",
                            "example": "Fremont"
                          },
                          "state": {
                            "type": "string",
                            "description": "State where the bank is located.",
                            "example": "CA"
                          },
                          "postCode": {
                            "type": "string",
                            "description": "Zip or postal code for the bank.",
                            "example": "94536"
                          },
                          "country": {
                            "type": "string",
                            "description": "Country where the bank is located.",
                            "example": "United States"
                          },
                          "addressLine1": {
                            "type": "string",
                            "description": "Line 1 of the street address for the bank.",
                            "example": "39148 Paseo Padre Pkwy"
                          },
                          "addressLine2": {
                            "type": "string",
                            "description": "Line 2 of the street address for the bank.",
                            "example": "Suite 104"
                          },
                          "addressLine3": {
                            "type": "string",
                            "description": "Line 3 of the street address for the bank which provides additional geographical information.",
                            "example": "Western industrial area"
                          }
                        }
                      }
                    },
                    "required": [
                      "bankName",
                      "currency"
                    ]
                  },
                  "accounting": {
                    "type": "object",
                    "description": "Accounting information",
                    "properties": {
                      "glAccount": {
                        "type": "object",
                        "description": "General Ledger account that this savings account is associated with.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "example": "90"
                          },
                          "id": {
                            "type": "string",
                            "example": "1047--Savings a/c France"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/31"
                          }
                        }
                      },
                      "apJournal": {
                        "type": "object",
                        "description": "Default payables GL journal",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the gl-journal.",
                            "example": "3"
                          },
                          "id": {
                            "type": "string",
                            "description": "The id of the journal",
                            "example": "AP ADJ--AP Adjustment Journal"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL for this journal.",
                            "example": "/objects/general-ledger/journal/3",
                            "readOnly": true
                          }
                        }
                      },
                      "arJournal": {
                        "type": "object",
                        "description": "Default receivables GL journal",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the gl-journal.",
                            "example": "3"
                          },
                          "id": {
                            "type": "string",
                            "description": "The id of the journal",
                            "example": "AR ADJ--AR Adjustment Journal"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL for this journal.",
                            "example": "/objects/general-ledger/journal/3",
                            "readOnly": true
                          }
                        }
                      },
                      "bankingTimeZone": {
                        "type": "string",
                        "description": "Time zone",
                        "nullable": true,
                        "example": "GMT+02:00 Eastern Europe Time",
                        "enum": [
                          null,
                          "GMT (Greenwich Mean Time) Dublin, Edinburgh, London",
                          "GMT+00:00 Western Europe Time",
                          "GMT+01:00 Western Europe Summer Time",
                          "GMT+01:00 British Summer Time",
                          "GMT+01:00 Irish Summer Time",
                          "GMT+01:00 Central Europe Time",
                          "GMT+01:00 Berlin, Stockholm, Rome, Bern, Brussels",
                          "GMT+01:00 Lisbon, Warsaw",
                          "GMT+01:00 Paris, Madrid",
                          "GMT+01:00 Prague",
                          "GMT+02:00 Central Europe Summer Time",
                          "GMT+02:00 Eastern Europe Time",
                          "GMT+02:00 Athens, Helsinki, Istanbul",
                          "GMT+02:00 Cairo",
                          "GMT+02:00 Harare, Pretoria",
                          "GMT+02:00 Israel",
                          "GMT+03:00 Eastern Europe Summer Time",
                          "GMT+03:00 Baghdad, Kuwait, Nairobi, Riyadh",
                          "GMT+03:00 Moscow, St. Petersburg, Volgograd",
                          "GMT+03:30 Tehran",
                          "GMT+04:00 Moscow Summer Time",
                          "GMT+04:00 Abu Dhabi, Muscat, Tbilisi, Kazan",
                          "GMT+04:30 Kabul",
                          "GMT+05:00 Islamabad, Karachi, Sverdlovsk, Tashkent",
                          "GMT+05:30 Bombay, Calcutta, Madras, New Delhi",
                          "GMT+06:00 Almaty, Dhaka",
                          "GMT+07:00 Bangkok, Jakarta, Hanoi",
                          "GMT+08:00 Beijing, Chongqing, Urumqi",
                          "GMT+08:00 Hong Kong SAR, Perth, Singapore, Taipei",
                          "GMT+08:00 (Australian) Western Standard Time",
                          "GMT+09:00 Tokyo, Osaka, Sapporo, Seoul, Yakutsk",
                          "GMT+09:30 (Australian) Central Standard Time",
                          "GMT+10:30 (Australian) Central Daylight Time",
                          "GMT+09:30 Adelaide",
                          "GMT+09:30 Darwin",
                          "GMT+10:00 Brisbane, Melbourne, Sydney",
                          "GMT+10:00 Guam, Port Moresby",
                          "GMT+10:00 Vladivostok",
                          "GMT+10:00 (Australian) Eastern Standard Time",
                          "GMT+11:00  (Australian) Eastern Daylight Time",
                          "GMT+12:00 Fiji Islands, Marshall Islands",
                          "GMT+12:00 Kamchatka",
                          "GMT+12:00 Magadan, Solomon Islands, New Caledonia",
                          "GMT+12:00 Wellington, Auckland",
                          "GMT+13:00 Nuku`alofa",
                          "GMT+13:00 Samoa",
                          "GMT-01:00 Azores, Cape Verde Island",
                          "GMT-03:00 Brasilia",
                          "GMT-03:00 Buenos Aires, Georgetown",
                          "GMT-03:30 Newfoundland Standard Time",
                          "GMT-02:30 Newfoundland Daylight Time",
                          "GMT-04:00 Atlantic Standard Time",
                          "GMT-03:00 Atlantic Daylight Time",
                          "GMT-04:00 Caracas, La Paz",
                          "GMT-05:00 Bogota, Lima",
                          "GMT-05:00 Eastern Standard Time",
                          "GMT-04:00 Eastern Daylight Saving Time",
                          "GMT-05:00 Indiana (East)",
                          "GMT-06:00 Central Standard Time",
                          "GMT-05:00 Central Daylight Saving Time",
                          "GMT-06:00 Mexico City, Tegucigalpa",
                          "GMT-06:00 Saskatchewan",
                          "GMT-07:00 Arizona",
                          "GMT-07:00 Mountain Standard Time",
                          "GMT-06:00 Mountain Daylight Saving Time",
                          "GMT-08:00 Pacific Standard Time",
                          "GMT-07:00 Pacific Daylight Saving Time",
                          "GMT-09:00 Alaska Standard Time",
                          "GMT-08:00 Alaska Standard Daylight Saving Time",
                          "GMT-10:00 Hawaii",
                          "GMT-11:00 Midway Island, Samoa",
                          "GMT-12:00 Eniwetok, Kwajalein"
                        ],
                        "default": null
                      },
                      "disableInterEntityTransfer": {
                        "type": "boolean",
                        "description": "Exclude this account from inter-entity transfers (IET) even if IET is globally enabled for the entire multi-entity shared structure of companies.",
                        "default": false,
                        "example": false
                      },
                      "serviceChargeGLAccount": {
                        "type": "object",
                        "description": "General ledger account for service charges. Used for reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "example": "13"
                          },
                          "id": {
                            "type": "string",
                            "example": "1004--Lloyds bank"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/13"
                          }
                        }
                      },
                      "serviceChargeAccountLabel": {
                        "type": "object",
                        "description": "General ledger account label for service charges.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "example": "8"
                          },
                          "id": {
                            "type": "string",
                            "example": "Accounting Fees"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "example": "/objects/accounts-payable/account-label/8"
                          }
                        }
                      },
                      "interestGLAccount": {
                        "type": "object",
                        "description": "General ledger account for earned interest. Used for reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "example": "15"
                          },
                          "id": {
                            "type": "string",
                            "example": "1006--Banorte Bank"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/15"
                          }
                        }
                      },
                      "interestAccountLabel": {
                        "type": "object",
                        "description": "General ledger account label for earned interest.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "example": "35"
                          },
                          "id": {
                            "type": "string",
                            "example": "Interest Fees"
                          },
                          "href": {
                            "type": "string",
                            "readOnly": true,
                            "example": "/objects/accounts-receivable/account-label/35"
                          }
                        }
                      }
                    },
                    "required": [
                      "glAccount"
                    ]
                  },
                  "reconciliation": {
                    "type": "object",
                    "description": "Reconciliation information",
                    "properties": {
                      "lastReconciledBalance": {
                        "type": "string",
                        "description": "Last reconciled balance.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "600.00"
                      },
                      "lastReconciledDate": {
                        "type": "string",
                        "format": "date",
                        "description": "Date of the last reconciliation.",
                        "readOnly": true,
                        "example": "2022-02-28"
                      },
                      "cutOffDate": {
                        "type": "string",
                        "format": "date",
                        "description": "The date after which initial reconciliation can begin.",
                        "readOnly": true,
                        "example": "2022-02-28"
                      },
                      "inProgressBalance": {
                        "type": "string",
                        "description": "In progress reconciliation balance.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "200.00"
                      },
                      "inProgressDate": {
                        "type": "string",
                        "format": "date",
                        "description": "In progress reconciliation date.",
                        "readOnly": true,
                        "example": "2022-01-28"
                      },
                      "matchSequence": {
                        "type": "object",
                        "description": "Reconciliation match sequence",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the document sequence number.",
                            "example": "2"
                          },
                          "id": {
                            "type": "string",
                            "description": "Document sequence ID",
                            "example": "2--Bank sequence Id"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL for the sequence number.",
                            "example": "/objects/company-config/document-sequence/2",
                            "readOnly": true
                          }
                        }
                      },
                      "useMatchSequenceForAutoMatch": {
                        "type": "boolean",
                        "default": true,
                        "description": "Use sequence number for automatically matched transactions.",
                        "example": false
                      },
                      "useMatchSequenceForManualMatch": {
                        "type": "boolean",
                        "default": true,
                        "description": "Use sequence number for manually matched transactions.",
                        "example": false
                      }
                    }
                  },
                  "department": {
                    "type": "object",
                    "description": "department",
                    "properties": {
                      "key": {
                        "type": "string",
                        "example": "8"
                      },
                      "id": {
                        "type": "string",
                        "example": "8--Finance"
                      },
                      "href": {
                        "type": "string",
                        "readOnly": true,
                        "example": "/objects/company-config/department/8"
                      }
                    }
                  },
                  "location": {
                    "type": "object",
                    "description": "location",
                    "properties": {
                      "key": {
                        "type": "string",
                        "example": "4"
                      },
                      "id": {
                        "type": "string",
                        "example": "4--Australia"
                      },
                      "href": {
                        "type": "string",
                        "readOnly": true,
                        "example": "/objects/company-config/location/4"
                      }
                    }
                  },
                  "status": {
                    "$ref": "#/components/schemas/status"
                  },
                  "audit": {
                    "$ref": "#/components/schemas/audit.s1"
                  },
                  "entity": {
                    "$ref": "#/components/schemas/entity-ref"
                  },
                  "bankingCloudConnection": {
                    "$ref": "#/components/schemas/banking-cloud-connection"
                  },
                  "financialInstitution": {
                    "description": "Financial institution reference",
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string",
                        "readOnly": true,
                        "example": "FinOne"
                      },
                      "key": {
                        "type": "string",
                        "readOnly": true,
                        "example": "1"
                      },
                      "href": {
                        "type": "string",
                        "readOnly": true,
                        "example": "/objects/cash-management/financial-institution/1"
                      }
                    },
                    "readOnly": true
                  },
                  "ruleSet": {
                    "type": "object",
                    "description": "Applied rule set",
                    "properties": {
                      "key": {
                        "type": "string",
                        "example": "1"
                      },
                      "id": {
                        "type": "string",
                        "example": "36--RuleSetToMatch"
                      },
                      "href": {
                        "type": "string",
                        "readOnly": true,
                        "example": "/objects/cash-management/bank-txn-rule-set/1"
                      }
                    }
                  },
                  "restrictions": {
                    "type": "object",
                    "description": "Restrict a bank account to a specific location or restrict one or more entity/locations to a specific account.",
                    "properties": {
                      "restrictionType": {
                        "type": "string",
                        "description": "Set which entities/locations within the company can access and use this checking account.\n\n**Valid values**\n- `unrestricted` - (default) This account is available to the top-level company and all entity-level locations.\n- `rootOnly` - Only the top-level company of a multi-entity structure can access this account.\n- `restricted` - Only specified locations, location groups, departments, or department groups can access this account.\n",
                        "enum": [
                          "unrestricted",
                          "rootOnly",
                          "restricted"
                        ],
                        "example": "unrestricted",
                        "default": "unrestricted"
                      },
                      "locations": {
                        "type": "array",
                        "description": "List of locations that can access this checking account when `restrictionType` is set to `restricted`.",
                        "items": {
                          "type": "string"
                        },
                        "example": [
                          "1--United States of America",
                          "2--United Kingdom"
                        ]
                      }
                    }
                  }
                },
                "required": [
                  "id",
                  "location"
                ]
              },
              "examples": {
                "Create a savings account": {
                  "value": {
                    "id": "SAVINGS455_6780",
                    "bankAccountDetails": {
                      "accountNumber": "",
                      "bankName": "Savings Bank of America New",
                      "routingNumber": "565676545",
                      "branchId": "123456791",
                      "phoneNumber": "6509876545",
                      "bankAddress": {
                        "addressLine1": "73466 Linkln St",
                        "addressLine2": null,
                        "addressLine3": null,
                        "city": "Montaine View",
                        "country": "United States",
                        "postCode": "67898",
                        "state": "CA"
                      },
                      "currency": "USD"
                    },
                    "accounting": {
                      "glAccount": {
                        "id": "2458.90.33--SAVINGS455 GL"
                      },
                      "apJournal": {
                        "key": "18"
                      },
                      "arJournal": {
                        "id": "ARJ--Accounts Receivable Journal"
                      },
                      "bankingTimeZone": "GMT+05:30 Bombay, Calcutta, Madras, New Delhi",
                      "serviceChargeGLAccount": {
                        "key": "417"
                      },
                      "interestGLAccount": {
                        "key": "572"
                      },
                      "disableInterEntityTransfer": true
                    },
                    "reconciliation": {
                      "matchingSequenceNumber": {
                        "key": "48"
                      },
                      "useSequenceNumberForAutoMatch": false,
                      "useMatchSequenceForManualMatch": true
                    },
                    "department": {
                      "id": "11--Accounting"
                    },
                    "location": {
                      "id": "1--United States of America"
                    },
                    "status": "active",
                    "ruleSet": {
                      "key": "53"
                    }
                  }
                }
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Created savings account",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "title": "New savings-account",
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
                  "201 response example": {
                    "value": {
                      "ia::result": {
                        "key": "12345",
                        "id": "ID123",
                        "href": "/objects/<application>/<name>/12345"
                      },
                      "ia::meta": {
                        "totalCount": 3,
                        "totalSuccess": 2,
                        "totalError": 1
                      }
                    }
                  }
                }
              }
            }
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
            "name": "Create a savings account",
            "source": "curl --request POST \\\n  --url https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account \\\n  --header 'Content-Type: application/json' \\\n  --data '{\"id\":\"SAVINGS455_6780\",\"bankAccountDetails\":{\"accountNumber\":\"\",\"bankName\":\"Savings Bank of America New\",\"routingNumber\":\"565676545\",\"branchId\":\"123456791\",\"phoneNumber\":\"6509876545\",\"bankAddress\":{\"addressLine1\":\"73466 Linkln St\",\"addressLine2\":null,\"addressLine3\":null,\"city\":\"Montaine View\",\"country\":\"United States\",\"postCode\":\"67898\",\"state\":\"CA\"},\"currency\":\"USD\"},\"accounting\":{\"glAccount\":{\"id\":\"2458.90.33--SAVINGS455 GL\"},\"apJournal\":{\"key\":\"18\"},\"arJournal\":{\"id\":\"ARJ--Accounts Receivable Journal\"},\"bankingTimeZone\":\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\",\"serviceChargeGLAccount\":{\"key\":\"417\"},\"interestGLAccount\":{\"key\":\"572\"},\"disableInterEntityTransfer\":true},\"reconciliation\":{\"matchingSequenceNumber\":{\"key\":\"48\"},\"useSequenceNumberForAutoMatch\":false,\"useMatchSequenceForManualMatch\":true},\"department\":{\"id\":\"11--Accounting\"},\"location\":{\"id\":\"1--United States of America\"},\"status\":\"active\",\"ruleSet\":{\"key\":\"53\"}}'",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_httpie",
            "name": "Create a savings account",
            "source": "echo '{\"id\":\"SAVINGS455_6780\",\"bankAccountDetails\":{\"accountNumber\":\"\",\"bankName\":\"Savings Bank of America New\",\"routingNumber\":\"565676545\",\"branchId\":\"123456791\",\"phoneNumber\":\"6509876545\",\"bankAddress\":{\"addressLine1\":\"73466 Linkln St\",\"addressLine2\":null,\"addressLine3\":null,\"city\":\"Montaine View\",\"country\":\"United States\",\"postCode\":\"67898\",\"state\":\"CA\"},\"currency\":\"USD\"},\"accounting\":{\"glAccount\":{\"id\":\"2458.90.33--SAVINGS455 GL\"},\"apJournal\":{\"key\":\"18\"},\"arJournal\":{\"id\":\"ARJ--Accounts Receivable Journal\"},\"bankingTimeZone\":\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\",\"serviceChargeGLAccount\":{\"key\":\"417\"},\"interestGLAccount\":{\"key\":\"572\"},\"disableInterEntityTransfer\":true},\"reconciliation\":{\"matchingSequenceNumber\":{\"key\":\"48\"},\"useSequenceNumberForAutoMatch\":false,\"useMatchSequenceForManualMatch\":true},\"department\":{\"id\":\"11--Accounting\"},\"location\":{\"id\":\"1--United States of America\"},\"status\":\"active\",\"ruleSet\":{\"key\":\"53\"}}' |  \\\n  http POST https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account \\\n  Content-Type:application/json",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_wget",
            "name": "Create a savings account",
            "source": "wget --quiet \\\n  --method POST \\\n  --header 'Content-Type: application/json' \\\n  --body-data '{\"id\":\"SAVINGS455_6780\",\"bankAccountDetails\":{\"accountNumber\":\"\",\"bankName\":\"Savings Bank of America New\",\"routingNumber\":\"565676545\",\"branchId\":\"123456791\",\"phoneNumber\":\"6509876545\",\"bankAddress\":{\"addressLine1\":\"73466 Linkln St\",\"addressLine2\":null,\"addressLine3\":null,\"city\":\"Montaine View\",\"country\":\"United States\",\"postCode\":\"67898\",\"state\":\"CA\"},\"currency\":\"USD\"},\"accounting\":{\"glAccount\":{\"id\":\"2458.90.33--SAVINGS455 GL\"},\"apJournal\":{\"key\":\"18\"},\"arJournal\":{\"id\":\"ARJ--Accounts Receivable Journal\"},\"bankingTimeZone\":\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\",\"serviceChargeGLAccount\":{\"key\":\"417\"},\"interestGLAccount\":{\"key\":\"572\"},\"disableInterEntityTransfer\":true},\"reconciliation\":{\"matchingSequenceNumber\":{\"key\":\"48\"},\"useSequenceNumberForAutoMatch\":false,\"useMatchSequenceForManualMatch\":true},\"department\":{\"id\":\"11--Accounting\"},\"location\":{\"id\":\"1--United States of America\"},\"status\":\"active\",\"ruleSet\":{\"key\":\"53\"}}' \\\n  --output-document \\\n  - https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_xhr",
            "name": "Create a savings account",
            "source": "const data = JSON.stringify({\n  \"id\": \"SAVINGS455_6780\",\n  \"bankAccountDetails\": {\n    \"accountNumber\": \"\",\n    \"bankName\": \"Savings Bank of America New\",\n    \"routingNumber\": \"565676545\",\n    \"branchId\": \"123456791\",\n    \"phoneNumber\": \"6509876545\",\n    \"bankAddress\": {\n      \"addressLine1\": \"73466 Linkln St\",\n      \"addressLine2\": null,\n      \"addressLine3\": null,\n      \"city\": \"Montaine View\",\n      \"country\": \"United States\",\n      \"postCode\": \"67898\",\n      \"state\": \"CA\"\n    },\n    \"currency\": \"USD\"\n  },\n  \"accounting\": {\n    \"glAccount\": {\n      \"id\": \"2458.90.33--SAVINGS455 GL\"\n    },\n    \"apJournal\": {\n      \"key\": \"18\"\n    },\n    \"arJournal\": {\n      \"id\": \"ARJ--Accounts Receivable Journal\"\n    },\n    \"bankingTimeZone\": \"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\",\n    \"serviceChargeGLAccount\": {\n      \"key\": \"417\"\n    },\n    \"interestGLAccount\": {\n      \"key\": \"572\"\n    },\n    \"disableInterEntityTransfer\": true\n  },\n  \"reconciliation\": {\n    \"matchingSequenceNumber\": {\n      \"key\": \"48\"\n    },\n    \"useSequenceNumberForAutoMatch\": false,\n    \"useMatchSequenceForManualMatch\": true\n  },\n  \"department\": {\n    \"id\": \"11--Accounting\"\n  },\n  \"location\": {\n    \"id\": \"1--United States of America\"\n  },\n  \"status\": \"active\",\n  \"ruleSet\": {\n    \"key\": \"53\"\n  }\n});\n\nconst xhr = new XMLHttpRequest();\nxhr.withCredentials = true;\n\nxhr.addEventListener(\"readystatechange\", function () {\n  if (this.readyState === this.DONE) {\n    console.log(this.responseText);\n  }\n});\n\nxhr.open(\"POST\", \"https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account\");\nxhr.setRequestHeader(\"Content-Type\", \"application/json\");\n\nxhr.send(data);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_jquery",
            "name": "Create a savings account",
            "source": "const settings = {\n  \"async\": true,\n  \"crossDomain\": true,\n  \"url\": \"https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account\",\n  \"method\": \"POST\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  },\n  \"processData\": false,\n  \"data\": \"{\\\"id\\\":\\\"SAVINGS455_6780\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"\\\",\\\"bankName\\\":\\\"Savings Bank of America New\\\",\\\"routingNumber\\\":\\\"565676545\\\",\\\"branchId\\\":\\\"123456791\\\",\\\"phoneNumber\\\":\\\"6509876545\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"73466 Linkln St\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":null,\\\"city\\\":\\\"Montaine View\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"67898\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"2458.90.33--SAVINGS455 GL\\\"},\\\"apJournal\\\":{\\\"key\\\":\\\"18\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"ARJ--Accounts Receivable Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\\\",\\\"serviceChargeGLAccount\\\":{\\\"key\\\":\\\"417\\\"},\\\"interestGLAccount\\\":{\\\"key\\\":\\\"572\\\"},\\\"disableInterEntityTransfer\\\":true},\\\"reconciliation\\\":{\\\"matchingSequenceNumber\\\":{\\\"key\\\":\\\"48\\\"},\\\"useSequenceNumberForAutoMatch\\\":false,\\\"useMatchSequenceForManualMatch\\\":true},\\\"department\\\":{\\\"id\\\":\\\"11--Accounting\\\"},\\\"location\\\":{\\\"id\\\":\\\"1--United States of America\\\"},\\\"status\\\":\\\"active\\\",\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\"}}\"\n};\n\n$.ajax(settings).done(function (response) {\n  console.log(response);\n});",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "node_native",
            "name": "Create a savings account",
            "source": "const http = require(\"https\");\n\nconst options = {\n  \"method\": \"POST\",\n  \"hostname\": \"api.intacct.com\",\n  \"port\": null,\n  \"path\": \"/ia/api/v1/objects/cash-management/savings-account\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  }\n};\n\nconst req = http.request(options, function (res) {\n  const chunks = [];\n\n  res.on(\"data\", function (chunk) {\n    chunks.push(chunk);\n  });\n\n  res.on(\"end\", function () {\n    const body = Buffer.concat(chunks);\n    console.log(body.toString());\n  });\n});\n\nreq.write(JSON.stringify({\n  id: 'SAVINGS455_6780',\n  bankAccountDetails: {\n    accountNumber: '',\n    bankName: 'Savings Bank of America New',\n    routingNumber: '565676545',\n    branchId: '123456791',\n    phoneNumber: '6509876545',\n    bankAddress: {\n      addressLine1: '73466 Linkln St',\n      addressLine2: null,\n      addressLine3: null,\n      city: 'Montaine View',\n      country: 'United States',\n      postCode: '67898',\n      state: 'CA'\n    },\n    currency: 'USD'\n  },\n  accounting: {\n    glAccount: {id: '2458.90.33--SAVINGS455 GL'},\n    apJournal: {key: '18'},\n    arJournal: {id: 'ARJ--Accounts Receivable Journal'},\n    bankingTimeZone: 'GMT+05:30 Bombay, Calcutta, Madras, New Delhi',\n    serviceChargeGLAccount: {key: '417'},\n    interestGLAccount: {key: '572'},\n    disableInterEntityTransfer: true\n  },\n  reconciliation: {\n    matchingSequenceNumber: {key: '48'},\n    useSequenceNumberForAutoMatch: false,\n    useMatchSequenceForManualMatch: true\n  },\n  department: {id: '11--Accounting'},\n  location: {id: '1--United States of America'},\n  status: 'active',\n  ruleSet: {key: '53'}\n}));\nreq.end();",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_httpclient",
            "name": "Create a savings account",
            "source": "var client = new HttpClient();\nvar request = new HttpRequestMessage\n{\n    Method = HttpMethod.Post,\n    RequestUri = new Uri(\"https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account\"),\n    Content = new StringContent(\"{\\\"id\\\":\\\"SAVINGS455_6780\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"\\\",\\\"bankName\\\":\\\"Savings Bank of America New\\\",\\\"routingNumber\\\":\\\"565676545\\\",\\\"branchId\\\":\\\"123456791\\\",\\\"phoneNumber\\\":\\\"6509876545\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"73466 Linkln St\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":null,\\\"city\\\":\\\"Montaine View\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"67898\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"2458.90.33--SAVINGS455 GL\\\"},\\\"apJournal\\\":{\\\"key\\\":\\\"18\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"ARJ--Accounts Receivable Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\\\",\\\"serviceChargeGLAccount\\\":{\\\"key\\\":\\\"417\\\"},\\\"interestGLAccount\\\":{\\\"key\\\":\\\"572\\\"},\\\"disableInterEntityTransfer\\\":true},\\\"reconciliation\\\":{\\\"matchingSequenceNumber\\\":{\\\"key\\\":\\\"48\\\"},\\\"useSequenceNumberForAutoMatch\\\":false,\\\"useMatchSequenceForManualMatch\\\":true},\\\"department\\\":{\\\"id\\\":\\\"11--Accounting\\\"},\\\"location\\\":{\\\"id\\\":\\\"1--United States of America\\\"},\\\"status\\\":\\\"active\\\",\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\"}}\")\n    {\n        Headers =\n        {\n            ContentType = new MediaTypeHeaderValue(\"application/json\")\n        }\n    }\n};\nusing (var response = await client.SendAsync(request))\n{\n    response.EnsureSuccessStatusCode();\n    var body = await response.Content.ReadAsStringAsync();\n    Console.WriteLine(body);\n}",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_restsharp",
            "name": "Create a savings account",
            "source": "var client = new RestClient(\"https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account\");\nvar request = new RestRequest(Method.POST);\nrequest.AddHeader(\"Content-Type\", \"application/json\");\nrequest.AddParameter(\"application/json\", \"{\\\"id\\\":\\\"SAVINGS455_6780\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"\\\",\\\"bankName\\\":\\\"Savings Bank of America New\\\",\\\"routingNumber\\\":\\\"565676545\\\",\\\"branchId\\\":\\\"123456791\\\",\\\"phoneNumber\\\":\\\"6509876545\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"73466 Linkln St\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":null,\\\"city\\\":\\\"Montaine View\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"67898\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"2458.90.33--SAVINGS455 GL\\\"},\\\"apJournal\\\":{\\\"key\\\":\\\"18\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"ARJ--Accounts Receivable Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\\\",\\\"serviceChargeGLAccount\\\":{\\\"key\\\":\\\"417\\\"},\\\"interestGLAccount\\\":{\\\"key\\\":\\\"572\\\"},\\\"disableInterEntityTransfer\\\":true},\\\"reconciliation\\\":{\\\"matchingSequenceNumber\\\":{\\\"key\\\":\\\"48\\\"},\\\"useSequenceNumberForAutoMatch\\\":false,\\\"useMatchSequenceForManualMatch\\\":true},\\\"department\\\":{\\\"id\\\":\\\"11--Accounting\\\"},\\\"location\\\":{\\\"id\\\":\\\"1--United States of America\\\"},\\\"status\\\":\\\"active\\\",\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\"}}\", ParameterType.RequestBody);\nIRestResponse response = client.Execute(request);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_python3",
            "name": "Create a savings account",
            "source": "import http.client\n\nconn = http.client.HTTPSConnection(\"api.intacct.com\")\n\npayload = \"{\\\"id\\\":\\\"SAVINGS455_6780\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"\\\",\\\"bankName\\\":\\\"Savings Bank of America New\\\",\\\"routingNumber\\\":\\\"565676545\\\",\\\"branchId\\\":\\\"123456791\\\",\\\"phoneNumber\\\":\\\"6509876545\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"73466 Linkln St\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":null,\\\"city\\\":\\\"Montaine View\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"67898\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"2458.90.33--SAVINGS455 GL\\\"},\\\"apJournal\\\":{\\\"key\\\":\\\"18\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"ARJ--Accounts Receivable Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\\\",\\\"serviceChargeGLAccount\\\":{\\\"key\\\":\\\"417\\\"},\\\"interestGLAccount\\\":{\\\"key\\\":\\\"572\\\"},\\\"disableInterEntityTransfer\\\":true},\\\"reconciliation\\\":{\\\"matchingSequenceNumber\\\":{\\\"key\\\":\\\"48\\\"},\\\"useSequenceNumberForAutoMatch\\\":false,\\\"useMatchSequenceForManualMatch\\\":true},\\\"department\\\":{\\\"id\\\":\\\"11--Accounting\\\"},\\\"location\\\":{\\\"id\\\":\\\"1--United States of America\\\"},\\\"status\\\":\\\"active\\\",\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\"}}\"\n\nheaders = { 'Content-Type': \"application/json\" }\n\nconn.request(\"POST\", \"/ia/api/v1/objects/cash-management/savings-account\", payload, headers)\n\nres = conn.getresponse()\ndata = res.read()\n\nprint(data.decode(\"utf-8\"))",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_requests",
            "name": "Create a savings account",
            "source": "import requests\n\nurl = \"https://api.intacct.com/ia/api/v1/objects/cash-management/savings-account\"\n\npayload = {\n    \"id\": \"SAVINGS455_6780\",\n    \"bankAccountDetails\": {\n        \"accountNumber\": \"\",\n        \"bankName\": \"Savings Bank of America New\",\n        \"routingNumber\": \"565676545\",\n        \"branchId\": \"123456791\",\n        \"phoneNumber\": \"6509876545\",\n        \"bankAddress\": {\n            \"addressLine1\": \"73466 Linkln St\",\n            \"addressLine2\": None,\n            \"addressLine3\": None,\n            \"city\": \"Montaine View\",\n            \"country\": \"United States\",\n            \"postCode\": \"67898\",\n            \"state\": \"CA\"\n        },\n        \"currency\": \"USD\"\n    },\n    \"accounting\": {\n        \"glAccount\": {\"id\": \"2458.90.33--SAVINGS455 GL\"},\n        \"apJournal\": {\"key\": \"18\"},\n        \"arJournal\": {\"id\": \"ARJ--Accounts Receivable Journal\"},\n        \"bankingTimeZone\": \"GMT+05:30 Bombay, Calcutta, Madras, New Delhi\",\n        \"serviceChargeGLAccount\": {\"key\": \"417\"},\n        \"interestGLAccount\": {\"key\": \"572\"},\n        \"disableInterEntityTransfer\": True\n    },\n    \"reconciliation\": {\n        \"matchingSequenceNumber\": {\"key\": \"48\"},\n        \"useSequenceNumberForAutoMatch\": False,\n        \"useMatchSequenceForManualMatch\": True\n    },\n    \"department\": {\"id\": \"11--Accounting\"},\n    \"location\": {\"id\": \"1--United States of America\"},\n    \"status\": \"active\",\n    \"ruleSet\": {\"key\": \"53\"}\n}\nheaders = {\"Content-Type\": \"application/json\"}\n\nresponse = requests.request(\"POST\", url, json=payload, headers=headers)\n\nprint(response.text)",
            "mimeType": "application/json",
            "isAutoGenerated": true
          }
        ]
      }
    }
  },
  "components": {
    "schemas": {
      "objects.cash-management.savings-account": {
        "type": "object",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the savings account.",
            "readOnly": true,
            "example": "34"
          },
          "id": {
            "type": "string",
            "description": "Name or other unique identifier for the savings account. The account ID cannot be modified.",
            "example": "SBI"
          },
          "href": {
            "type": "string",
            "description": "URL for the savings account.",
            "readOnly": true,
            "example": "/objects/cash-management/savings-account/34"
          },
          "bankAccountDetails": {
            "type": "object",
            "description": "Bank account details",
            "properties": {
              "accountNumber": {
                "type": "string",
                "description": "Bank account number for this savings account.",
                "example": "4356789400"
              },
              "bankName": {
                "type": "string",
                "description": "Name of the bank for this savings account.",
                "example": "Bank of the West"
              },
              "routingNumber": {
                "type": "string",
                "description": "Routing number for this savings account.",
                "example": "123456791"
              },
              "branchId": {
                "type": "string",
                "description": "Bank branch ID for this savings account.",
                "example": "123456791"
              },
              "phoneNumber": {
                "type": "string",
                "description": "Phone number of the bank branch.",
                "example": "555-927-6200"
              },
              "currency": {
                "type": "string",
                "description": "The currency for this account. The default is the base currency for the company or entity. If this account is with a foreign bank, the currency should match the country.",
                "example": "USD"
              },
              "bankAddress": {
                "type": "object",
                "properties": {
                  "city": {
                    "type": "string",
                    "description": "City where the bank is located.",
                    "example": "Fremont"
                  },
                  "state": {
                    "type": "string",
                    "description": "State where the bank is located.",
                    "example": "CA"
                  },
                  "postCode": {
                    "type": "string",
                    "description": "Zip or postal code for the bank.",
                    "example": "94536"
                  },
                  "country": {
                    "type": "string",
                    "description": "Country where the bank is located.",
                    "example": "United States"
                  },
                  "addressLine1": {
                    "type": "string",
                    "description": "Line 1 of the street address for the bank.",
                    "example": "39148 Paseo Padre Pkwy"
                  },
                  "addressLine2": {
                    "type": "string",
                    "description": "Line 2 of the street address for the bank.",
                    "example": "Suite 104"
                  },
                  "addressLine3": {
                    "type": "string",
                    "description": "Line 3 of the street address for the bank which provides additional geographical information.",
                    "example": "Western industrial area"
                  }
                }
              }
            }
          },
          "accounting": {
            "type": "object",
            "description": "Accounting information",
            "properties": {
              "glAccount": {
                "type": "object",
                "description": "General Ledger account that this savings account is associated with.",
                "properties": {
                  "key": {
                    "type": "string",
                    "example": "90"
                  },
                  "id": {
                    "type": "string",
                    "example": "1047--Savings a/c France"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/31"
                  }
                }
              },
              "apJournal": {
                "type": "object",
                "description": "Default payables GL journal",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the gl-journal.",
                    "example": "3"
                  },
                  "id": {
                    "type": "string",
                    "description": "The id of the journal",
                    "example": "AP ADJ--AP Adjustment Journal"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL for this journal.",
                    "example": "/objects/general-ledger/journal/3",
                    "readOnly": true
                  }
                }
              },
              "arJournal": {
                "type": "object",
                "description": "Default receivables GL journal",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the gl-journal.",
                    "example": "3"
                  },
                  "id": {
                    "type": "string",
                    "description": "The id of the journal",
                    "example": "AR ADJ--AR Adjustment Journal"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL for this journal.",
                    "example": "/objects/general-ledger/journal/3",
                    "readOnly": true
                  }
                }
              },
              "bankingTimeZone": {
                "type": "string",
                "description": "Time zone",
                "nullable": true,
                "example": "GMT+02:00 Eastern Europe Time",
                "enum": [
                  null,
                  "GMT (Greenwich Mean Time) Dublin, Edinburgh, London",
                  "GMT+00:00 Western Europe Time",
                  "GMT+01:00 Western Europe Summer Time",
                  "GMT+01:00 British Summer Time",
                  "GMT+01:00 Irish Summer Time",
                  "GMT+01:00 Central Europe Time",
                  "GMT+01:00 Berlin, Stockholm, Rome, Bern, Brussels",
                  "GMT+01:00 Lisbon, Warsaw",
                  "GMT+01:00 Paris, Madrid",
                  "GMT+01:00 Prague",
                  "GMT+02:00 Central Europe Summer Time",
                  "GMT+02:00 Eastern Europe Time",
                  "GMT+02:00 Athens, Helsinki, Istanbul",
                  "GMT+02:00 Cairo",
                  "GMT+02:00 Harare, Pretoria",
                  "GMT+02:00 Israel",
                  "GMT+03:00 Eastern Europe Summer Time",
                  "GMT+03:00 Baghdad, Kuwait, Nairobi, Riyadh",
                  "GMT+03:00 Moscow, St. Petersburg, Volgograd",
                  "GMT+03:30 Tehran",
                  "GMT+04:00 Moscow Summer Time",
                  "GMT+04:00 Abu Dhabi, Muscat, Tbilisi, Kazan",
                  "GMT+04:30 Kabul",
                  "GMT+05:00 Islamabad, Karachi, Sverdlovsk, Tashkent",
                  "GMT+05:30 Bombay, Calcutta, Madras, New Delhi",
                  "GMT+06:00 Almaty, Dhaka",
                  "GMT+07:00 Bangkok, Jakarta, Hanoi",
                  "GMT+08:00 Beijing, Chongqing, Urumqi",
                  "GMT+08:00 Hong Kong SAR, Perth, Singapore, Taipei",
                  "GMT+08:00 (Australian) Western Standard Time",
                  "GMT+09:00 Tokyo, Osaka, Sapporo, Seoul, Yakutsk",
                  "GMT+09:30 (Australian) Central Standard Time",
                  "GMT+10:30 (Australian) Central Daylight Time",
                  "GMT+09:30 Adelaide",
                  "GMT+09:30 Darwin",
                  "GMT+10:00 Brisbane, Melbourne, Sydney",
                  "GMT+10:00 Guam, Port Moresby",
                  "GMT+10:00 Vladivostok",
                  "GMT+10:00 (Australian) Eastern Standard Time",
                  "GMT+11:00  (Australian) Eastern Daylight Time",
                  "GMT+12:00 Fiji Islands, Marshall Islands",
                  "GMT+12:00 Kamchatka",
                  "GMT+12:00 Magadan, Solomon Islands, New Caledonia",
                  "GMT+12:00 Wellington, Auckland",
                  "GMT+13:00 Nuku`alofa",
                  "GMT+13:00 Samoa",
                  "GMT-01:00 Azores, Cape Verde Island",
                  "GMT-03:00 Brasilia",
                  "GMT-03:00 Buenos Aires, Georgetown",
                  "GMT-03:30 Newfoundland Standard Time",
                  "GMT-02:30 Newfoundland Daylight Time",
                  "GMT-04:00 Atlantic Standard Time",
                  "GMT-03:00 Atlantic Daylight Time",
                  "GMT-04:00 Caracas, La Paz",
                  "GMT-05:00 Bogota, Lima",
                  "GMT-05:00 Eastern Standard Time",
                  "GMT-04:00 Eastern Daylight Saving Time",
                  "GMT-05:00 Indiana (East)",
                  "GMT-06:00 Central Standard Time",
                  "GMT-05:00 Central Daylight Saving Time",
                  "GMT-06:00 Mexico City, Tegucigalpa",
                  "GMT-06:00 Saskatchewan",
                  "GMT-07:00 Arizona",
                  "GMT-07:00 Mountain Standard Time",
                  "GMT-06:00 Mountain Daylight Saving Time",
                  "GMT-08:00 Pacific Standard Time",
                  "GMT-07:00 Pacific Daylight Saving Time",
                  "GMT-09:00 Alaska Standard Time",
                  "GMT-08:00 Alaska Standard Daylight Saving Time",
                  "GMT-10:00 Hawaii",
                  "GMT-11:00 Midway Island, Samoa",
                  "GMT-12:00 Eniwetok, Kwajalein"
                ],
                "default": null
              },
              "disableInterEntityTransfer": {
                "type": "boolean",
                "description": "Exclude this account from inter-entity transfers (IET) even if IET is globally enabled for the entire multi-entity shared structure of companies.",
                "default": false,
                "example": false
              },
              "serviceChargeGLAccount": {
                "type": "object",
                "description": "General ledger account for service charges. Used for reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "example": "13"
                  },
                  "id": {
                    "type": "string",
                    "example": "1004--Lloyds bank"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/13"
                  }
                }
              },
              "serviceChargeAccountLabel": {
                "type": "object",
                "description": "General ledger account label for service charges.",
                "properties": {
                  "key": {
                    "type": "string",
                    "example": "8"
                  },
                  "id": {
                    "type": "string",
                    "example": "Accounting Fees"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/account-label/8"
                  }
                }
              },
              "interestGLAccount": {
                "type": "object",
                "description": "General ledger account for earned interest. Used for reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "example": "15"
                  },
                  "id": {
                    "type": "string",
                    "example": "1006--Banorte Bank"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/15"
                  }
                }
              },
              "interestAccountLabel": {
                "type": "object",
                "description": "General ledger account label for earned interest.",
                "properties": {
                  "key": {
                    "type": "string",
                    "example": "35"
                  },
                  "id": {
                    "type": "string",
                    "example": "Interest Fees"
                  },
                  "href": {
                    "type": "string",
                    "readOnly": true,
                    "example": "/objects/accounts-receivable/account-label/35"
                  }
                }
              }
            }
          },
          "reconciliation": {
            "type": "object",
            "description": "Reconciliation information",
            "properties": {
              "lastReconciledBalance": {
                "type": "string",
                "description": "Last reconciled balance.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "600.00"
              },
              "lastReconciledDate": {
                "type": "string",
                "format": "date",
                "description": "Date of the last reconciliation.",
                "readOnly": true,
                "example": "2022-02-28"
              },
              "cutOffDate": {
                "type": "string",
                "format": "date",
                "description": "The date after which initial reconciliation can begin.",
                "readOnly": true,
                "example": "2022-02-28"
              },
              "inProgressBalance": {
                "type": "string",
                "description": "In progress reconciliation balance.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "200.00"
              },
              "inProgressDate": {
                "type": "string",
                "format": "date",
                "description": "In progress reconciliation date.",
                "readOnly": true,
                "example": "2022-01-28"
              },
              "matchSequence": {
                "type": "object",
                "description": "Reconciliation match sequence",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the document sequence number.",
                    "example": "2"
                  },
                  "id": {
                    "type": "string",
                    "description": "Document sequence ID",
                    "example": "2--Bank sequence Id"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL for the sequence number.",
                    "example": "/objects/company-config/document-sequence/2",
                    "readOnly": true
                  }
                }
              },
              "useMatchSequenceForAutoMatch": {
                "type": "boolean",
                "default": true,
                "description": "Use sequence number for automatically matched transactions.",
                "example": false
              },
              "useMatchSequenceForManualMatch": {
                "type": "boolean",
                "default": true,
                "description": "Use sequence number for manually matched transactions.",
                "example": false
              }
            }
          },
          "department": {
            "type": "object",
            "description": "department",
            "properties": {
              "key": {
                "type": "string",
                "example": "8"
              },
              "id": {
                "type": "string",
                "example": "8--Finance"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/company-config/department/8"
              }
            }
          },
          "location": {
            "type": "object",
            "description": "location",
            "properties": {
              "key": {
                "type": "string",
                "example": "4"
              },
              "id": {
                "type": "string",
                "example": "4--Australia"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/company-config/location/4"
              }
            }
          },
          "status": {
            "$ref": "#/components/schemas/status"
          },
          "audit": {
            "$ref": "#/components/schemas/audit.s1"
          },
          "entity": {
            "$ref": "#/components/schemas/entity-ref"
          },
          "bankingCloudConnection": {
            "$ref": "#/components/schemas/banking-cloud-connection"
          },
          "financialInstitution": {
            "description": "Financial institution reference",
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "readOnly": true,
                "example": "FinOne"
              },
              "key": {
                "type": "string",
                "readOnly": true,
                "example": "1"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/cash-management/financial-institution/1"
              }
            },
            "readOnly": true
          },
          "ruleSet": {
            "type": "object",
            "description": "Applied rule set",
            "properties": {
              "key": {
                "type": "string",
                "example": "1"
              },
              "id": {
                "type": "string",
                "example": "36--RuleSetToMatch"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/cash-management/bank-txn-rule-set/1"
              }
            }
          },
          "restrictions": {
            "type": "object",
            "description": "Restrict a bank account to a specific location or restrict one or more entity/locations to a specific account.",
            "properties": {
              "restrictionType": {
                "type": "string",
                "description": "Set which entities/locations within the company can access and use this checking account.\n\n**Valid values**\n- `unrestricted` - (default) This account is available to the top-level company and all entity-level locations.\n- `rootOnly` - Only the top-level company of a multi-entity structure can access this account.\n- `restricted` - Only specified locations, location groups, departments, or department groups can access this account.\n",
                "enum": [
                  "unrestricted",
                  "rootOnly",
                  "restricted"
                ],
                "example": "unrestricted",
                "default": "unrestricted"
              },
              "locations": {
                "type": "array",
                "description": "List of locations that can access this checking account when `restrictionType` is set to `restricted`.",
                "items": {
                  "type": "string"
                },
                "example": [
                  "1--United States of America",
                  "2--United Kingdom"
                ]
              }
            }
          }
        }
      },
      "cash-management-savings-accountRequiredProperties": {
        "type": "object",
        "required": [
          "id",
          "location"
        ],
        "properties": {
          "bankAccountDetails": {
            "type": "object",
            "required": [
              "bankName",
              "currency"
            ]
          },
          "accounting": {
            "type": "object",
            "required": [
              "glAccount"
            ]
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
      "timezone": {
        "type": "string",
        "description": "Time zone.",
        "nullable": true,
        "example": "GMT-08:00 Pacific Standard Time",
        "enum": [
          null,
          "GMT (Greenwich Mean Time) Dublin, Edinburgh, London",
          "GMT+00:00 Western Europe Time",
          "GMT+01:00 Western Europe Summer Time",
          "GMT+01:00 British Summer Time",
          "GMT+01:00 Irish Summer Time",
          "GMT+01:00 Central Europe Time",
          "GMT+01:00 Berlin, Stockholm, Rome, Bern, Brussels",
          "GMT+01:00 Lisbon, Warsaw",
          "GMT+01:00 Paris, Madrid",
          "GMT+01:00 Prague",
          "GMT+02:00 Central Europe Summer Time",
          "GMT+02:00 Eastern Europe Time",
          "GMT+02:00 Athens, Helsinki, Istanbul",
          "GMT+02:00 Cairo",
          "GMT+02:00 Harare, Pretoria",
          "GMT+02:00 Israel",
          "GMT+03:00 Eastern Europe Summer Time",
          "GMT+03:00 Baghdad, Kuwait, Nairobi, Riyadh",
          "GMT+03:00 Moscow, St. Petersburg, Volgograd",
          "GMT+03:30 Tehran",
          "GMT+04:00 Moscow Summer Time",
          "GMT+04:00 Abu Dhabi, Muscat, Tbilisi, Kazan",
          "GMT+04:30 Kabul",
          "GMT+05:00 Islamabad, Karachi, Sverdlovsk, Tashkent",
          "GMT+05:30 Bombay, Calcutta, Madras, New Delhi",
          "GMT+06:00 Almaty, Dhaka",
          "GMT+07:00 Bangkok, Jakarta, Hanoi",
          "GMT+08:00 Beijing, Chongqing, Urumqi",
          "GMT+08:00 Hong Kong SAR, Perth, Singapore, Taipei",
          "GMT+08:00 (Australian) Western Standard Time",
          "GMT+09:00 Tokyo, Osaka, Sapporo, Seoul, Yakutsk",
          "GMT+09:30 (Australian) Central Standard Time",
          "GMT+10:30 (Australian) Central Daylight Time",
          "GMT+09:30 Adelaide",
          "GMT+09:30 Darwin",
          "GMT+10:00 Brisbane, Melbourne, Sydney",
          "GMT+10:00 Guam, Port Moresby",
          "GMT+10:00 Vladivostok",
          "GMT+10:00 (Australian) Eastern Standard Time",
          "GMT+11:00  (Australian) Eastern Daylight Time",
          "GMT+12:00 Fiji Islands, Marshall Islands",
          "GMT+12:00 Kamchatka",
          "GMT+12:00 Magadan, Solomon Islands, New Caledonia",
          "GMT+12:00 Wellington, Auckland",
          "GMT+13:00 Nuku`alofa",
          "GMT+13:00 Samoa",
          "GMT-01:00 Azores, Cape Verde Island",
          "GMT-03:00 Brasilia",
          "GMT-03:00 Buenos Aires, Georgetown",
          "GMT-03:30 Newfoundland Standard Time",
          "GMT-02:30 Newfoundland Daylight Time",
          "GMT-04:00 Atlantic Standard Time",
          "GMT-03:00 Atlantic Daylight Time",
          "GMT-04:00 Caracas, La Paz",
          "GMT-05:00 Bogota, Lima",
          "GMT-05:00 Eastern Standard Time",
          "GMT-04:00 Eastern Daylight Saving Time",
          "GMT-05:00 Indiana (East)",
          "GMT-06:00 Central Standard Time",
          "GMT-05:00 Central Daylight Saving Time",
          "GMT-06:00 Mexico City, Tegucigalpa",
          "GMT-06:00 Saskatchewan",
          "GMT-07:00 Arizona",
          "GMT-07:00 Mountain Standard Time",
          "GMT-06:00 Mountain Daylight Saving Time",
          "GMT-08:00 Pacific Standard Time",
          "GMT-07:00 Pacific Daylight Saving Time",
          "GMT-09:00 Alaska Standard Time",
          "GMT-08:00 Alaska Standard Daylight Saving Time",
          "GMT-10:00 Hawaii",
          "GMT-11:00 Midway Island, Samoa",
          "GMT-12:00 Eniwetok, Kwajalein"
        ]
      },
      "status": {
        "type": "string",
        "description": "Object status. Active objects are fully functional. Inactive objects are essentially hidden and cannot be used or referenced.",
        "enum": [
          "active",
          "inactive"
        ],
        "default": "active",
        "example": "active"
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
      "banking-cloud-connection": {
        "type": "object",
        "properties": {
          "name": {
            "type": "string",
            "description": "Name of the connected bank account",
            "readOnly": true,
            "example": "Plaid Checking"
          },
          "bankName": {
            "type": "string",
            "description": "Name of the connected bank",
            "readOnly": true,
            "example": "Bank of America"
          },
          "status": {
            "type": "string",
            "description": "Connected bank status",
            "enum": [
              null,
              "notConnected",
              "connectionRequested",
              "inProgress",
              "pending",
              "pendingConfirmation",
              "connected",
              "authRequired",
              "verifyingAuth",
              "inactiveFeed",
              "inactiveClient",
              "canceled",
              "invalid",
              "canceling",
              "disconnecting"
            ],
            "readOnly": true,
            "example": "connected",
            "nullable": true
          },
          "importStatus": {
            "type": "string",
            "description": "Transaction import connection status",
            "enum": [
              null,
              "initiated",
              "connected"
            ],
            "readOnly": true,
            "example": "connected",
            "nullable": true
          },
          "lastBankTxnDateTime": {
            "type": "string",
            "format": "date-time",
            "description": "Date of the last bank transaction received",
            "readOnly": true,
            "example": "2014-01-08T11:28:12Z"
          },
          "lastRefreshedDateTime": {
            "type": "string",
            "format": "date-time",
            "description": "Date of the last attempt of fetching bank transactions",
            "readOnly": true,
            "example": "2014-01-08T11:28:12Z"
          },
          "refreshStatus": {
            "type": "string",
            "description": "Current status Of fetching bank transaction",
            "enum": [
              null,
              "queued",
              "refreshing",
              "success",
              "partialSuccess",
              "failure"
            ],
            "example": "queued",
            "readOnly": true,
            "nullable": true,
            "default": null
          },
          "supportMultiAccountLinking": {
            "type": "boolean",
            "description": "Does account support multi account linking",
            "readOnly": true,
            "example": true,
            "default": true
          },
          "availableBalance": {
            "type": "object",
            "description": "Available balance information from connected bank",
            "readOnly": true,
            "properties": {
              "amount": {
                "type": "string",
                "format": "decimal-precision-2",
                "description": "Available balance amount",
                "readOnly": true,
                "nullable": true,
                "example": "100.00"
              },
              "date": {
                "type": "string",
                "format": "date",
                "description": "Available balance date",
                "readOnly": true,
                "nullable": true,
                "example": "2014-01-08"
              }
            }
          },
          "ledgerBalance": {
            "type": "object",
            "description": "Ledger balance information from connected bank",
            "readOnly": true,
            "properties": {
              "amount": {
                "type": "string",
                "format": "decimal-precision-2",
                "description": "Ledger balance amount",
                "nullable": true,
                "readOnly": true,
                "example": "100.00"
              },
              "date": {
                "type": "string",
                "format": "date",
                "description": "Ledger balance date",
                "nullable": true,
                "readOnly": true,
                "example": "2014-01-08"
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
