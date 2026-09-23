```json
{
  "openapi": "3.0.3",
  "info": {
    "title": "Create a credit card account",
    "version": "1",
    "description": "Creates a new credit card account."
  },
  "servers": [
    {
      "url": "https://api.intacct.com/ia/api/v1",
      "x-try-it": "sage-intacct-api"
    }
  ],
  "paths": {
    "/objects/cash-management/credit-card-account": {
      "post": {
        "summary": "Create a credit card account",
        "description": "Creates a new credit card account.",
        "tags": [
          "Cash_Management_Credit card accounts"
        ],
        "operationId": "post-objects-credit-card-account",
        "requestBody": {
          "description": "",
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "description": "Credit card accounts are used to record transactions made outside of Sage Intacct. Credit card accounts include credit and debit payment method accounts.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the credit card account.",
                    "readOnly": true,
                    "example": "10"
                  },
                  "id": {
                    "type": "string",
                    "description": "Name or other unique identifier for the credit card account. The account ID cannot be modified.",
                    "example": "Card101"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the credit card account.",
                    "readOnly": true,
                    "example": "/objects/cash-management/credit-card-account/10"
                  },
                  "accountDetails": {
                    "type": "object",
                    "description": "Credit/debit card account details.",
                    "properties": {
                      "description": {
                        "type": "string",
                        "description": "Optional description about how this card is used.",
                        "example": "Travel Visa card 1"
                      },
                      "cardType": {
                        "type": "string",
                        "description": "Specifies the card type. The card type cannot be changed after the account is created.",
                        "enum": [
                          "visa",
                          "mastercard",
                          "discover",
                          "americanExpress",
                          "dinersClub",
                          "otherChargeCard"
                        ],
                        "example": "visa"
                      },
                      "number": {
                        "type": "string",
                        "description": "Required only if the `cardType` is `americanExpress`. This is a 16-digit card number without spaces or hyphens.",
                        "example": "xxxxxxxxxxxx1111"
                      },
                      "accountType": {
                        "type": "string",
                        "description": "Account type, credit or debit. Credit cards require an associated vendor and offset GL account. Debit cards used to pay bills require an associated checking account and vendor. \n",
                        "enum": [
                          "credit",
                          "debit"
                        ],
                        "example": "credit"
                      },
                      "expirationMonth": {
                        "type": "string",
                        "description": "Month when the credit card expires.",
                        "enum": [
                          "01",
                          "02",
                          "03",
                          "04",
                          "05",
                          "06",
                          "07",
                          "08",
                          "09",
                          "10",
                          "11",
                          "12"
                        ],
                        "example": "11"
                      },
                      "expirationYear": {
                        "type": "string",
                        "description": "Year when the credit card expires.",
                        "example": "2032"
                      },
                      "currency": {
                        "type": "string",
                        "description": "Card currency",
                        "readOnly": true,
                        "example": "USD"
                      },
                      "billingAddress": {
                        "type": "object",
                        "description": "Billing address for the credit card.",
                        "properties": {
                          "addressLine1": {
                            "type": "string",
                            "description": "Street address",
                            "example": "300 Park Ave"
                          },
                          "addressLine2": {
                            "type": "string",
                            "description": "Suite or unit number",
                            "example": "1400"
                          },
                          "addressLine3": {
                            "type": "string",
                            "description": "Address line 3",
                            "example": "Western industrial area"
                          },
                          "city": {
                            "type": "string",
                            "description": "City",
                            "example": "San Jose"
                          },
                          "state": {
                            "type": "string",
                            "description": "State",
                            "example": "CA"
                          },
                          "postCode": {
                            "type": "string",
                            "description": "Zip or postal code",
                            "example": "10001"
                          },
                          "country": {
                            "type": "string",
                            "description": "Country",
                            "example": "USA"
                          },
                          "countryCode": {
                            "type": "string",
                            "description": "ISO country code. When ISO country codes are enabled for a company, both `country` and `countryCode` must be provided.",
                            "example": "US"
                          }
                        }
                      },
                      "debitCardCheckingAccount": {
                        "description": "For credit card accounts defined as a debit card, the checking account linked to the debit card.",
                        "type": "object",
                        "properties": {
                          "id": {
                            "type": "string",
                            "description": "ID for the checking account.",
                            "example": "BOA"
                          },
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the checking account.",
                            "readOnly": true,
                            "example": "10"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the checking account.",
                            "readOnly": true,
                            "example": "/objects/cash-management/checking-account/10"
                          }
                        },
                        "readOnly": true
                      }
                    },
                    "required": [
                      "accountType",
                      "cardType",
                      "expirationMonth",
                      "expirationYear"
                    ]
                  },
                  "accounting": {
                    "type": "object",
                    "description": "Accounting information for the credit card account.",
                    "properties": {
                      "offsetGLAccount": {
                        "type": "object",
                        "description": "Credit card offset GL account, which is the offset account used to track the credit card liability.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key of the offset GL account.",
                            "example": "155"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the offset GL account.",
                            "example": "11000"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the offset GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/155"
                          }
                        }
                      },
                      "financeChargeGLAccount": {
                        "type": "object",
                        "description": "GL account to use for finance charges and other fees during reconciliation if those fees are to be tracked separately.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the GL account.",
                            "example": "201"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the GL account.",
                            "example": "4562.67"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/201"
                          }
                        }
                      },
                      "financeChargeAPAccountLabel": {
                        "type": "object",
                        "description": "Finance charges AP account label.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the AP account label.",
                            "example": "22"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the AP account label.",
                            "example": "Finance Charges"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the AP account label.",
                            "readOnly": true,
                            "example": "/objects/accounts-payable/account-label/22"
                          }
                        }
                      },
                      "otherFeesGLAccount": {
                        "type": "object",
                        "description": "Other fees GL account used only for credit card reconciliation; does not apply to debit cards.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the GL account.",
                            "example": "33"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the GL account.",
                            "example": "3556.1"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/33"
                          }
                        }
                      },
                      "otherFeesAPAccountLabel": {
                        "type": "object",
                        "description": "Other fees AP account label.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the AP account label.",
                            "example": "5"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the AP account label.",
                            "example": "Other fees"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the AP account label.",
                            "readOnly": true,
                            "example": "/objects/accounts-payable/account-label/5"
                          }
                        }
                      },
                      "defaultAccrualBasisGLJournal": {
                        "type": "object",
                        "description": "Default accrual basis journal.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the GL journal.",
                            "example": "13"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the GL journal.",
                            "example": "GJ"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL journal.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/journal/13"
                          }
                        }
                      },
                      "defaultCashBasisGLJournal": {
                        "type": "object",
                        "description": "Default cash basis journal. For dual-method reporting, separate accrual and cash journals can be specified.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the GL journal.",
                            "example": "33"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the GL journal.",
                            "example": "IJ"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL journal.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/journal/33"
                          }
                        }
                      },
                      "employeeExpenseGLAccount": {
                        "type": "object",
                        "description": "GL account to use for employee expense.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the GL account.",
                            "example": "201"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the GL account.",
                            "example": "4562.67"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/201"
                          }
                        }
                      },
                      "employeeExpenseAccountLabel": {
                        "type": "object",
                        "description": "Employee expense AP account label.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the AP account label.",
                            "example": "22"
                          },
                          "id": {
                            "type": "string",
                            "description": "ID for the AP account label.",
                            "example": "Employee Expense"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the AP account label.",
                            "readOnly": true,
                            "example": "/objects/accounts-payable/account-label/22"
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
                        "description": "Set to `true` to disable inter-entity transfers.",
                        "default": false,
                        "example": false
                      },
                      "useInEmployeeExpense": {
                        "type": "boolean",
                        "description": "Set to `true` to use this credit card account for employee expense.",
                        "default": false,
                        "example": false
                      }
                    },
                    "required": [
                      "offsetGLAccount"
                    ]
                  },
                  "reconciliation": {
                    "type": "object",
                    "description": "Reconciliation information for the credit card account; does not apply to debit card accounts.",
                    "properties": {
                      "lastReconciledBalance": {
                        "type": "string",
                        "description": "If the account was previously reconciled, this is the balance of that reconciliation.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "110000.00"
                      },
                      "lastReconciledDate": {
                        "type": "string",
                        "format": "date",
                        "description": "If the account was previously reconciled, this is the date of that reconciliation.",
                        "readOnly": true,
                        "example": "2024-04-15"
                      },
                      "cutOffDate": {
                        "type": "string",
                        "format": "date",
                        "description": "The date after which the first reconciliation can begin.",
                        "readOnly": true,
                        "example": "2023-07-31"
                      },
                      "inProgressBalance": {
                        "type": "string",
                        "description": "For reconciliations in progress, the current reconciliation balance.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "160207.75"
                      },
                      "inProgressDate": {
                        "type": "string",
                        "format": "date",
                        "description": "For reconciliations in progress, the date of that reconciliation.",
                        "readOnly": true,
                        "example": "2024-04-21"
                      },
                      "matchSequence": {
                        "type": "object",
                        "description": "Reconciliation match sequence. This is a document sequence that tracks matches in reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "System-assigned key for the document sequence number.",
                            "example": "2",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Document sequence ID",
                            "example": "2--Bank sequence Id",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the sequence number.",
                            "readOnly": true,
                            "example": "/objects/company-config/document-sequence/2",
                            "nullable": true
                          }
                        }
                      },
                      "useMatchSequenceForAutoMatch": {
                        "type": "boolean",
                        "default": true,
                        "description": "Use sequence number for transactions that were matched automatically with a rule set.",
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
                    "description": "Default department to associate with this card account.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "System-assigned key for the department.",
                        "example": "11"
                      },
                      "id": {
                        "type": "string",
                        "description": "ID for the department.",
                        "example": "8"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the department.",
                        "readOnly": true,
                        "example": "/objects/company-config/department/11"
                      }
                    }
                  },
                  "location": {
                    "type": "object",
                    "description": "Default location for transactions that draw on this account.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "System-assigned key for the location.",
                        "example": "5"
                      },
                      "id": {
                        "type": "string",
                        "description": "ID for the location.",
                        "example": "CA"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the location.",
                        "readOnly": true,
                        "example": "/objects/company-config/location/6"
                      }
                    }
                  },
                  "vendor": {
                    "type": "object",
                    "description": "The vendor is the credit card provider. Associate the credit card with a vendor to pay off the credit card in accounts payable. All credit card charges and payments go to the ledger for this vendor. Use a unique vendor for each credit card account. The vendor cannot be changed after the credit card account is created.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "System-assigned key for the credit card vendor.",
                        "example": "122"
                      },
                      "id": {
                        "type": "string",
                        "description": "ID for the credit card vendor.",
                        "example": "Amex 1"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the credit card vendor.",
                        "readOnly": true,
                        "example": "/objects/accounts-payable/vendor/122"
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
                    "description": "Financial institution in Sage Intacct that the credit card is mapped to. Accounts are mapped to a financial institution record in Sage Intacct to manage multiple account logins for a bank feed.",
                    "type": "object",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "System-assigned key for the financial institution.",
                        "readOnly": true,
                        "example": "5"
                      },
                      "id": {
                        "type": "string",
                        "description": "ID for the financial institution.",
                        "readOnly": true,
                        "example": "5"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the financial institution.",
                        "readOnly": true,
                        "example": "/objects/cash-management/financial-institution/5"
                      }
                    },
                    "readOnly": true
                  },
                  "ruleSet": {
                    "type": "object",
                    "description": "Rule set to use during reconciliation. If the credit card will be reconciled using a bank feed, this rule set matches incoming bank transactions to Sage Intacct transactions.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "System-assigned key for the rule set.",
                        "example": "36"
                      },
                      "id": {
                        "type": "string",
                        "description": "ID for the rule set.",
                        "example": "36--RuleSetToMatch"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the rule set.",
                        "readOnly": true,
                        "example": "/objects/cash-management/bank-txn-rule-set/36"
                      }
                    }
                  }
                },
                "required": [
                  "id",
                  "vendor"
                ]
              },
              "examples": {
                "Create a credit card account": {
                  "value": {
                    "id": "V002",
                    "accountDetails": {
                      "description": "Card for employee expense",
                      "cardType": "visa",
                      "expirationMonth": "01",
                      "expirationYear": "2028",
                      "billingAddress": {
                        "addressLine1": "1295",
                        "addressLine2": null,
                        "addressLine3": "Adobe Ave",
                        "city": "San Jose",
                        "country": "United States",
                        "countryCode": "US",
                        "postCode": "978754",
                        "state": "CA"
                      },
                      "accountType": "credit",
                      "currency": "USD"
                    },
                    "status": "active",
                    "accounting": {
                      "offsetGLAccount": {
                        "id": "4564.44.44",
                        "key": "561"
                      },
                      "otherFeesGLAccount": {
                        "id": "0077",
                        "key": "432"
                      },
                      "defaultAccrualBasisGLJournal": {
                        "id": "DISB",
                        "key": "14"
                      },
                      "disableInterEntityTransfer": false
                    },
                    "vendor": {
                      "id": "First Security - Gold",
                      "key": "263"
                    },
                    "department": {
                      "id": "CHS--Channel Sales",
                      "key": "34"
                    },
                    "location": {
                      "id": "San Francisco"
                    },
                    "reconciliation": {
                      "matchSequence": {
                        "id": "48--bankseq-1"
                      },
                      "useMatchSequenceForAutoMatch": true,
                      "useMatchSequenceForManualMatch": true
                    },
                    "ruleSet": {
                      "key": "35",
                      "id": "35--creditcard"
                    }
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
                  "title": "New credit card account",
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
                  "Reference to new credit card account object": {
                    "value": {
                      "ia::result": {
                        "key": "34",
                        "id": "V002",
                        "href": "/objects/cash-management/credit-card-account/34"
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
            "name": "Create a credit card account",
            "source": "curl --request POST \\\n  --url https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account \\\n  --header 'Content-Type: application/json' \\\n  --data '{\"id\":\"V002\",\"accountDetails\":{\"description\":\"Card for employee expense\",\"cardType\":\"visa\",\"expirationMonth\":\"01\",\"expirationYear\":\"2028\",\"billingAddress\":{\"addressLine1\":\"1295\",\"addressLine2\":null,\"addressLine3\":\"Adobe Ave\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"978754\",\"state\":\"CA\"},\"accountType\":\"credit\",\"currency\":\"USD\"},\"status\":\"active\",\"accounting\":{\"offsetGLAccount\":{\"id\":\"4564.44.44\",\"key\":\"561\"},\"otherFeesGLAccount\":{\"id\":\"0077\",\"key\":\"432\"},\"defaultAccrualBasisGLJournal\":{\"id\":\"DISB\",\"key\":\"14\"},\"disableInterEntityTransfer\":false},\"vendor\":{\"id\":\"First Security - Gold\",\"key\":\"263\"},\"department\":{\"id\":\"CHS--Channel Sales\",\"key\":\"34\"},\"location\":{\"id\":\"San Francisco\"},\"reconciliation\":{\"matchSequence\":{\"id\":\"48--bankseq-1\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ruleSet\":{\"key\":\"35\",\"id\":\"35--creditcard\"}}'",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_httpie",
            "name": "Create a credit card account",
            "source": "echo '{\"id\":\"V002\",\"accountDetails\":{\"description\":\"Card for employee expense\",\"cardType\":\"visa\",\"expirationMonth\":\"01\",\"expirationYear\":\"2028\",\"billingAddress\":{\"addressLine1\":\"1295\",\"addressLine2\":null,\"addressLine3\":\"Adobe Ave\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"978754\",\"state\":\"CA\"},\"accountType\":\"credit\",\"currency\":\"USD\"},\"status\":\"active\",\"accounting\":{\"offsetGLAccount\":{\"id\":\"4564.44.44\",\"key\":\"561\"},\"otherFeesGLAccount\":{\"id\":\"0077\",\"key\":\"432\"},\"defaultAccrualBasisGLJournal\":{\"id\":\"DISB\",\"key\":\"14\"},\"disableInterEntityTransfer\":false},\"vendor\":{\"id\":\"First Security - Gold\",\"key\":\"263\"},\"department\":{\"id\":\"CHS--Channel Sales\",\"key\":\"34\"},\"location\":{\"id\":\"San Francisco\"},\"reconciliation\":{\"matchSequence\":{\"id\":\"48--bankseq-1\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ruleSet\":{\"key\":\"35\",\"id\":\"35--creditcard\"}}' |  \\\n  http POST https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account \\\n  Content-Type:application/json",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_wget",
            "name": "Create a credit card account",
            "source": "wget --quiet \\\n  --method POST \\\n  --header 'Content-Type: application/json' \\\n  --body-data '{\"id\":\"V002\",\"accountDetails\":{\"description\":\"Card for employee expense\",\"cardType\":\"visa\",\"expirationMonth\":\"01\",\"expirationYear\":\"2028\",\"billingAddress\":{\"addressLine1\":\"1295\",\"addressLine2\":null,\"addressLine3\":\"Adobe Ave\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"978754\",\"state\":\"CA\"},\"accountType\":\"credit\",\"currency\":\"USD\"},\"status\":\"active\",\"accounting\":{\"offsetGLAccount\":{\"id\":\"4564.44.44\",\"key\":\"561\"},\"otherFeesGLAccount\":{\"id\":\"0077\",\"key\":\"432\"},\"defaultAccrualBasisGLJournal\":{\"id\":\"DISB\",\"key\":\"14\"},\"disableInterEntityTransfer\":false},\"vendor\":{\"id\":\"First Security - Gold\",\"key\":\"263\"},\"department\":{\"id\":\"CHS--Channel Sales\",\"key\":\"34\"},\"location\":{\"id\":\"San Francisco\"},\"reconciliation\":{\"matchSequence\":{\"id\":\"48--bankseq-1\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ruleSet\":{\"key\":\"35\",\"id\":\"35--creditcard\"}}' \\\n  --output-document \\\n  - https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_xhr",
            "name": "Create a credit card account",
            "source": "const data = JSON.stringify({\n  \"id\": \"V002\",\n  \"accountDetails\": {\n    \"description\": \"Card for employee expense\",\n    \"cardType\": \"visa\",\n    \"expirationMonth\": \"01\",\n    \"expirationYear\": \"2028\",\n    \"billingAddress\": {\n      \"addressLine1\": \"1295\",\n      \"addressLine2\": null,\n      \"addressLine3\": \"Adobe Ave\",\n      \"city\": \"San Jose\",\n      \"country\": \"United States\",\n      \"countryCode\": \"US\",\n      \"postCode\": \"978754\",\n      \"state\": \"CA\"\n    },\n    \"accountType\": \"credit\",\n    \"currency\": \"USD\"\n  },\n  \"status\": \"active\",\n  \"accounting\": {\n    \"offsetGLAccount\": {\n      \"id\": \"4564.44.44\",\n      \"key\": \"561\"\n    },\n    \"otherFeesGLAccount\": {\n      \"id\": \"0077\",\n      \"key\": \"432\"\n    },\n    \"defaultAccrualBasisGLJournal\": {\n      \"id\": \"DISB\",\n      \"key\": \"14\"\n    },\n    \"disableInterEntityTransfer\": false\n  },\n  \"vendor\": {\n    \"id\": \"First Security - Gold\",\n    \"key\": \"263\"\n  },\n  \"department\": {\n    \"id\": \"CHS--Channel Sales\",\n    \"key\": \"34\"\n  },\n  \"location\": {\n    \"id\": \"San Francisco\"\n  },\n  \"reconciliation\": {\n    \"matchSequence\": {\n      \"id\": \"48--bankseq-1\"\n    },\n    \"useMatchSequenceForAutoMatch\": true,\n    \"useMatchSequenceForManualMatch\": true\n  },\n  \"ruleSet\": {\n    \"key\": \"35\",\n    \"id\": \"35--creditcard\"\n  }\n});\n\nconst xhr = new XMLHttpRequest();\nxhr.withCredentials = true;\n\nxhr.addEventListener(\"readystatechange\", function () {\n  if (this.readyState === this.DONE) {\n    console.log(this.responseText);\n  }\n});\n\nxhr.open(\"POST\", \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account\");\nxhr.setRequestHeader(\"Content-Type\", \"application/json\");\n\nxhr.send(data);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_jquery",
            "name": "Create a credit card account",
            "source": "const settings = {\n  \"async\": true,\n  \"crossDomain\": true,\n  \"url\": \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account\",\n  \"method\": \"POST\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  },\n  \"processData\": false,\n  \"data\": \"{\\\"id\\\":\\\"V002\\\",\\\"accountDetails\\\":{\\\"description\\\":\\\"Card for employee expense\\\",\\\"cardType\\\":\\\"visa\\\",\\\"expirationMonth\\\":\\\"01\\\",\\\"expirationYear\\\":\\\"2028\\\",\\\"billingAddress\\\":{\\\"addressLine1\\\":\\\"1295\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":\\\"Adobe Ave\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"978754\\\",\\\"state\\\":\\\"CA\\\"},\\\"accountType\\\":\\\"credit\\\",\\\"currency\\\":\\\"USD\\\"},\\\"status\\\":\\\"active\\\",\\\"accounting\\\":{\\\"offsetGLAccount\\\":{\\\"id\\\":\\\"4564.44.44\\\",\\\"key\\\":\\\"561\\\"},\\\"otherFeesGLAccount\\\":{\\\"id\\\":\\\"0077\\\",\\\"key\\\":\\\"432\\\"},\\\"defaultAccrualBasisGLJournal\\\":{\\\"id\\\":\\\"DISB\\\",\\\"key\\\":\\\"14\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"vendor\\\":{\\\"id\\\":\\\"First Security - Gold\\\",\\\"key\\\":\\\"263\\\"},\\\"department\\\":{\\\"id\\\":\\\"CHS--Channel Sales\\\",\\\"key\\\":\\\"34\\\"},\\\"location\\\":{\\\"id\\\":\\\"San Francisco\\\"},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"id\\\":\\\"48--bankseq-1\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"35\\\",\\\"id\\\":\\\"35--creditcard\\\"}}\"\n};\n\n$.ajax(settings).done(function (response) {\n  console.log(response);\n});",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "node_native",
            "name": "Create a credit card account",
            "source": "const http = require(\"https\");\n\nconst options = {\n  \"method\": \"POST\",\n  \"hostname\": \"api.intacct.com\",\n  \"port\": null,\n  \"path\": \"/ia/api/v1/objects/cash-management/credit-card-account\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  }\n};\n\nconst req = http.request(options, function (res) {\n  const chunks = [];\n\n  res.on(\"data\", function (chunk) {\n    chunks.push(chunk);\n  });\n\n  res.on(\"end\", function () {\n    const body = Buffer.concat(chunks);\n    console.log(body.toString());\n  });\n});\n\nreq.write(JSON.stringify({\n  id: 'V002',\n  accountDetails: {\n    description: 'Card for employee expense',\n    cardType: 'visa',\n    expirationMonth: '01',\n    expirationYear: '2028',\n    billingAddress: {\n      addressLine1: '1295',\n      addressLine2: null,\n      addressLine3: 'Adobe Ave',\n      city: 'San Jose',\n      country: 'United States',\n      countryCode: 'US',\n      postCode: '978754',\n      state: 'CA'\n    },\n    accountType: 'credit',\n    currency: 'USD'\n  },\n  status: 'active',\n  accounting: {\n    offsetGLAccount: {id: '4564.44.44', key: '561'},\n    otherFeesGLAccount: {id: '0077', key: '432'},\n    defaultAccrualBasisGLJournal: {id: 'DISB', key: '14'},\n    disableInterEntityTransfer: false\n  },\n  vendor: {id: 'First Security - Gold', key: '263'},\n  department: {id: 'CHS--Channel Sales', key: '34'},\n  location: {id: 'San Francisco'},\n  reconciliation: {\n    matchSequence: {id: '48--bankseq-1'},\n    useMatchSequenceForAutoMatch: true,\n    useMatchSequenceForManualMatch: true\n  },\n  ruleSet: {key: '35', id: '35--creditcard'}\n}));\nreq.end();",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_httpclient",
            "name": "Create a credit card account",
            "source": "var client = new HttpClient();\nvar request = new HttpRequestMessage\n{\n    Method = HttpMethod.Post,\n    RequestUri = new Uri(\"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account\"),\n    Content = new StringContent(\"{\\\"id\\\":\\\"V002\\\",\\\"accountDetails\\\":{\\\"description\\\":\\\"Card for employee expense\\\",\\\"cardType\\\":\\\"visa\\\",\\\"expirationMonth\\\":\\\"01\\\",\\\"expirationYear\\\":\\\"2028\\\",\\\"billingAddress\\\":{\\\"addressLine1\\\":\\\"1295\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":\\\"Adobe Ave\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"978754\\\",\\\"state\\\":\\\"CA\\\"},\\\"accountType\\\":\\\"credit\\\",\\\"currency\\\":\\\"USD\\\"},\\\"status\\\":\\\"active\\\",\\\"accounting\\\":{\\\"offsetGLAccount\\\":{\\\"id\\\":\\\"4564.44.44\\\",\\\"key\\\":\\\"561\\\"},\\\"otherFeesGLAccount\\\":{\\\"id\\\":\\\"0077\\\",\\\"key\\\":\\\"432\\\"},\\\"defaultAccrualBasisGLJournal\\\":{\\\"id\\\":\\\"DISB\\\",\\\"key\\\":\\\"14\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"vendor\\\":{\\\"id\\\":\\\"First Security - Gold\\\",\\\"key\\\":\\\"263\\\"},\\\"department\\\":{\\\"id\\\":\\\"CHS--Channel Sales\\\",\\\"key\\\":\\\"34\\\"},\\\"location\\\":{\\\"id\\\":\\\"San Francisco\\\"},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"id\\\":\\\"48--bankseq-1\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"35\\\",\\\"id\\\":\\\"35--creditcard\\\"}}\")\n    {\n        Headers =\n        {\n            ContentType = new MediaTypeHeaderValue(\"application/json\")\n        }\n    }\n};\nusing (var response = await client.SendAsync(request))\n{\n    response.EnsureSuccessStatusCode();\n    var body = await response.Content.ReadAsStringAsync();\n    Console.WriteLine(body);\n}",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_restsharp",
            "name": "Create a credit card account",
            "source": "var client = new RestClient(\"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account\");\nvar request = new RestRequest(Method.POST);\nrequest.AddHeader(\"Content-Type\", \"application/json\");\nrequest.AddParameter(\"application/json\", \"{\\\"id\\\":\\\"V002\\\",\\\"accountDetails\\\":{\\\"description\\\":\\\"Card for employee expense\\\",\\\"cardType\\\":\\\"visa\\\",\\\"expirationMonth\\\":\\\"01\\\",\\\"expirationYear\\\":\\\"2028\\\",\\\"billingAddress\\\":{\\\"addressLine1\\\":\\\"1295\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":\\\"Adobe Ave\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"978754\\\",\\\"state\\\":\\\"CA\\\"},\\\"accountType\\\":\\\"credit\\\",\\\"currency\\\":\\\"USD\\\"},\\\"status\\\":\\\"active\\\",\\\"accounting\\\":{\\\"offsetGLAccount\\\":{\\\"id\\\":\\\"4564.44.44\\\",\\\"key\\\":\\\"561\\\"},\\\"otherFeesGLAccount\\\":{\\\"id\\\":\\\"0077\\\",\\\"key\\\":\\\"432\\\"},\\\"defaultAccrualBasisGLJournal\\\":{\\\"id\\\":\\\"DISB\\\",\\\"key\\\":\\\"14\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"vendor\\\":{\\\"id\\\":\\\"First Security - Gold\\\",\\\"key\\\":\\\"263\\\"},\\\"department\\\":{\\\"id\\\":\\\"CHS--Channel Sales\\\",\\\"key\\\":\\\"34\\\"},\\\"location\\\":{\\\"id\\\":\\\"San Francisco\\\"},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"id\\\":\\\"48--bankseq-1\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"35\\\",\\\"id\\\":\\\"35--creditcard\\\"}}\", ParameterType.RequestBody);\nIRestResponse response = client.Execute(request);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_python3",
            "name": "Create a credit card account",
            "source": "import http.client\n\nconn = http.client.HTTPSConnection(\"api.intacct.com\")\n\npayload = \"{\\\"id\\\":\\\"V002\\\",\\\"accountDetails\\\":{\\\"description\\\":\\\"Card for employee expense\\\",\\\"cardType\\\":\\\"visa\\\",\\\"expirationMonth\\\":\\\"01\\\",\\\"expirationYear\\\":\\\"2028\\\",\\\"billingAddress\\\":{\\\"addressLine1\\\":\\\"1295\\\",\\\"addressLine2\\\":null,\\\"addressLine3\\\":\\\"Adobe Ave\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"978754\\\",\\\"state\\\":\\\"CA\\\"},\\\"accountType\\\":\\\"credit\\\",\\\"currency\\\":\\\"USD\\\"},\\\"status\\\":\\\"active\\\",\\\"accounting\\\":{\\\"offsetGLAccount\\\":{\\\"id\\\":\\\"4564.44.44\\\",\\\"key\\\":\\\"561\\\"},\\\"otherFeesGLAccount\\\":{\\\"id\\\":\\\"0077\\\",\\\"key\\\":\\\"432\\\"},\\\"defaultAccrualBasisGLJournal\\\":{\\\"id\\\":\\\"DISB\\\",\\\"key\\\":\\\"14\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"vendor\\\":{\\\"id\\\":\\\"First Security - Gold\\\",\\\"key\\\":\\\"263\\\"},\\\"department\\\":{\\\"id\\\":\\\"CHS--Channel Sales\\\",\\\"key\\\":\\\"34\\\"},\\\"location\\\":{\\\"id\\\":\\\"San Francisco\\\"},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"id\\\":\\\"48--bankseq-1\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"35\\\",\\\"id\\\":\\\"35--creditcard\\\"}}\"\n\nheaders = { 'Content-Type': \"application/json\" }\n\nconn.request(\"POST\", \"/ia/api/v1/objects/cash-management/credit-card-account\", payload, headers)\n\nres = conn.getresponse()\ndata = res.read()\n\nprint(data.decode(\"utf-8\"))",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_requests",
            "name": "Create a credit card account",
            "source": "import requests\n\nurl = \"https://api.intacct.com/ia/api/v1/objects/cash-management/credit-card-account\"\n\npayload = {\n    \"id\": \"V002\",\n    \"accountDetails\": {\n        \"description\": \"Card for employee expense\",\n        \"cardType\": \"visa\",\n        \"expirationMonth\": \"01\",\n        \"expirationYear\": \"2028\",\n        \"billingAddress\": {\n            \"addressLine1\": \"1295\",\n            \"addressLine2\": None,\n            \"addressLine3\": \"Adobe Ave\",\n            \"city\": \"San Jose\",\n            \"country\": \"United States\",\n            \"countryCode\": \"US\",\n            \"postCode\": \"978754\",\n            \"state\": \"CA\"\n        },\n        \"accountType\": \"credit\",\n        \"currency\": \"USD\"\n    },\n    \"status\": \"active\",\n    \"accounting\": {\n        \"offsetGLAccount\": {\n            \"id\": \"4564.44.44\",\n            \"key\": \"561\"\n        },\n        \"otherFeesGLAccount\": {\n            \"id\": \"0077\",\n            \"key\": \"432\"\n        },\n        \"defaultAccrualBasisGLJournal\": {\n            \"id\": \"DISB\",\n            \"key\": \"14\"\n        },\n        \"disableInterEntityTransfer\": False\n    },\n    \"vendor\": {\n        \"id\": \"First Security - Gold\",\n        \"key\": \"263\"\n    },\n    \"department\": {\n        \"id\": \"CHS--Channel Sales\",\n        \"key\": \"34\"\n    },\n    \"location\": {\"id\": \"San Francisco\"},\n    \"reconciliation\": {\n        \"matchSequence\": {\"id\": \"48--bankseq-1\"},\n        \"useMatchSequenceForAutoMatch\": True,\n        \"useMatchSequenceForManualMatch\": True\n    },\n    \"ruleSet\": {\n        \"key\": \"35\",\n        \"id\": \"35--creditcard\"\n    }\n}\nheaders = {\"Content-Type\": \"application/json\"}\n\nresponse = requests.request(\"POST\", url, json=payload, headers=headers)\n\nprint(response.text)",
            "mimeType": "application/json",
            "isAutoGenerated": true
          }
        ]
      }
    }
  },
  "components": {
    "schemas": {
      "objects.cash-management.credit-card-account": {
        "type": "object",
        "description": "Credit card accounts are used to record transactions made outside of Sage Intacct. Credit card accounts include credit and debit payment method accounts.",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the credit card account.",
            "readOnly": true,
            "example": "10"
          },
          "id": {
            "type": "string",
            "description": "Name or other unique identifier for the credit card account. The account ID cannot be modified.",
            "example": "Card101"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the credit card account.",
            "readOnly": true,
            "example": "/objects/cash-management/credit-card-account/10"
          },
          "accountDetails": {
            "type": "object",
            "description": "Credit/debit card account details.",
            "properties": {
              "description": {
                "type": "string",
                "description": "Optional description about how this card is used.",
                "example": "Travel Visa card 1"
              },
              "cardType": {
                "type": "string",
                "description": "Specifies the card type. The card type cannot be changed after the account is created.",
                "enum": [
                  "visa",
                  "mastercard",
                  "discover",
                  "americanExpress",
                  "dinersClub",
                  "otherChargeCard"
                ],
                "example": "visa"
              },
              "number": {
                "type": "string",
                "description": "Required only if the `cardType` is `americanExpress`. This is a 16-digit card number without spaces or hyphens.",
                "example": "xxxxxxxxxxxx1111"
              },
              "accountType": {
                "type": "string",
                "description": "Account type, credit or debit. Credit cards require an associated vendor and offset GL account. Debit cards used to pay bills require an associated checking account and vendor. \n",
                "enum": [
                  "credit",
                  "debit"
                ],
                "example": "credit"
              },
              "expirationMonth": {
                "type": "string",
                "description": "Month when the credit card expires.",
                "enum": [
                  "01",
                  "02",
                  "03",
                  "04",
                  "05",
                  "06",
                  "07",
                  "08",
                  "09",
                  "10",
                  "11",
                  "12"
                ],
                "example": "11"
              },
              "expirationYear": {
                "type": "string",
                "description": "Year when the credit card expires.",
                "example": "2032"
              },
              "currency": {
                "type": "string",
                "description": "Card currency",
                "readOnly": true,
                "example": "USD"
              },
              "billingAddress": {
                "type": "object",
                "description": "Billing address for the credit card.",
                "properties": {
                  "addressLine1": {
                    "type": "string",
                    "description": "Street address",
                    "example": "300 Park Ave"
                  },
                  "addressLine2": {
                    "type": "string",
                    "description": "Suite or unit number",
                    "example": "1400"
                  },
                  "addressLine3": {
                    "type": "string",
                    "description": "Address line 3",
                    "example": "Western industrial area"
                  },
                  "city": {
                    "type": "string",
                    "description": "City",
                    "example": "San Jose"
                  },
                  "state": {
                    "type": "string",
                    "description": "State",
                    "example": "CA"
                  },
                  "postCode": {
                    "type": "string",
                    "description": "Zip or postal code",
                    "example": "10001"
                  },
                  "country": {
                    "type": "string",
                    "description": "Country",
                    "example": "USA"
                  },
                  "countryCode": {
                    "type": "string",
                    "description": "ISO country code. When ISO country codes are enabled for a company, both `country` and `countryCode` must be provided.",
                    "example": "US"
                  }
                }
              },
              "debitCardCheckingAccount": {
                "description": "For credit card accounts defined as a debit card, the checking account linked to the debit card.",
                "type": "object",
                "properties": {
                  "id": {
                    "type": "string",
                    "description": "ID for the checking account.",
                    "example": "BOA"
                  },
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the checking account.",
                    "readOnly": true,
                    "example": "10"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the checking account.",
                    "readOnly": true,
                    "example": "/objects/cash-management/checking-account/10"
                  }
                },
                "readOnly": true
              }
            }
          },
          "accounting": {
            "type": "object",
            "description": "Accounting information for the credit card account.",
            "properties": {
              "offsetGLAccount": {
                "type": "object",
                "description": "Credit card offset GL account, which is the offset account used to track the credit card liability.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key of the offset GL account.",
                    "example": "155"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the offset GL account.",
                    "example": "11000"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the offset GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/155"
                  }
                }
              },
              "financeChargeGLAccount": {
                "type": "object",
                "description": "GL account to use for finance charges and other fees during reconciliation if those fees are to be tracked separately.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the GL account.",
                    "example": "201"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the GL account.",
                    "example": "4562.67"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/201"
                  }
                }
              },
              "financeChargeAPAccountLabel": {
                "type": "object",
                "description": "Finance charges AP account label.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the AP account label.",
                    "example": "22"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the AP account label.",
                    "example": "Finance Charges"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the AP account label.",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/account-label/22"
                  }
                }
              },
              "otherFeesGLAccount": {
                "type": "object",
                "description": "Other fees GL account used only for credit card reconciliation; does not apply to debit cards.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the GL account.",
                    "example": "33"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the GL account.",
                    "example": "3556.1"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/33"
                  }
                }
              },
              "otherFeesAPAccountLabel": {
                "type": "object",
                "description": "Other fees AP account label.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the AP account label.",
                    "example": "5"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the AP account label.",
                    "example": "Other fees"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the AP account label.",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/account-label/5"
                  }
                }
              },
              "defaultAccrualBasisGLJournal": {
                "type": "object",
                "description": "Default accrual basis journal.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the GL journal.",
                    "example": "13"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the GL journal.",
                    "example": "GJ"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL journal.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/journal/13"
                  }
                }
              },
              "defaultCashBasisGLJournal": {
                "type": "object",
                "description": "Default cash basis journal. For dual-method reporting, separate accrual and cash journals can be specified.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the GL journal.",
                    "example": "33"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the GL journal.",
                    "example": "IJ"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL journal.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/journal/33"
                  }
                }
              },
              "employeeExpenseGLAccount": {
                "type": "object",
                "description": "GL account to use for employee expense.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the GL account.",
                    "example": "201"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the GL account.",
                    "example": "4562.67"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/201"
                  }
                }
              },
              "employeeExpenseAccountLabel": {
                "type": "object",
                "description": "Employee expense AP account label.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the AP account label.",
                    "example": "22"
                  },
                  "id": {
                    "type": "string",
                    "description": "ID for the AP account label.",
                    "example": "Employee Expense"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the AP account label.",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/account-label/22"
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
                "description": "Set to `true` to disable inter-entity transfers.",
                "default": false,
                "example": false
              },
              "useInEmployeeExpense": {
                "type": "boolean",
                "description": "Set to `true` to use this credit card account for employee expense.",
                "default": false,
                "example": false
              }
            }
          },
          "reconciliation": {
            "type": "object",
            "description": "Reconciliation information for the credit card account; does not apply to debit card accounts.",
            "properties": {
              "lastReconciledBalance": {
                "type": "string",
                "description": "If the account was previously reconciled, this is the balance of that reconciliation.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "110000.00"
              },
              "lastReconciledDate": {
                "type": "string",
                "format": "date",
                "description": "If the account was previously reconciled, this is the date of that reconciliation.",
                "readOnly": true,
                "example": "2024-04-15"
              },
              "cutOffDate": {
                "type": "string",
                "format": "date",
                "description": "The date after which the first reconciliation can begin.",
                "readOnly": true,
                "example": "2023-07-31"
              },
              "inProgressBalance": {
                "type": "string",
                "description": "For reconciliations in progress, the current reconciliation balance.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "160207.75"
              },
              "inProgressDate": {
                "type": "string",
                "format": "date",
                "description": "For reconciliations in progress, the date of that reconciliation.",
                "readOnly": true,
                "example": "2024-04-21"
              },
              "matchSequence": {
                "type": "object",
                "description": "Reconciliation match sequence. This is a document sequence that tracks matches in reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned key for the document sequence number.",
                    "example": "2",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Document sequence ID",
                    "example": "2--Bank sequence Id",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the sequence number.",
                    "readOnly": true,
                    "example": "/objects/company-config/document-sequence/2",
                    "nullable": true
                  }
                }
              },
              "useMatchSequenceForAutoMatch": {
                "type": "boolean",
                "default": true,
                "description": "Use sequence number for transactions that were matched automatically with a rule set.",
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
            "description": "Default department to associate with this card account.",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the department.",
                "example": "11"
              },
              "id": {
                "type": "string",
                "description": "ID for the department.",
                "example": "8"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the department.",
                "readOnly": true,
                "example": "/objects/company-config/department/11"
              }
            }
          },
          "location": {
            "type": "object",
            "description": "Default location for transactions that draw on this account.",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the location.",
                "example": "5"
              },
              "id": {
                "type": "string",
                "description": "ID for the location.",
                "example": "CA"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the location.",
                "readOnly": true,
                "example": "/objects/company-config/location/6"
              }
            }
          },
          "vendor": {
            "type": "object",
            "description": "The vendor is the credit card provider. Associate the credit card with a vendor to pay off the credit card in accounts payable. All credit card charges and payments go to the ledger for this vendor. Use a unique vendor for each credit card account. The vendor cannot be changed after the credit card account is created.",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the credit card vendor.",
                "example": "122"
              },
              "id": {
                "type": "string",
                "description": "ID for the credit card vendor.",
                "example": "Amex 1"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the credit card vendor.",
                "readOnly": true,
                "example": "/objects/accounts-payable/vendor/122"
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
            "description": "Financial institution in Sage Intacct that the credit card is mapped to. Accounts are mapped to a financial institution record in Sage Intacct to manage multiple account logins for a bank feed.",
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the financial institution.",
                "readOnly": true,
                "example": "5"
              },
              "id": {
                "type": "string",
                "description": "ID for the financial institution.",
                "readOnly": true,
                "example": "5"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the financial institution.",
                "readOnly": true,
                "example": "/objects/cash-management/financial-institution/5"
              }
            },
            "readOnly": true
          },
          "ruleSet": {
            "type": "object",
            "description": "Rule set to use during reconciliation. If the credit card will be reconciled using a bank feed, this rule set matches incoming bank transactions to Sage Intacct transactions.",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the rule set.",
                "example": "36"
              },
              "id": {
                "type": "string",
                "description": "ID for the rule set.",
                "example": "36--RuleSetToMatch"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the rule set.",
                "readOnly": true,
                "example": "/objects/cash-management/bank-txn-rule-set/36"
              }
            }
          }
        }
      },
      "cash-management-credit-card-accountRequiredProperties": {
        "type": "object",
        "required": [
          "id",
          "vendor"
        ],
        "properties": {
          "accountDetails": {
            "type": "object",
            "required": [
              "accountType",
              "cardType",
              "expirationMonth",
              "expirationYear"
            ]
          },
          "accounting": {
            "type": "object",
            "required": [
              "offsetGLAccount"
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
