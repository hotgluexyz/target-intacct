```json
{
  "openapi": "3.0.3",
  "info": {
    "title": "Create a checking account",
    "version": "1",
    "description": "Creates a new checking account."
  },
  "servers": [
    {
      "url": "https://api.intacct.com/ia/api/v1",
      "x-try-it": "sage-intacct-api"
    }
  ],
  "paths": {
    "/objects/cash-management/checking-account": {
      "post": {
        "summary": "Create a checking account",
        "description": "Creates a new checking account.",
        "tags": [
          "Cash_Management_Checking accounts"
        ],
        "operationId": "post-objects-checking-account",
        "requestBody": {
          "description": "Create a new checking account",
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "description": "A checking account represents a specific type of cash account used to manage day-to-day transactions, such as vendor payments, customer deposits, payroll and reconciliations.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "System-assigned  unique key for the account.",
                    "readOnly": true,
                    "example": "2"
                  },
                  "id": {
                    "type": "string",
                    "description": "Unique identifier for the account.",
                    "example": "BOA"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the checking account",
                    "readOnly": true,
                    "example": "/objects/cash-management/checking-account/62"
                  },
                  "bankAccountDetails": {
                    "type": "object",
                    "description": "Bank account details for the account.",
                    "properties": {
                      "accountNumber": {
                        "type": "string",
                        "description": "Bank account number for the account.",
                        "example": "4356789402",
                        "nullable": true
                      },
                      "bankName": {
                        "type": "string",
                        "description": "Bank name for the account.",
                        "example": "Bank of America"
                      },
                      "accountHolderName": {
                        "type": "string",
                        "description": "Account holder name for the account. This is the official name that the bank has on file.",
                        "maxLength": 100,
                        "example": "ABC Software",
                        "nullable": true
                      },
                      "routingNumber": {
                        "type": "string",
                        "description": "Routing number for the account, required for issuing payments from the account, regardless of the payment method.",
                        "example": "121000358",
                        "nullable": true
                      },
                      "branchId": {
                        "type": "string",
                        "description": "Identifier for the branch associated with the checking account.",
                        "example": "89099",
                        "nullable": true
                      },
                      "phoneNumber": {
                        "type": "string",
                        "description": "Phone number for the branch associated with the checking account.",
                        "example": "5559878978",
                        "nullable": true
                      },
                      "currency": {
                        "type": "string",
                        "description": "Currency for the account. The default is the base currency for the company or entity. If the account is with a foreign bank, the currency should match the country.",
                        "example": "USD"
                      },
                      "bankAddress": {
                        "type": "object",
                        "properties": {
                          "city": {
                            "type": "string",
                            "description": "City for the branch associated with the checking account.",
                            "example": "Newark",
                            "nullable": true
                          },
                          "state": {
                            "type": "string",
                            "description": "State for the branch associated with the checking account.",
                            "example": "CA",
                            "nullable": true
                          },
                          "postCode": {
                            "type": "string",
                            "description": "Zip or postal code for the branch associated with the checking account.",
                            "example": "94560",
                            "nullable": true
                          },
                          "country": {
                            "type": "string",
                            "description": "Country for the branch associated with the checking account.",
                            "example": "United States",
                            "nullable": true
                          },
                          "addressLine1": {
                            "type": "string",
                            "description": "First line of the street for the branch associated with the checking account.",
                            "example": "36900 Neward Blvd",
                            "nullable": true
                          },
                          "addressLine2": {
                            "type": "string",
                            "description": "Second line of the street for the branch associated with the checking account.",
                            "example": "Suite 101",
                            "nullable": true
                          },
                          "addressLine3": {
                            "type": "string",
                            "description": "Third line of the street for the branch associated with the checking account.",
                            "example": "Western Industrial Area",
                            "nullable": true
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
                    "description": "Specifies the accounting details for the checking account.",
                    "properties": {
                      "glAccount": {
                        "type": "object",
                        "description": "General Ledger (GL) account associated with the checking account.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the GL account.",
                            "example": "256"
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the GL account.",
                            "example": "9899 Expense GL Account 33"
                          },
                          "name": {
                            "type": "string",
                            "description": "Name for the GL account.",
                            "readOnly": true,
                            "example": "Expense Account"
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/256"
                          }
                        }
                      },
                      "apJournal": {
                        "type": "object",
                        "description": "Specifies the default General Ledger (GL) journal for Accounts Payable (AP).",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the AP journal.",
                            "example": "3",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the AP journal.",
                            "example": "AP-ADJ AP Adjustment Journal",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the AP journal.",
                            "example": "/objects/general-ledger/journal/3",
                            "readOnly": true,
                            "nullable": true
                          }
                        }
                      },
                      "arJournal": {
                        "type": "object",
                        "description": "Specifies the default General Ledger (GL) journal for Accounts Receivable (AR).",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the AR journal.",
                            "example": "3",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the AR journal.",
                            "example": "AR-ADJ AR Adjustment Journal",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the AR journal.",
                            "example": "/objects/general-ledger/journal/3",
                            "readOnly": true,
                            "nullable": true
                          }
                        }
                      },
                      "disableInterEntityTransfer": {
                        "type": "boolean",
                        "description": "Excludes the checking account from inter-entity transfers (IET) even if IET is globally enabled for the entire multi-entity shared structure of companies.",
                        "default": false,
                        "example": false
                      },
                      "serviceChargeGLAccount": {
                        "type": "object",
                        "description": "Specifies the General Ledger (GL) journal for service charges, used for reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the service charge GL account.",
                            "example": "432",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the service charge GL account.",
                            "example": "0077  Service Charge GL Account 54",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the service charge GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/432",
                            "nullable": true
                          }
                        }
                      },
                      "serviceChargeAccountLabel": {
                        "type": "object",
                        "description": "Specifies the account label for the service charge GL account.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the service charge GL account label.",
                            "example": "15",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the service charge GL account label.",
                            "example": "Car Payment",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the service charge GL account label.",
                            "readOnly": true,
                            "example": "/objects/accounts-payable/account-label/15",
                            "nullable": true
                          }
                        }
                      },
                      "interestGLAccount": {
                        "type": "object",
                        "description": "Specifies the General Ledger (GL) journal for earned interest, used for reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the earned interest GL account.",
                            "example": "419",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the earned interest GL account.",
                            "example": "0099 Interest Earned GL Account 40",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the earned interest GL account.",
                            "readOnly": true,
                            "example": "/objects/general-ledger/account/419",
                            "nullable": true
                          }
                        }
                      },
                      "interestAccountLabel": {
                        "type": "object",
                        "description": "Specifies the account label for the earned interest GL account.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the earned interest GL account label.",
                            "example": "35",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the earned interest GL account label.",
                            "example": "Sales Account",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the earned interest GL account label.",
                            "readOnly": true,
                            "example": "/objects/accounts-receivable/account-label/35",
                            "nullable": true
                          }
                        }
                      },
                      "bankingTimeZone": {
                        "type": "string",
                        "description": "Determines the time stamp for transactions generated from creation rules and incoming bank feed transactions.",
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
                      }
                    },
                    "required": [
                      "glAccount"
                    ]
                  },
                  "reconciliation": {
                    "type": "object",
                    "description": "Specifies the reconciliation details for the account.",
                    "properties": {
                      "lastReconciledBalance": {
                        "type": "string",
                        "description": "Balance of the last reconciliation.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "8970.98",
                        "nullable": true
                      },
                      "lastReconciledDate": {
                        "type": "string",
                        "format": "date",
                        "description": "Date the last reconciliation occurred.",
                        "readOnly": true,
                        "nullable": true,
                        "example": "2019-03-22"
                      },
                      "cutOffDate": {
                        "type": "string",
                        "format": "date",
                        "description": "Date after which the initial reconciliation can begin. Applies only to accounts not previously reconciled in Sage Intacct.",
                        "readOnly": true,
                        "example": "2018-01-01",
                        "nullable": true
                      },
                      "inProgressBalance": {
                        "type": "string",
                        "description": "Balance of the in-progress reconciliation.",
                        "format": "decimal-precision-2",
                        "readOnly": true,
                        "example": "-221021.61",
                        "nullable": true
                      },
                      "inProgressDate": {
                        "type": "string",
                        "format": "date",
                        "description": "Date the in-progress reconciliation occurred.",
                        "readOnly": true,
                        "example": "2023-01-05",
                        "nullable": true
                      },
                      "matchSequence": {
                        "type": "object",
                        "description": "Reconciliation match sequence, a document sequence that tracks matches in reconciliation.",
                        "properties": {
                          "key": {
                            "type": "string",
                            "description": "Unique key for the match sequence.",
                            "example": "2",
                            "nullable": true
                          },
                          "id": {
                            "type": "string",
                            "description": "Identifier for the match sequence.",
                            "example": "0022 CHASESQ 0033",
                            "nullable": true
                          },
                          "href": {
                            "type": "string",
                            "description": "URL endpoint for the match sequence.",
                            "readOnly": true,
                            "example": "/objects/company-config/document-sequence/2",
                            "nullable": true
                          }
                        }
                      },
                      "useMatchSequenceForAutoMatch": {
                        "type": "boolean",
                        "default": true,
                        "description": "Indicates whether to use sequence number for automatically matched transactions.",
                        "example": false
                      },
                      "useMatchSequenceForManualMatch": {
                        "type": "boolean",
                        "default": true,
                        "description": "Indicates whether to use sequence number for manually matched transactions.",
                        "example": false
                      }
                    }
                  },
                  "checkPrinting": {
                    "type": "object",
                    "properties": {
                      "disablePrinting": {
                        "type": "boolean",
                        "default": false,
                        "description": "Indicates whether to disable check printing for the account.",
                        "example": false
                      },
                      "addressSettings": {
                        "type": "object",
                        "description": "Defines the address settings for check printing.",
                        "properties": {
                          "printAddress": {
                            "type": "boolean",
                            "default": false,
                            "description": "Indicates whether to print an address on checks:\n\n  - `true` - Prints the address on checks.\n  - `false` - Does not print the address on checks. Set to `false` if you don't want to include an address or when using pre-printed check stock that already includes the address.\n",
                            "example": false
                          },
                          "addressToPrint": {
                            "type": "string",
                            "description": "Specifies the address to print on checks:\n\n  - `company` - Uses the address set for the company.\n  - `custom` - Uses the address defined in the `name` and `address` fields.\n",
                            "enum": [
                              null,
                              "company",
                              "custom"
                            ],
                            "nullable": true,
                            "default": null,
                            "example": "company"
                          },
                          "name": {
                            "type": "string",
                            "description": "Specifies the company name to print on checks from this checking account if `addressToPrint` is set to `custom`.",
                            "maxLength": 100,
                            "example": "Zine Inc.",
                            "nullable": true
                          },
                          "address": {
                            "type": "object",
                            "description": "Specifies the address to print on checks from this checking account if `addressToPrint` is set to `custom`.",
                            "properties": {
                              "addressLine1": {
                                "type": "string",
                                "description": "First line of the street to print on qualifying checks.",
                                "example": "75688 Post st",
                                "nullable": true
                              },
                              "addressLine2": {
                                "type": "string",
                                "description": "Second line of the street to print on qualifying checks.",
                                "example": "East Gwalopak",
                                "nullable": true
                              },
                              "addressLine3": {
                                "type": "string",
                                "description": "Third line of the street to print on qualifying checks.",
                                "example": "456",
                                "nullable": true
                              },
                              "city": {
                                "type": "string",
                                "description": "City to print on qualifying checks.",
                                "example": "San Ramon",
                                "nullable": true
                              },
                              "state": {
                                "type": "string",
                                "description": "State to print on qualifying checks.",
                                "example": "CA",
                                "nullable": true
                              },
                              "postCode": {
                                "type": "string",
                                "description": "Zip or postal code to print on qualifying checks.",
                                "example": "94536",
                                "nullable": true
                              },
                              "country": {
                                "type": "string",
                                "description": "Country to print on qualifying checks.",
                                "example": "United States",
                                "nullable": true
                              },
                              "countryCode": {
                                "type": "string",
                                "description": "ISO country code to print on qualifying checks. When ISO country codes are enabled for a company, both `country` and `countryCode` must be provided.",
                                "example": "US",
                                "nullable": true
                              },
                              "phone": {
                                "type": "string",
                                "description": "Phone number to print on qualifying checks.",
                                "maxLength": 30,
                                "example": "6609336532",
                                "nullable": true
                              }
                            }
                          },
                          "printLogo": {
                            "type": "boolean",
                            "default": false,
                            "description": "Indicates whether to print the company logo on qualifying checks. Requires a logo image file to be uploaded in Sage Intacct.\n\nFor more information, read about [adding logos to checks](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Check_logos) in the Sage Intacct Help Center.\n",
                            "example": false
                          }
                        }
                      },
                      "signatures": {
                        "type": "object",
                        "description": "Defines the uploaded signature images to print on qualifying checks for the account. By default, signatures are not included on blank or preprinted check stock. \n\nFor more information, read about [check signatures](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Uploading_Your_Check_Signature) in the Sage Intacct Help Center.\n",
                        "properties": {
                          "firstSignature": {
                            "type": "string",
                            "description": "Image file name for the first signature.",
                            "example": "sigimg1_j.jpg",
                            "nullable": true
                          },
                          "limitForFirstSignatureAmount": {
                            "type": "string",
                            "format": "decimal-precision-2",
                            "description": "Specifies the limit as an amount for printing the first signature on qualifying checks. The first signature is printed on checks for less than this amount, while a manual signature line is printed on checks for more than this amount, or when this field is `null`.\n",
                            "example": "50.00",
                            "nullable": true
                          },
                          "useSecondSignature": {
                            "type": "boolean",
                            "default": false,
                            "description": "Indicates whether to include second signature on qualifying checks. Set to `false` to always show just one signature.",
                            "example": true
                          },
                          "secondSignature": {
                            "type": "string",
                            "description": "Image file name for the second signature.",
                            "example": "sigimg2_j.jpg",
                            "nullable": true
                          },
                          "limitForSecondSignatureAmount": {
                            "type": "string",
                            "format": "decimal-precision-2",
                            "description": "Specifies the limit as an amount for printing the second signature on qualifying checks. The second signature is printed on checks for less than this this amount, while a manual signature line is printed on checks for more than this amount, or when this field is `null`. Applies when `useSecondSignature` is set to `true`.\n",
                            "example": "60.00",
                            "nullable": true
                          },
                          "thresholdForSecondSignatureAmount": {
                            "type": "string",
                            "format": "decimal-precision-2",
                            "description": "Specifies the threshold as an amount for printing the second signature on qualifying checks. The second signature is printed on checks for more than this amount.",
                            "example": "60.00",
                            "nullable": true
                          }
                        }
                      },
                      "printSettings": {
                        "type": "object",
                        "description": "Specifies the check printing settings for the account.",
                        "properties": {
                          "printOn": {
                            "type": "string",
                            "description": "Indicates the check stock to use for printing checks for the account:  \n\n- `prePrintedCheckStock` - Uses check paper that is pre-printed with company information.\n- `blankCheckStock` - Uses blank check paper.\n",
                            "enum": [
                              "prePrintedCheckStock",
                              "blankCheckStock"
                            ],
                            "example": "blankCheckStock",
                            "default": "blankCheckStock"
                          },
                          "nextCheckNumber": {
                            "type": "string",
                            "description": "Specifies the starting check number to use when printing checks for the account, for example 1001. Check numbers increment by one for each succeeding check printed.\n",
                            "maxLength": 10,
                            "pattern": "^[0-9]{1,10}$",
                            "example": "1012",
                            "nullable": true
                          },
                          "printingFormat": {
                            "type": "string",
                            "description": "Specifies the format to use when printing checks for the account:\n\n\n\n\n\n  - `standard` - For pre-printed checks that already show the bank account number, routing number, and check numbers. Not available for CAD checking accounts.\n  - `business` - Prints amounts in a font that makes alterations difficult (for security).\n  - `highSecurity` - Same as `standard`, plus features that reduce fraud related to check washing, forgery, and copying. Not available for CAD checking accounts.\n  - `cadCheck` - Prints checks with dates formatted for Canadian companies, can be used with CAD and USD checking accounts.\n  - `jpmorganChaseBusiness` - For USD checking accounts with business checks where the Pay to the order of field is not above the Amount field but is next to the Vendor address.\n  - `jpmorganChaseStandard` - For USD checking accounts with standard checks where the Pay to the order of field is not above the Amount field but is next to the Vendor address.\n",
                            "enum": [
                              "standard",
                              "business",
                              "highSecurity",
                              "cadCheck",
                              "jpmorganChaseBusiness",
                              "jpmorganChaseStandard"
                            ],
                            "example": "standard",
                            "default": "standard"
                          },
                          "paperFormat": {
                            "type": "string",
                            "description": "Specifies the location for check printing on three-part forms: top, middle, or bottom panel. Pre-printed check stock is only compatible with the top and middle printing position.\n\nCanadian check stock is only compatible with the top printing position.\n",
                            "enum": [
                              "top",
                              "middle",
                              "bottom"
                            ],
                            "example": "top",
                            "default": "top"
                          },
                          "printLineItems": {
                            "type": "boolean",
                            "description": "Indicates whether to include additional fields in the non-remittance panel of checks. These fields include columns for the account, department, and location of each line item. A check can include up to 18 line items per page in either summary or detail mode.\n",
                            "default": false,
                            "example": true
                          },
                          "printLocation": {
                            "type": "string",
                            "description": "Specifies the location information to include on checks:\n\n  - `id` - Print the location identifier only in the location column.\n  - `name` - Print the location name only in the location column.\n  - `both`(default) - Print both the location identifier and the location name in the location column.\n",
                            "enum": [
                              "id",
                              "name",
                              "both"
                            ],
                            "example": "id",
                            "default": "id"
                          },
                          "additionalText": {
                            "type": "string",
                            "description": "Specifies additional text to print under the signatures.",
                            "example": "Pay to check holder",
                            "nullable": true
                          },
                          "numberOfChecksInPreview": {
                            "type": "string",
                            "description": "Specifies the number of checks per page to preview before printing.",
                            "enum": [
                              null,
                              "one",
                              "three"
                            ],
                            "example": "one",
                            "nullable": true,
                            "default": null
                          }
                        }
                      },
                      "micrSettings": {
                        "type": "object",
                        "description": "Specifies the Magnetic Ink Character Recognition (MICR) settings. MICR format is a widely adopted bank standard for blank check stock, standardizing the appearance of the routing, account, and other numbers at the bottom of every check. Use these settings if your bank requires specific horizontal alignment of the account number on the MICR line on the printed check. \n\nFor more information, read the [MICR printing guidelines](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=MICR_information_on_checks) in the Sage Intacct Help Center.\n",
                        "properties": {
                          "accountNumberAlignment": {
                            "type": "string",
                            "description": "Specifies how the bank account number is aligned on the MICR line.",
                            "enum": [
                              "left",
                              "right"
                            ],
                            "example": "right",
                            "default": "right"
                          },
                          "accountNumberPositioning": {
                            "type": "integer",
                            "description": "Specifies the positioning of the bank account number on the MICR line, defined by the number of spaces added before or after the account number.",
                            "example": 1,
                            "nullable": true
                          },
                          "minCheckNumberLength": {
                            "type": "string",
                            "description": "Specifies the required length for check numbers on the MICR line. Minimum check number length is six digits, shorter check numbers are left-padded with zeros.\n",
                            "example": "6",
                            "nullable": true
                          },
                          "regionalSettings": {
                            "type": "object",
                            "description": "Specifies the regional settings for MICR.",
                            "properties": {
                              "printCode45": {
                                "type": "boolean",
                                "default": false,
                                "description": "Indicates whether to print the transaction code 45 on the MICR line.",
                                "example": true
                              },
                              "printUSFundsUnderCheckAmount": {
                                "type": "boolean",
                                "default": false,
                                "description": "Indicates whether to print US funds under the check amount box for CPA member banks.",
                                "example": true
                              },
                              "printOnUsSymbol": {
                                "type": "boolean",
                                "default": false,
                                "description": "Indicates whether to print the ON-US symbol in front of the account number on the MICR line.",
                                "example": true
                              },
                              "positionOfOnUsSymbol": {
                                "type": "string",
                                "description": "Specifies the position of the ON-US symbol on the MICR line. To position the symbol before the checking account number, set to `position31` or `position32`.",
                                "enum": [
                                  "position31",
                                  "position32"
                                ],
                                "example": "position31",
                                "default": "position31"
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "department": {
                    "type": "object",
                    "description": "Specifies the department to use for General Ledger (GL) posting (optional).",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the department.",
                        "example": "9",
                        "nullable": true
                      },
                      "id": {
                        "type": "string",
                        "description": "Identifier for the department.",
                        "example": "11-Accounting",
                        "nullable": true
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the department.",
                        "readOnly": true,
                        "example": "/objects/company-config/department/9",
                        "nullable": true
                      }
                    }
                  },
                  "location": {
                    "type": "object",
                    "description": "Specifies the location to use for General Ledger (GL) posting (optional).",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the location.",
                        "example": "1"
                      },
                      "id": {
                        "type": "string",
                        "description": "Identifier for the location.",
                        "example": "001-United States of America"
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the location.",
                        "readOnly": true,
                        "example": "/objects/company-config/location/1"
                      }
                    }
                  },
                  "status": {
                    "$ref": "#/components/schemas/status"
                  },
                  "ach": {
                    "type": "object",
                    "description": "Specifies the Automated Clearing House (ACH) details.\n\nFor more information, read about [setting up a checking account for ACH payments](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_account_setup) in Sage Intacct Help Center.\n",
                    "properties": {
                      "enableACH": {
                        "type": "boolean",
                        "description": "Indicates whether to to enable the account to make standard ACH or American Express ACH Payment Services payments.",
                        "default": false,
                        "example": true
                      },
                      "bankId": {
                        "type": "string",
                        "description": "Identifier for the bank, as specified in the ACH bank record.",
                        "example": "BOA_ACH",
                        "nullable": true
                      },
                      "companyName": {
                        "type": "string",
                        "description": "Name of the company, as specified in the ACH bank record.",
                        "maxLength": 16,
                        "example": "Ventura",
                        "nullable": true
                      },
                      "companyIdentification": {
                        "type": "string",
                        "description": "Specifies the 10-digit identifier (including hyphens) for the company, as specified in the ACH bank record.",
                        "maxLength": 10,
                        "pattern": "^[\\w\\s_\\-\\.]{0,20}$",
                        "example": "Ventura",
                        "nullable": true
                      },
                      "originatingFinancialInstitution": {
                        "type": "string",
                        "description": "References the first eight digits of the routing number for the bank, as specified in the ACH bank record.",
                        "maxLength": 8,
                        "pattern": "^[0-9]{1,8}$",
                        "example": "89096789",
                        "nullable": true
                      },
                      "companyEntryDescription": {
                        "type": "string",
                        "description": "Indicates optional text that can be included with ACH payments.",
                        "maxLength": 10,
                        "example": "Investment",
                        "nullable": true
                      },
                      "companyDiscretionaryData": {
                        "type": "string",
                        "description": "Specifies additional information that can be included with ACH payments. Typically this will consist of codes (unique to each bank) that describe any special handling of entries.",
                        "maxLength": 20,
                        "example": "89078900",
                        "nullable": true
                      },
                      "useRecommendedSetup": {
                        "type": "boolean",
                        "description": "Indicates whether to automatically generate the ACH payment file, with `serviceClassCode` set to `220` (credits only), and set up numbering sequences for standard ACH payments.",
                        "default": false,
                        "example": true
                      },
                      "recordTypeCode": {
                        "type": "string",
                        "description": "Indicates the record type code for ACH payments.",
                        "maxLength": 1,
                        "default": "5",
                        "readOnly": true,
                        "example": "5",
                        "nullable": true
                      },
                      "serviceClassCode": {
                        "type": "string",
                        "description": "Specifies the service class code for ACH payments. Use `220` for payments (credits) only, or `200` for both credits and debits. If using `200`, then `useRecommendedSetup` must be `false`.",
                        "enum": [
                          null,
                          "220",
                          "200"
                        ],
                        "nullable": true,
                        "default": null,
                        "example": "220"
                      },
                      "originatorStatusCode": {
                        "type": "string",
                        "description": "Indicates the originator status code for ACH payments.",
                        "default": "1",
                        "readOnly": true,
                        "maxLength": 1,
                        "pattern": "^[0-9]{1}$",
                        "example": "6",
                        "nullable": true
                      },
                      "batchId": {
                        "type": "string",
                        "description": "Identifies the number sequence that automatically numbers payment batches. The batch number must be 7-digits, with no prefixes or suffixes. Required if `useRecommendedSetup` is `false`. \n",
                        "example": "BOA_ACH_BatchNo",
                        "nullable": true
                      },
                      "traceNumberSequence": {
                        "type": "string",
                        "description": "Identifies the number sequence that generates the trace number for ACH entries. The trace number is formed by concatenating the bank routing number with a 7-digit sequence number, and has no prefixes or suffixes. Required if `useRecommendedSetup` is `false`. \n",
                        "example": "BOA_ACH_TraceNo",
                        "nullable": true
                      },
                      "paymentNumberSequence": {
                        "type": "string",
                        "description": "Identifies the number sequence that assigns a unique payment number to confirmed Accounts Payable (AP) payments. You can use the same number sequence specified in `traceNumberSequence`. Required if `useRecommendedSetup` is `false`.\n",
                        "example": "BOA_ACH_PayNo",
                        "nullable": true
                      },
                      "useTraceNumber": {
                        "type": "string",
                        "description": "Indicates whether to use the trace number as a payment (`useAsPayment`) or as a numbering sequence (`useNumberSequence`).",
                        "enum": [
                          null,
                          "useAsPayment",
                          "useNumberingSequence"
                        ],
                        "nullable": true,
                        "default": "useAsPayment",
                        "example": "useAsPayment"
                      }
                    }
                  },
                  "bankFile": {
                    "type": "object",
                    "description": "Specifies the bank file details for companies subscribed to Sage Cloud Services and enabled for bank file payments. A bank file is a standard file used by banks to make multiple payments, they enable your company to pay vendors using international checking accounts.\n\nFor more information, read about [setting up a checking account for bank file payments](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_account_setup) in Sage Intacct Help Center.\n",
                    "properties": {
                      "enableBankFile": {
                        "type": "boolean",
                        "description": "Indicates whether to enable bank file payments for checking accounts in supported countries.",
                        "default": false,
                        "example": true
                      },
                      "bankFileFormat": {
                        "type": "string",
                        "description": "Specifies the bank file format of the bank associated with the checking account. \n\nFor more information, read about [bank files](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_payment) in the Sage Intacct Help Center.\n",
                        "example": "ABA - Westpac",
                        "nullable": true
                      },
                      "bankCode": {
                        "type": "string",
                        "description": "Specifies the bank code for the bank associated with the checking account.",
                        "example": "Westpac Banking Corporation",
                        "nullable": true
                      },
                      "apcaNumber": {
                        "type": "string",
                        "description": "Six-digit identifier for the company or individual, used by Australian banks to make direct payments.",
                        "example": "865551",
                        "nullable": true
                      },
                      "bsbNumber": {
                        "type": "string",
                        "description": "Six-digit identifier for an individual branch of a financial institution in Australia, expressed as two groups of three separated by a hyphen.",
                        "example": "042-457",
                        "nullable": true
                      },
                      "sunNumber": {
                        "type": "string",
                        "description": "Unique identifier (optional) for organizations that collect payment with bank files. The service user number, together with the bank file, creates a record of the transaction. For HSBC customers only.",
                        "example": "6789",
                        "nullable": true
                      },
                      "sortCode": {
                        "type": "string",
                        "description": "Six-digit identifier for the UK bank and branch where the checking account is held, expressed as three groups of two separated by hyphens.",
                        "maxLength": 10,
                        "nullable": true,
                        "example": "23-44-16"
                      },
                      "seedValue": {
                        "type": "string",
                        "description": "Specifies the 32-character encryption key (seed value) issued by NedBank, used to generate and validate secure payment files.",
                        "maxLength": 40,
                        "nullable": true,
                        "example": "ABFGHETOUFEH1234IOIADRTO78DD899"
                      },
                      "userReference": {
                        "type": "string",
                        "description": "Specifies the 10-character reference supplied by Standard Bank, this reference is used on bank statements.",
                        "maxLength": 10,
                        "nullable": true,
                        "example": "SBXXSHRTNA"
                      },
                      "clientCode": {
                        "type": "string",
                        "description": "User code that identifies the client to Standard Bank.",
                        "maxLength": 10,
                        "nullable": true,
                        "example": "ProLite"
                      },
                      "serviceType": {
                        "type": "string",
                        "description": "Indicates the type of Bulk Electronic Fund Transfer (BEFT) service to use.",
                        "maxLength": 10,
                        "nullable": true,
                        "example": "PAYMENT"
                      },
                      "originatorId": {
                        "type": "string",
                        "description": "Identifier for the originator.",
                        "maxLength": 20,
                        "example": "IE26SCT803015",
                        "nullable": true
                      },
                      "businessIdCode": {
                        "type": "string",
                        "description": "Identifier for the business.",
                        "maxLength": 20,
                        "example": "BOFIIE2DXXX",
                        "nullable": true
                      },
                      "processingDataCenterCode": {
                        "type": "string",
                        "maxLength": 5,
                        "description": "Specifies the 5-digit identifier for the originating direct clearer.",
                        "example": "01674",
                        "nullable": true
                      },
                      "debtorBankNumber": {
                        "type": "string",
                        "maxLength": 3,
                        "description": "Identifier for the settlement institutional bank (processing bank).",
                        "example": "674",
                        "nullable": true
                      },
                      "branchTransitNumber": {
                        "type": "string",
                        "maxLength": 5,
                        "description": "Branch transit number for the settlement institutional bank (processing bank).",
                        "example": "43876",
                        "nullable": true
                      },
                      "returnAccountNumber": {
                        "type": "string",
                        "maxLength": 12,
                        "description": "Specifies the return account number for the checking account.",
                        "example": "IE26SCT80301",
                        "nullable": true
                      },
                      "messageIdPrefix": {
                        "type": "string",
                        "maxLength": 23,
                        "description": "Specifies a 23-character identifier for each submitted payment file, specific to the Bank of Ireland SEPA bank file format, combining a customer-defined prefix with a system-generated 12-digit date/time stamp.\n",
                        "example": "SEPA240212",
                        "nullable": true
                      },
                      "immediateDestinationId": {
                        "type": "string",
                        "maxLength": 9,
                        "description": "Bank routing number for the institution receiving the payment file.",
                        "example": "984569845",
                        "nullable": true
                      },
                      "immediateOriginId": {
                        "type": "string",
                        "maxLength": 9,
                        "description": "Bank routing number for the institution sending the payment file.",
                        "example": "878767675",
                        "nullable": true
                      },
                      "immediateOriginName": {
                        "type": "string",
                        "maxLength": 50,
                        "description": "Name of the company sending the payment file.",
                        "example": "Investment Corporation",
                        "nullable": true
                      },
                      "immediateDestinationName": {
                        "type": "string",
                        "maxLength": 50,
                        "description": "Name of the company receiving the payment file.",
                        "example": "BOA",
                        "nullable": true
                      },
                      "companyEntryDescription": {
                        "type": "string",
                        "description": "Indicates optional text used to describe the transaction, for example, Payroll or Payables, to be included with payments.",
                        "maxLength": 10,
                        "example": "Investment",
                        "nullable": true
                      },
                      "companyName": {
                        "type": "string",
                        "description": "Company name for the checking account.",
                        "maxLength": 16,
                        "example": "Ventura",
                        "nullable": true
                      },
                      "paymentNumberSequence": {
                        "type": "string",
                        "description": "Specifies the number sequence that generates a unique payment number for payment files uploaded to the bank. You can use the same number sequence specified in `traceNumberSequence`. Required if `useRecommendedSetup` is `false`.\n",
                        "maxLength": 7,
                        "example": "0000078",
                        "nullable": true
                      },
                      "fileIdSequence": {
                        "type": "string",
                        "description": "Specifies the sequence used to identify payment files generated each calendar day. The first file generated for each calendar day starts with `A`. Each subsequent file increments alphabetically, then numerically, and resets at the start of the next calendar day.\n",
                        "maxLength": 1,
                        "pattern": "^[A-Za-z0-9]$",
                        "example": "B",
                        "nullable": true
                      },
                      "postalAddress": {
                        "type": "object",
                        "description": "Postal address for the bank.",
                        "properties": {
                          "addressLine1": {
                            "type": "string",
                            "description": "First address line for the bank.",
                            "example": "36900 Neward Blvd",
                            "nullable": true
                          },
                          "addressLine2": {
                            "type": "string",
                            "description": "Second address line for the bank.",
                            "example": "Suite 100",
                            "nullable": true
                          },
                          "postCode": {
                            "type": "string",
                            "description": "Postal code for the bank.",
                            "example": "94536",
                            "nullable": true
                          },
                          "county": {
                            "type": "string",
                            "description": "County for the bank.",
                            "example": "Alameda",
                            "nullable": true
                          },
                          "countryCode": {
                            "type": "string",
                            "description": "ISO country code for the bank.",
                            "readOnly": true,
                            "example": "US",
                            "nullable": true
                          }
                        }
                      }
                    }
                  },
                  "audit": {
                    "$ref": "#/components/schemas/audit.s1",
                    "readOnly": true
                  },
                  "bankingCloudConnection": {
                    "$ref": "#/components/schemas/banking-cloud-connection"
                  },
                  "financialInstitution": {
                    "description": "financial-institutionref",
                    "type": "object",
                    "properties": {
                      "id": {
                        "type": "string",
                        "description": "Identifier for the financial institution.",
                        "readOnly": true,
                        "example": "1",
                        "nullable": true
                      },
                      "key": {
                        "type": "string",
                        "description": "Unique key for the financial institution.",
                        "readOnly": true,
                        "example": "FINTTEC4",
                        "nullable": true
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the financial institution.",
                        "readOnly": true,
                        "example": "/objects/cash-management/financial-institution/1",
                        "nullable": true
                      }
                    },
                    "readOnly": true
                  },
                  "entity": {
                    "$ref": "#/components/schemas/entity-ref"
                  },
                  "ruleSet": {
                    "type": "object",
                    "description": "Specifies the rule set this account uses to match incoming transactions for reconciliation from a bank feed or import file. You can't reconcile an account with a bank feed or import file without a rule set.",
                    "properties": {
                      "key": {
                        "type": "string",
                        "description": "Unique key for the rule set.",
                        "example": "36",
                        "nullable": true
                      },
                      "id": {
                        "type": "string",
                        "description": "Identifier for the rule set.",
                        "example": "36-RuleSetToMatch",
                        "nullable": true
                      },
                      "name": {
                        "type": "string",
                        "description": "Name for the rule set.",
                        "readOnly": true,
                        "example": "RULE-SET-CHECKING-ACCOUNTS",
                        "nullable": true
                      },
                      "href": {
                        "type": "string",
                        "description": "URL endpoint for the rule set.",
                        "readOnly": true,
                        "example": "/objects/cash-management/bank-txn-rule-set/2",
                        "nullable": true
                      }
                    }
                  },
                  "restrictions": {
                    "type": "object",
                    "description": "Specifies the restriction type, along with the entities and locations allowed to use the checking account for making payments. \n\nFor more information, read about [restricting a bank account](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Restrict_a_bank_account) in the Sage Intacct Help Center.\n",
                    "properties": {
                      "restrictionType": {
                        "type": "string",
                        "description": "Specify which entities and/or locations can access and use the checking account.\n\n- `unrestricted` (default) - the account is available to the top-level company and all entity-level locations.\n- `rootOnly` - Only the top-level company of a multi-entity structure can access the account.\n- `restricted` - Only specified locations, location groups, departments, or department groups can access the account.\n",
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
                        "description": "List of locations that can access the checking account when `restrictionType` is set to `restricted`.",
                        "items": {
                          "type": "string"
                        },
                        "example": [
                          "001-United States of America",
                          "002-United Kingdom"
                        ]
                      }
                    }
                  },
                  "paymentProviderBankAccounts": {
                    "type": "array",
                    "items": {
                      "$ref": "#/components/schemas/objects.cash-management.payment-provider-bank-account"
                    }
                  }
                },
                "required": [
                  "id",
                  "location"
                ]
              },
              "examples": {
                "Create a checking account": {
                  "value": {
                    "id": "BOA",
                    "bankAccountDetails": {
                      "accountNumber": "890000088",
                      "bankName": "Bank of America",
                      "routingNumber": "123456789",
                      "branchId": "00-1355",
                      "phoneNumber": "9007780000",
                      "bankAddress": {
                        "addressLine1": "40714",
                        "addressLine2": "Grimmer Blvd",
                        "addressLine3": "West Cameron",
                        "city": "Fremont",
                        "country": "United States",
                        "postCode": "98765",
                        "state": "CA"
                      },
                      "currency": "USD",
                      "accountHolderName": "ACME Software"
                    },
                    "accounting": {
                      "glAccount": {
                        "id": "9090.09.90-RestNextGenGL"
                      },
                      "apJournal": {
                        "id": "AR ADJ-AR Adjustment Journal"
                      },
                      "arJournal": {
                        "id": "RCPT-Receipts Journal"
                      },
                      "bankingTimeZone": "GMT+02:00 Central Europe Summer Time",
                      "serviceChargeAccountLabel": {
                        "key": "15"
                      },
                      "interestAccountLabel": {
                        "id": "Sales"
                      },
                      "disableInterEntityTransfer": false
                    },
                    "checkPrinting": {
                      "addressSettings": {
                        "addressToPrint": "company",
                        "printAddress": true,
                        "printLogo": true,
                        "address": {
                          "addressLine1": "75688 Post st",
                          "addressLine2": "456",
                          "addressLine3": "West Cameron",
                          "city": "San Jose",
                          "country": "United States",
                          "countryCode": "US",
                          "postCode": "94536",
                          "state": "CA",
                          "phone": "6609336532"
                        },
                        "name": "Zine Inc."
                      },
                      "micrSettings": {
                        "regionalSettings": {
                          "positionOfOnUsSymbol": "position31",
                          "printCode45": true,
                          "printOnUsSymbol": true,
                          "printUSFundsUnderCheckAmount": false
                        },
                        "accountNumberAlignment": "right",
                        "accountNumberPositioning": 1,
                        "minCheckNumberLength": "6"
                      },
                      "printSettings": {
                        "additionalText": "Pay to check holder",
                        "paperFormat": "top",
                        "printLineItems": true,
                        "printLocation": "id",
                        "printingFormat": "standard",
                        "nextCheckNumber": "1012",
                        "numberOfChecksInPreview": "One",
                        "printOn": "blankCheckStock"
                      },
                      "signatures": {
                        "firstSignature": "sigimg1_g.gif",
                        "limitForFirstSignatureAmount": "20",
                        "limitForSecondSignatureAmount": "30",
                        "secondSignature": "sigimg2_g.gif",
                        "thresholdForSecondSignatureAmount": "99.00"
                      },
                      "disablePrinting": false
                    },
                    "reconciliation": {
                      "matchSequence": {
                        "key": "48"
                      },
                      "useMatchSequenceForAutoMatch": true,
                      "useMatchSequenceForManualMatch": true
                    },
                    "ach": {
                      "bankId": "ACH-1",
                      "companyName": "origin",
                      "companyIdentification": "originid",
                      "originatingFinancialInstitution": "8909",
                      "companyEntryDescription": "entry desc",
                      "companyDiscretionaryData": "disc",
                      "serviceClassCode": "220",
                      "batchId": "BOA_ACH_BatchNo",
                      "traceNumberSequence": "BOA_ACH_TraceNo",
                      "paymentNumberSequence": "CONTINVOICE",
                      "useTraceNumber": "T",
                      "enableACH": true,
                      "useRecommendedSetup": true
                    },
                    "ruleSet": {
                      "key": "53",
                      "id": "MatchDateAmountGrpbyDateRuleSet"
                    },
                    "restrictions": {
                      "restrictionType": "restricted",
                      "locations": [
                        "1-United States of America",
                        "200-My New Entity"
                      ]
                    },
                    "location": {
                      "key": "1",
                      "id": "1-United States of America"
                    }
                  }
                }
              }
            }
          }
        },
        "responses": {
          "201": {
            "description": "Created checking account",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "title": "New checking-account",
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
                  "Reference to new checking account": {
                    "value": {
                      "id": "BOA-000987g",
                      "bankAccountDetails": {
                        "accountNumber": "890000088",
                        "bankName": "Bank of America",
                        "routingNumber": "123456789",
                        "branchId": "00-1355",
                        "phoneNumber": "9007780000",
                        "bankAddress": {
                          "addressLine1": "40714",
                          "addressLine2": "Grimmer Blvd",
                          "addressLine3": null,
                          "city": "Fremont",
                          "country": "United States",
                          "postCode": "98765",
                          "state": "CA"
                        },
                        "currency": "GBP",
                        "accountHolderName": "ACME Software"
                      },
                      "accounting": {
                        "glAccount": {
                          "id": "9090.09.90-RestNextGenGL"
                        },
                        "apJournal": {
                          "id": "AR ADJ-AR Adjustment Journal"
                        },
                        "arJournal": {
                          "id": "CHASE D-CHASE BANK DISB"
                        },
                        "bankingTimeZone": "GMT+05:30 Bombay, Calcutta, Madras, New Delhi",
                        "serviceChargeAccountLabel": {
                          "key": "15"
                        },
                        "interestAccountLabel": {
                          "id": "Sales"
                        },
                        "disableInterEntityTransfer": false
                      },
                      "checkPrinting": {
                        "addressSettings": {
                          "addressToPrint": "company",
                          "printAddress": true,
                          "printLogo": true,
                          "address": {
                            "addressLine1": "75688 Post st",
                            "addressLine2": "456",
                            "addressLine3": null,
                            "city": "San Jose",
                            "country": "United States",
                            "countryCode": "US",
                            "postCode": "94536",
                            "state": "CA",
                            "phone": "6609336532"
                          },
                          "name": "Zine Inc."
                        },
                        "micrSettings": {
                          "regionalSettings": {
                            "positionOfOnUsSymbol": "position31",
                            "printCode45": true,
                            "printOnUsSymbol": true,
                            "printUSFundsUnderCheckAmount": false
                          },
                          "accountNumberAlignment": "right",
                          "accountNumberPositioning": 1,
                          "minCheckNumberLength": "6"
                        },
                        "printSettings": {
                          "additionalText": "Pay to check holder",
                          "paperFormat": "top",
                          "printLineItems": true,
                          "printLocation": "id",
                          "printingFormat": "standard",
                          "nextCheckNumber": "1012",
                          "// \"printPreview\"": "One",
                          "printOn": "blankCheckStock"
                        },
                        "signatures": {
                          "firstSignature": "sigimg1_g.gif",
                          "limitForFirstSignatureAmount": null,
                          "limitForSecondSignatureAmount": null,
                          "secondSignature": "sigimg2_g.gif",
                          "thresholdForSecondSignatureAmount": "99.00"
                        },
                        "disablePrinting": false
                      },
                      "reconciliation": {
                        "matchSequence": {
                          "key": "48"
                        },
                        "useMatchSequenceForAutoMatch": true,
                        "useMatchSequenceForManualMatch": true
                      },
                      "department": {
                        "id": "11-Accounting"
                      },
                      "location": {
                        "id": "1-United States of America"
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
            "name": "Create a checking account",
            "source": "curl --request POST \\\n  --url https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account \\\n  --header 'Content-Type: application/json' \\\n  --data '{\"id\":\"BOA\",\"bankAccountDetails\":{\"accountNumber\":\"890000088\",\"bankName\":\"Bank of America\",\"routingNumber\":\"123456789\",\"branchId\":\"00-1355\",\"phoneNumber\":\"9007780000\",\"bankAddress\":{\"addressLine1\":\"40714\",\"addressLine2\":\"Grimmer Blvd\",\"addressLine3\":\"West Cameron\",\"city\":\"Fremont\",\"country\":\"United States\",\"postCode\":\"98765\",\"state\":\"CA\"},\"currency\":\"USD\",\"accountHolderName\":\"ACME Software\"},\"accounting\":{\"glAccount\":{\"id\":\"9090.09.90-RestNextGenGL\"},\"apJournal\":{\"id\":\"AR ADJ-AR Adjustment Journal\"},\"arJournal\":{\"id\":\"RCPT-Receipts Journal\"},\"bankingTimeZone\":\"GMT+02:00 Central Europe Summer Time\",\"serviceChargeAccountLabel\":{\"key\":\"15\"},\"interestAccountLabel\":{\"id\":\"Sales\"},\"disableInterEntityTransfer\":false},\"checkPrinting\":{\"addressSettings\":{\"addressToPrint\":\"company\",\"printAddress\":true,\"printLogo\":true,\"address\":{\"addressLine1\":\"75688 Post st\",\"addressLine2\":\"456\",\"addressLine3\":\"West Cameron\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"94536\",\"state\":\"CA\",\"phone\":\"6609336532\"},\"name\":\"Zine Inc.\"},\"micrSettings\":{\"regionalSettings\":{\"positionOfOnUsSymbol\":\"position31\",\"printCode45\":true,\"printOnUsSymbol\":true,\"printUSFundsUnderCheckAmount\":false},\"accountNumberAlignment\":\"right\",\"accountNumberPositioning\":1,\"minCheckNumberLength\":\"6\"},\"printSettings\":{\"additionalText\":\"Pay to check holder\",\"paperFormat\":\"top\",\"printLineItems\":true,\"printLocation\":\"id\",\"printingFormat\":\"standard\",\"nextCheckNumber\":\"1012\",\"numberOfChecksInPreview\":\"One\",\"printOn\":\"blankCheckStock\"},\"signatures\":{\"firstSignature\":\"sigimg1_g.gif\",\"limitForFirstSignatureAmount\":\"20\",\"limitForSecondSignatureAmount\":\"30\",\"secondSignature\":\"sigimg2_g.gif\",\"thresholdForSecondSignatureAmount\":\"99.00\"},\"disablePrinting\":false},\"reconciliation\":{\"matchSequence\":{\"key\":\"48\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ach\":{\"bankId\":\"ACH-1\",\"companyName\":\"origin\",\"companyIdentification\":\"originid\",\"originatingFinancialInstitution\":\"8909\",\"companyEntryDescription\":\"entry desc\",\"companyDiscretionaryData\":\"disc\",\"serviceClassCode\":\"220\",\"batchId\":\"BOA_ACH_BatchNo\",\"traceNumberSequence\":\"BOA_ACH_TraceNo\",\"paymentNumberSequence\":\"CONTINVOICE\",\"useTraceNumber\":\"T\",\"enableACH\":true,\"useRecommendedSetup\":true},\"ruleSet\":{\"key\":\"53\",\"id\":\"MatchDateAmountGrpbyDateRuleSet\"},\"restrictions\":{\"restrictionType\":\"restricted\",\"locations\":[\"1-United States of America\",\"200-My New Entity\"]},\"location\":{\"key\":\"1\",\"id\":\"1-United States of America\"}}'",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_httpie",
            "name": "Create a checking account",
            "source": "echo '{\"id\":\"BOA\",\"bankAccountDetails\":{\"accountNumber\":\"890000088\",\"bankName\":\"Bank of America\",\"routingNumber\":\"123456789\",\"branchId\":\"00-1355\",\"phoneNumber\":\"9007780000\",\"bankAddress\":{\"addressLine1\":\"40714\",\"addressLine2\":\"Grimmer Blvd\",\"addressLine3\":\"West Cameron\",\"city\":\"Fremont\",\"country\":\"United States\",\"postCode\":\"98765\",\"state\":\"CA\"},\"currency\":\"USD\",\"accountHolderName\":\"ACME Software\"},\"accounting\":{\"glAccount\":{\"id\":\"9090.09.90-RestNextGenGL\"},\"apJournal\":{\"id\":\"AR ADJ-AR Adjustment Journal\"},\"arJournal\":{\"id\":\"RCPT-Receipts Journal\"},\"bankingTimeZone\":\"GMT+02:00 Central Europe Summer Time\",\"serviceChargeAccountLabel\":{\"key\":\"15\"},\"interestAccountLabel\":{\"id\":\"Sales\"},\"disableInterEntityTransfer\":false},\"checkPrinting\":{\"addressSettings\":{\"addressToPrint\":\"company\",\"printAddress\":true,\"printLogo\":true,\"address\":{\"addressLine1\":\"75688 Post st\",\"addressLine2\":\"456\",\"addressLine3\":\"West Cameron\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"94536\",\"state\":\"CA\",\"phone\":\"6609336532\"},\"name\":\"Zine Inc.\"},\"micrSettings\":{\"regionalSettings\":{\"positionOfOnUsSymbol\":\"position31\",\"printCode45\":true,\"printOnUsSymbol\":true,\"printUSFundsUnderCheckAmount\":false},\"accountNumberAlignment\":\"right\",\"accountNumberPositioning\":1,\"minCheckNumberLength\":\"6\"},\"printSettings\":{\"additionalText\":\"Pay to check holder\",\"paperFormat\":\"top\",\"printLineItems\":true,\"printLocation\":\"id\",\"printingFormat\":\"standard\",\"nextCheckNumber\":\"1012\",\"numberOfChecksInPreview\":\"One\",\"printOn\":\"blankCheckStock\"},\"signatures\":{\"firstSignature\":\"sigimg1_g.gif\",\"limitForFirstSignatureAmount\":\"20\",\"limitForSecondSignatureAmount\":\"30\",\"secondSignature\":\"sigimg2_g.gif\",\"thresholdForSecondSignatureAmount\":\"99.00\"},\"disablePrinting\":false},\"reconciliation\":{\"matchSequence\":{\"key\":\"48\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ach\":{\"bankId\":\"ACH-1\",\"companyName\":\"origin\",\"companyIdentification\":\"originid\",\"originatingFinancialInstitution\":\"8909\",\"companyEntryDescription\":\"entry desc\",\"companyDiscretionaryData\":\"disc\",\"serviceClassCode\":\"220\",\"batchId\":\"BOA_ACH_BatchNo\",\"traceNumberSequence\":\"BOA_ACH_TraceNo\",\"paymentNumberSequence\":\"CONTINVOICE\",\"useTraceNumber\":\"T\",\"enableACH\":true,\"useRecommendedSetup\":true},\"ruleSet\":{\"key\":\"53\",\"id\":\"MatchDateAmountGrpbyDateRuleSet\"},\"restrictions\":{\"restrictionType\":\"restricted\",\"locations\":[\"1-United States of America\",\"200-My New Entity\"]},\"location\":{\"key\":\"1\",\"id\":\"1-United States of America\"}}' |  \\\n  http POST https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account \\\n  Content-Type:application/json",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "shell_wget",
            "name": "Create a checking account",
            "source": "wget --quiet \\\n  --method POST \\\n  --header 'Content-Type: application/json' \\\n  --body-data '{\"id\":\"BOA\",\"bankAccountDetails\":{\"accountNumber\":\"890000088\",\"bankName\":\"Bank of America\",\"routingNumber\":\"123456789\",\"branchId\":\"00-1355\",\"phoneNumber\":\"9007780000\",\"bankAddress\":{\"addressLine1\":\"40714\",\"addressLine2\":\"Grimmer Blvd\",\"addressLine3\":\"West Cameron\",\"city\":\"Fremont\",\"country\":\"United States\",\"postCode\":\"98765\",\"state\":\"CA\"},\"currency\":\"USD\",\"accountHolderName\":\"ACME Software\"},\"accounting\":{\"glAccount\":{\"id\":\"9090.09.90-RestNextGenGL\"},\"apJournal\":{\"id\":\"AR ADJ-AR Adjustment Journal\"},\"arJournal\":{\"id\":\"RCPT-Receipts Journal\"},\"bankingTimeZone\":\"GMT+02:00 Central Europe Summer Time\",\"serviceChargeAccountLabel\":{\"key\":\"15\"},\"interestAccountLabel\":{\"id\":\"Sales\"},\"disableInterEntityTransfer\":false},\"checkPrinting\":{\"addressSettings\":{\"addressToPrint\":\"company\",\"printAddress\":true,\"printLogo\":true,\"address\":{\"addressLine1\":\"75688 Post st\",\"addressLine2\":\"456\",\"addressLine3\":\"West Cameron\",\"city\":\"San Jose\",\"country\":\"United States\",\"countryCode\":\"US\",\"postCode\":\"94536\",\"state\":\"CA\",\"phone\":\"6609336532\"},\"name\":\"Zine Inc.\"},\"micrSettings\":{\"regionalSettings\":{\"positionOfOnUsSymbol\":\"position31\",\"printCode45\":true,\"printOnUsSymbol\":true,\"printUSFundsUnderCheckAmount\":false},\"accountNumberAlignment\":\"right\",\"accountNumberPositioning\":1,\"minCheckNumberLength\":\"6\"},\"printSettings\":{\"additionalText\":\"Pay to check holder\",\"paperFormat\":\"top\",\"printLineItems\":true,\"printLocation\":\"id\",\"printingFormat\":\"standard\",\"nextCheckNumber\":\"1012\",\"numberOfChecksInPreview\":\"One\",\"printOn\":\"blankCheckStock\"},\"signatures\":{\"firstSignature\":\"sigimg1_g.gif\",\"limitForFirstSignatureAmount\":\"20\",\"limitForSecondSignatureAmount\":\"30\",\"secondSignature\":\"sigimg2_g.gif\",\"thresholdForSecondSignatureAmount\":\"99.00\"},\"disablePrinting\":false},\"reconciliation\":{\"matchSequence\":{\"key\":\"48\"},\"useMatchSequenceForAutoMatch\":true,\"useMatchSequenceForManualMatch\":true},\"ach\":{\"bankId\":\"ACH-1\",\"companyName\":\"origin\",\"companyIdentification\":\"originid\",\"originatingFinancialInstitution\":\"8909\",\"companyEntryDescription\":\"entry desc\",\"companyDiscretionaryData\":\"disc\",\"serviceClassCode\":\"220\",\"batchId\":\"BOA_ACH_BatchNo\",\"traceNumberSequence\":\"BOA_ACH_TraceNo\",\"paymentNumberSequence\":\"CONTINVOICE\",\"useTraceNumber\":\"T\",\"enableACH\":true,\"useRecommendedSetup\":true},\"ruleSet\":{\"key\":\"53\",\"id\":\"MatchDateAmountGrpbyDateRuleSet\"},\"restrictions\":{\"restrictionType\":\"restricted\",\"locations\":[\"1-United States of America\",\"200-My New Entity\"]},\"location\":{\"key\":\"1\",\"id\":\"1-United States of America\"}}' \\\n  --output-document \\\n  - https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_xhr",
            "name": "Create a checking account",
            "source": "const data = JSON.stringify({\n  \"id\": \"BOA\",\n  \"bankAccountDetails\": {\n    \"accountNumber\": \"890000088\",\n    \"bankName\": \"Bank of America\",\n    \"routingNumber\": \"123456789\",\n    \"branchId\": \"00-1355\",\n    \"phoneNumber\": \"9007780000\",\n    \"bankAddress\": {\n      \"addressLine1\": \"40714\",\n      \"addressLine2\": \"Grimmer Blvd\",\n      \"addressLine3\": \"West Cameron\",\n      \"city\": \"Fremont\",\n      \"country\": \"United States\",\n      \"postCode\": \"98765\",\n      \"state\": \"CA\"\n    },\n    \"currency\": \"USD\",\n    \"accountHolderName\": \"ACME Software\"\n  },\n  \"accounting\": {\n    \"glAccount\": {\n      \"id\": \"9090.09.90-RestNextGenGL\"\n    },\n    \"apJournal\": {\n      \"id\": \"AR ADJ-AR Adjustment Journal\"\n    },\n    \"arJournal\": {\n      \"id\": \"RCPT-Receipts Journal\"\n    },\n    \"bankingTimeZone\": \"GMT+02:00 Central Europe Summer Time\",\n    \"serviceChargeAccountLabel\": {\n      \"key\": \"15\"\n    },\n    \"interestAccountLabel\": {\n      \"id\": \"Sales\"\n    },\n    \"disableInterEntityTransfer\": false\n  },\n  \"checkPrinting\": {\n    \"addressSettings\": {\n      \"addressToPrint\": \"company\",\n      \"printAddress\": true,\n      \"printLogo\": true,\n      \"address\": {\n        \"addressLine1\": \"75688 Post st\",\n        \"addressLine2\": \"456\",\n        \"addressLine3\": \"West Cameron\",\n        \"city\": \"San Jose\",\n        \"country\": \"United States\",\n        \"countryCode\": \"US\",\n        \"postCode\": \"94536\",\n        \"state\": \"CA\",\n        \"phone\": \"6609336532\"\n      },\n      \"name\": \"Zine Inc.\"\n    },\n    \"micrSettings\": {\n      \"regionalSettings\": {\n        \"positionOfOnUsSymbol\": \"position31\",\n        \"printCode45\": true,\n        \"printOnUsSymbol\": true,\n        \"printUSFundsUnderCheckAmount\": false\n      },\n      \"accountNumberAlignment\": \"right\",\n      \"accountNumberPositioning\": 1,\n      \"minCheckNumberLength\": \"6\"\n    },\n    \"printSettings\": {\n      \"additionalText\": \"Pay to check holder\",\n      \"paperFormat\": \"top\",\n      \"printLineItems\": true,\n      \"printLocation\": \"id\",\n      \"printingFormat\": \"standard\",\n      \"nextCheckNumber\": \"1012\",\n      \"numberOfChecksInPreview\": \"One\",\n      \"printOn\": \"blankCheckStock\"\n    },\n    \"signatures\": {\n      \"firstSignature\": \"sigimg1_g.gif\",\n      \"limitForFirstSignatureAmount\": \"20\",\n      \"limitForSecondSignatureAmount\": \"30\",\n      \"secondSignature\": \"sigimg2_g.gif\",\n      \"thresholdForSecondSignatureAmount\": \"99.00\"\n    },\n    \"disablePrinting\": false\n  },\n  \"reconciliation\": {\n    \"matchSequence\": {\n      \"key\": \"48\"\n    },\n    \"useMatchSequenceForAutoMatch\": true,\n    \"useMatchSequenceForManualMatch\": true\n  },\n  \"ach\": {\n    \"bankId\": \"ACH-1\",\n    \"companyName\": \"origin\",\n    \"companyIdentification\": \"originid\",\n    \"originatingFinancialInstitution\": \"8909\",\n    \"companyEntryDescription\": \"entry desc\",\n    \"companyDiscretionaryData\": \"disc\",\n    \"serviceClassCode\": \"220\",\n    \"batchId\": \"BOA_ACH_BatchNo\",\n    \"traceNumberSequence\": \"BOA_ACH_TraceNo\",\n    \"paymentNumberSequence\": \"CONTINVOICE\",\n    \"useTraceNumber\": \"T\",\n    \"enableACH\": true,\n    \"useRecommendedSetup\": true\n  },\n  \"ruleSet\": {\n    \"key\": \"53\",\n    \"id\": \"MatchDateAmountGrpbyDateRuleSet\"\n  },\n  \"restrictions\": {\n    \"restrictionType\": \"restricted\",\n    \"locations\": [\n      \"1-United States of America\",\n      \"200-My New Entity\"\n    ]\n  },\n  \"location\": {\n    \"key\": \"1\",\n    \"id\": \"1-United States of America\"\n  }\n});\n\nconst xhr = new XMLHttpRequest();\nxhr.withCredentials = true;\n\nxhr.addEventListener(\"readystatechange\", function () {\n  if (this.readyState === this.DONE) {\n    console.log(this.responseText);\n  }\n});\n\nxhr.open(\"POST\", \"https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account\");\nxhr.setRequestHeader(\"Content-Type\", \"application/json\");\n\nxhr.send(data);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "javascript_jquery",
            "name": "Create a checking account",
            "source": "const settings = {\n  \"async\": true,\n  \"crossDomain\": true,\n  \"url\": \"https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account\",\n  \"method\": \"POST\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  },\n  \"processData\": false,\n  \"data\": \"{\\\"id\\\":\\\"BOA\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"890000088\\\",\\\"bankName\\\":\\\"Bank of America\\\",\\\"routingNumber\\\":\\\"123456789\\\",\\\"branchId\\\":\\\"00-1355\\\",\\\"phoneNumber\\\":\\\"9007780000\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"40714\\\",\\\"addressLine2\\\":\\\"Grimmer Blvd\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"Fremont\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"98765\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\",\\\"accountHolderName\\\":\\\"ACME Software\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"9090.09.90-RestNextGenGL\\\"},\\\"apJournal\\\":{\\\"id\\\":\\\"AR ADJ-AR Adjustment Journal\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"RCPT-Receipts Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+02:00 Central Europe Summer Time\\\",\\\"serviceChargeAccountLabel\\\":{\\\"key\\\":\\\"15\\\"},\\\"interestAccountLabel\\\":{\\\"id\\\":\\\"Sales\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"checkPrinting\\\":{\\\"addressSettings\\\":{\\\"addressToPrint\\\":\\\"company\\\",\\\"printAddress\\\":true,\\\"printLogo\\\":true,\\\"address\\\":{\\\"addressLine1\\\":\\\"75688 Post st\\\",\\\"addressLine2\\\":\\\"456\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"94536\\\",\\\"state\\\":\\\"CA\\\",\\\"phone\\\":\\\"6609336532\\\"},\\\"name\\\":\\\"Zine Inc.\\\"},\\\"micrSettings\\\":{\\\"regionalSettings\\\":{\\\"positionOfOnUsSymbol\\\":\\\"position31\\\",\\\"printCode45\\\":true,\\\"printOnUsSymbol\\\":true,\\\"printUSFundsUnderCheckAmount\\\":false},\\\"accountNumberAlignment\\\":\\\"right\\\",\\\"accountNumberPositioning\\\":1,\\\"minCheckNumberLength\\\":\\\"6\\\"},\\\"printSettings\\\":{\\\"additionalText\\\":\\\"Pay to check holder\\\",\\\"paperFormat\\\":\\\"top\\\",\\\"printLineItems\\\":true,\\\"printLocation\\\":\\\"id\\\",\\\"printingFormat\\\":\\\"standard\\\",\\\"nextCheckNumber\\\":\\\"1012\\\",\\\"numberOfChecksInPreview\\\":\\\"One\\\",\\\"printOn\\\":\\\"blankCheckStock\\\"},\\\"signatures\\\":{\\\"firstSignature\\\":\\\"sigimg1_g.gif\\\",\\\"limitForFirstSignatureAmount\\\":\\\"20\\\",\\\"limitForSecondSignatureAmount\\\":\\\"30\\\",\\\"secondSignature\\\":\\\"sigimg2_g.gif\\\",\\\"thresholdForSecondSignatureAmount\\\":\\\"99.00\\\"},\\\"disablePrinting\\\":false},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"key\\\":\\\"48\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ach\\\":{\\\"bankId\\\":\\\"ACH-1\\\",\\\"companyName\\\":\\\"origin\\\",\\\"companyIdentification\\\":\\\"originid\\\",\\\"originatingFinancialInstitution\\\":\\\"8909\\\",\\\"companyEntryDescription\\\":\\\"entry desc\\\",\\\"companyDiscretionaryData\\\":\\\"disc\\\",\\\"serviceClassCode\\\":\\\"220\\\",\\\"batchId\\\":\\\"BOA_ACH_BatchNo\\\",\\\"traceNumberSequence\\\":\\\"BOA_ACH_TraceNo\\\",\\\"paymentNumberSequence\\\":\\\"CONTINVOICE\\\",\\\"useTraceNumber\\\":\\\"T\\\",\\\"enableACH\\\":true,\\\"useRecommendedSetup\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\",\\\"id\\\":\\\"MatchDateAmountGrpbyDateRuleSet\\\"},\\\"restrictions\\\":{\\\"restrictionType\\\":\\\"restricted\\\",\\\"locations\\\":[\\\"1-United States of America\\\",\\\"200-My New Entity\\\"]},\\\"location\\\":{\\\"key\\\":\\\"1\\\",\\\"id\\\":\\\"1-United States of America\\\"}}\"\n};\n\n$.ajax(settings).done(function (response) {\n  console.log(response);\n});",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "node_native",
            "name": "Create a checking account",
            "source": "const http = require(\"https\");\n\nconst options = {\n  \"method\": \"POST\",\n  \"hostname\": \"api.intacct.com\",\n  \"port\": null,\n  \"path\": \"/ia/api/v1/objects/cash-management/checking-account\",\n  \"headers\": {\n    \"Content-Type\": \"application/json\"\n  }\n};\n\nconst req = http.request(options, function (res) {\n  const chunks = [];\n\n  res.on(\"data\", function (chunk) {\n    chunks.push(chunk);\n  });\n\n  res.on(\"end\", function () {\n    const body = Buffer.concat(chunks);\n    console.log(body.toString());\n  });\n});\n\nreq.write(JSON.stringify({\n  id: 'BOA',\n  bankAccountDetails: {\n    accountNumber: '890000088',\n    bankName: 'Bank of America',\n    routingNumber: '123456789',\n    branchId: '00-1355',\n    phoneNumber: '9007780000',\n    bankAddress: {\n      addressLine1: '40714',\n      addressLine2: 'Grimmer Blvd',\n      addressLine3: 'West Cameron',\n      city: 'Fremont',\n      country: 'United States',\n      postCode: '98765',\n      state: 'CA'\n    },\n    currency: 'USD',\n    accountHolderName: 'ACME Software'\n  },\n  accounting: {\n    glAccount: {id: '9090.09.90-RestNextGenGL'},\n    apJournal: {id: 'AR ADJ-AR Adjustment Journal'},\n    arJournal: {id: 'RCPT-Receipts Journal'},\n    bankingTimeZone: 'GMT+02:00 Central Europe Summer Time',\n    serviceChargeAccountLabel: {key: '15'},\n    interestAccountLabel: {id: 'Sales'},\n    disableInterEntityTransfer: false\n  },\n  checkPrinting: {\n    addressSettings: {\n      addressToPrint: 'company',\n      printAddress: true,\n      printLogo: true,\n      address: {\n        addressLine1: '75688 Post st',\n        addressLine2: '456',\n        addressLine3: 'West Cameron',\n        city: 'San Jose',\n        country: 'United States',\n        countryCode: 'US',\n        postCode: '94536',\n        state: 'CA',\n        phone: '6609336532'\n      },\n      name: 'Zine Inc.'\n    },\n    micrSettings: {\n      regionalSettings: {\n        positionOfOnUsSymbol: 'position31',\n        printCode45: true,\n        printOnUsSymbol: true,\n        printUSFundsUnderCheckAmount: false\n      },\n      accountNumberAlignment: 'right',\n      accountNumberPositioning: 1,\n      minCheckNumberLength: '6'\n    },\n    printSettings: {\n      additionalText: 'Pay to check holder',\n      paperFormat: 'top',\n      printLineItems: true,\n      printLocation: 'id',\n      printingFormat: 'standard',\n      nextCheckNumber: '1012',\n      numberOfChecksInPreview: 'One',\n      printOn: 'blankCheckStock'\n    },\n    signatures: {\n      firstSignature: 'sigimg1_g.gif',\n      limitForFirstSignatureAmount: '20',\n      limitForSecondSignatureAmount: '30',\n      secondSignature: 'sigimg2_g.gif',\n      thresholdForSecondSignatureAmount: '99.00'\n    },\n    disablePrinting: false\n  },\n  reconciliation: {\n    matchSequence: {key: '48'},\n    useMatchSequenceForAutoMatch: true,\n    useMatchSequenceForManualMatch: true\n  },\n  ach: {\n    bankId: 'ACH-1',\n    companyName: 'origin',\n    companyIdentification: 'originid',\n    originatingFinancialInstitution: '8909',\n    companyEntryDescription: 'entry desc',\n    companyDiscretionaryData: 'disc',\n    serviceClassCode: '220',\n    batchId: 'BOA_ACH_BatchNo',\n    traceNumberSequence: 'BOA_ACH_TraceNo',\n    paymentNumberSequence: 'CONTINVOICE',\n    useTraceNumber: 'T',\n    enableACH: true,\n    useRecommendedSetup: true\n  },\n  ruleSet: {key: '53', id: 'MatchDateAmountGrpbyDateRuleSet'},\n  restrictions: {\n    restrictionType: 'restricted',\n    locations: ['1-United States of America', '200-My New Entity']\n  },\n  location: {key: '1', id: '1-United States of America'}\n}));\nreq.end();",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_httpclient",
            "name": "Create a checking account",
            "source": "var client = new HttpClient();\nvar request = new HttpRequestMessage\n{\n    Method = HttpMethod.Post,\n    RequestUri = new Uri(\"https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account\"),\n    Content = new StringContent(\"{\\\"id\\\":\\\"BOA\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"890000088\\\",\\\"bankName\\\":\\\"Bank of America\\\",\\\"routingNumber\\\":\\\"123456789\\\",\\\"branchId\\\":\\\"00-1355\\\",\\\"phoneNumber\\\":\\\"9007780000\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"40714\\\",\\\"addressLine2\\\":\\\"Grimmer Blvd\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"Fremont\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"98765\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\",\\\"accountHolderName\\\":\\\"ACME Software\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"9090.09.90-RestNextGenGL\\\"},\\\"apJournal\\\":{\\\"id\\\":\\\"AR ADJ-AR Adjustment Journal\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"RCPT-Receipts Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+02:00 Central Europe Summer Time\\\",\\\"serviceChargeAccountLabel\\\":{\\\"key\\\":\\\"15\\\"},\\\"interestAccountLabel\\\":{\\\"id\\\":\\\"Sales\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"checkPrinting\\\":{\\\"addressSettings\\\":{\\\"addressToPrint\\\":\\\"company\\\",\\\"printAddress\\\":true,\\\"printLogo\\\":true,\\\"address\\\":{\\\"addressLine1\\\":\\\"75688 Post st\\\",\\\"addressLine2\\\":\\\"456\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"94536\\\",\\\"state\\\":\\\"CA\\\",\\\"phone\\\":\\\"6609336532\\\"},\\\"name\\\":\\\"Zine Inc.\\\"},\\\"micrSettings\\\":{\\\"regionalSettings\\\":{\\\"positionOfOnUsSymbol\\\":\\\"position31\\\",\\\"printCode45\\\":true,\\\"printOnUsSymbol\\\":true,\\\"printUSFundsUnderCheckAmount\\\":false},\\\"accountNumberAlignment\\\":\\\"right\\\",\\\"accountNumberPositioning\\\":1,\\\"minCheckNumberLength\\\":\\\"6\\\"},\\\"printSettings\\\":{\\\"additionalText\\\":\\\"Pay to check holder\\\",\\\"paperFormat\\\":\\\"top\\\",\\\"printLineItems\\\":true,\\\"printLocation\\\":\\\"id\\\",\\\"printingFormat\\\":\\\"standard\\\",\\\"nextCheckNumber\\\":\\\"1012\\\",\\\"numberOfChecksInPreview\\\":\\\"One\\\",\\\"printOn\\\":\\\"blankCheckStock\\\"},\\\"signatures\\\":{\\\"firstSignature\\\":\\\"sigimg1_g.gif\\\",\\\"limitForFirstSignatureAmount\\\":\\\"20\\\",\\\"limitForSecondSignatureAmount\\\":\\\"30\\\",\\\"secondSignature\\\":\\\"sigimg2_g.gif\\\",\\\"thresholdForSecondSignatureAmount\\\":\\\"99.00\\\"},\\\"disablePrinting\\\":false},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"key\\\":\\\"48\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ach\\\":{\\\"bankId\\\":\\\"ACH-1\\\",\\\"companyName\\\":\\\"origin\\\",\\\"companyIdentification\\\":\\\"originid\\\",\\\"originatingFinancialInstitution\\\":\\\"8909\\\",\\\"companyEntryDescription\\\":\\\"entry desc\\\",\\\"companyDiscretionaryData\\\":\\\"disc\\\",\\\"serviceClassCode\\\":\\\"220\\\",\\\"batchId\\\":\\\"BOA_ACH_BatchNo\\\",\\\"traceNumberSequence\\\":\\\"BOA_ACH_TraceNo\\\",\\\"paymentNumberSequence\\\":\\\"CONTINVOICE\\\",\\\"useTraceNumber\\\":\\\"T\\\",\\\"enableACH\\\":true,\\\"useRecommendedSetup\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\",\\\"id\\\":\\\"MatchDateAmountGrpbyDateRuleSet\\\"},\\\"restrictions\\\":{\\\"restrictionType\\\":\\\"restricted\\\",\\\"locations\\\":[\\\"1-United States of America\\\",\\\"200-My New Entity\\\"]},\\\"location\\\":{\\\"key\\\":\\\"1\\\",\\\"id\\\":\\\"1-United States of America\\\"}}\")\n    {\n        Headers =\n        {\n            ContentType = new MediaTypeHeaderValue(\"application/json\")\n        }\n    }\n};\nusing (var response = await client.SendAsync(request))\n{\n    response.EnsureSuccessStatusCode();\n    var body = await response.Content.ReadAsStringAsync();\n    Console.WriteLine(body);\n}",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "csharp_restsharp",
            "name": "Create a checking account",
            "source": "var client = new RestClient(\"https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account\");\nvar request = new RestRequest(Method.POST);\nrequest.AddHeader(\"Content-Type\", \"application/json\");\nrequest.AddParameter(\"application/json\", \"{\\\"id\\\":\\\"BOA\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"890000088\\\",\\\"bankName\\\":\\\"Bank of America\\\",\\\"routingNumber\\\":\\\"123456789\\\",\\\"branchId\\\":\\\"00-1355\\\",\\\"phoneNumber\\\":\\\"9007780000\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"40714\\\",\\\"addressLine2\\\":\\\"Grimmer Blvd\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"Fremont\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"98765\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\",\\\"accountHolderName\\\":\\\"ACME Software\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"9090.09.90-RestNextGenGL\\\"},\\\"apJournal\\\":{\\\"id\\\":\\\"AR ADJ-AR Adjustment Journal\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"RCPT-Receipts Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+02:00 Central Europe Summer Time\\\",\\\"serviceChargeAccountLabel\\\":{\\\"key\\\":\\\"15\\\"},\\\"interestAccountLabel\\\":{\\\"id\\\":\\\"Sales\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"checkPrinting\\\":{\\\"addressSettings\\\":{\\\"addressToPrint\\\":\\\"company\\\",\\\"printAddress\\\":true,\\\"printLogo\\\":true,\\\"address\\\":{\\\"addressLine1\\\":\\\"75688 Post st\\\",\\\"addressLine2\\\":\\\"456\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"94536\\\",\\\"state\\\":\\\"CA\\\",\\\"phone\\\":\\\"6609336532\\\"},\\\"name\\\":\\\"Zine Inc.\\\"},\\\"micrSettings\\\":{\\\"regionalSettings\\\":{\\\"positionOfOnUsSymbol\\\":\\\"position31\\\",\\\"printCode45\\\":true,\\\"printOnUsSymbol\\\":true,\\\"printUSFundsUnderCheckAmount\\\":false},\\\"accountNumberAlignment\\\":\\\"right\\\",\\\"accountNumberPositioning\\\":1,\\\"minCheckNumberLength\\\":\\\"6\\\"},\\\"printSettings\\\":{\\\"additionalText\\\":\\\"Pay to check holder\\\",\\\"paperFormat\\\":\\\"top\\\",\\\"printLineItems\\\":true,\\\"printLocation\\\":\\\"id\\\",\\\"printingFormat\\\":\\\"standard\\\",\\\"nextCheckNumber\\\":\\\"1012\\\",\\\"numberOfChecksInPreview\\\":\\\"One\\\",\\\"printOn\\\":\\\"blankCheckStock\\\"},\\\"signatures\\\":{\\\"firstSignature\\\":\\\"sigimg1_g.gif\\\",\\\"limitForFirstSignatureAmount\\\":\\\"20\\\",\\\"limitForSecondSignatureAmount\\\":\\\"30\\\",\\\"secondSignature\\\":\\\"sigimg2_g.gif\\\",\\\"thresholdForSecondSignatureAmount\\\":\\\"99.00\\\"},\\\"disablePrinting\\\":false},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"key\\\":\\\"48\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ach\\\":{\\\"bankId\\\":\\\"ACH-1\\\",\\\"companyName\\\":\\\"origin\\\",\\\"companyIdentification\\\":\\\"originid\\\",\\\"originatingFinancialInstitution\\\":\\\"8909\\\",\\\"companyEntryDescription\\\":\\\"entry desc\\\",\\\"companyDiscretionaryData\\\":\\\"disc\\\",\\\"serviceClassCode\\\":\\\"220\\\",\\\"batchId\\\":\\\"BOA_ACH_BatchNo\\\",\\\"traceNumberSequence\\\":\\\"BOA_ACH_TraceNo\\\",\\\"paymentNumberSequence\\\":\\\"CONTINVOICE\\\",\\\"useTraceNumber\\\":\\\"T\\\",\\\"enableACH\\\":true,\\\"useRecommendedSetup\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\",\\\"id\\\":\\\"MatchDateAmountGrpbyDateRuleSet\\\"},\\\"restrictions\\\":{\\\"restrictionType\\\":\\\"restricted\\\",\\\"locations\\\":[\\\"1-United States of America\\\",\\\"200-My New Entity\\\"]},\\\"location\\\":{\\\"key\\\":\\\"1\\\",\\\"id\\\":\\\"1-United States of America\\\"}}\", ParameterType.RequestBody);\nIRestResponse response = client.Execute(request);",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_python3",
            "name": "Create a checking account",
            "source": "import http.client\n\nconn = http.client.HTTPSConnection(\"api.intacct.com\")\n\npayload = \"{\\\"id\\\":\\\"BOA\\\",\\\"bankAccountDetails\\\":{\\\"accountNumber\\\":\\\"890000088\\\",\\\"bankName\\\":\\\"Bank of America\\\",\\\"routingNumber\\\":\\\"123456789\\\",\\\"branchId\\\":\\\"00-1355\\\",\\\"phoneNumber\\\":\\\"9007780000\\\",\\\"bankAddress\\\":{\\\"addressLine1\\\":\\\"40714\\\",\\\"addressLine2\\\":\\\"Grimmer Blvd\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"Fremont\\\",\\\"country\\\":\\\"United States\\\",\\\"postCode\\\":\\\"98765\\\",\\\"state\\\":\\\"CA\\\"},\\\"currency\\\":\\\"USD\\\",\\\"accountHolderName\\\":\\\"ACME Software\\\"},\\\"accounting\\\":{\\\"glAccount\\\":{\\\"id\\\":\\\"9090.09.90-RestNextGenGL\\\"},\\\"apJournal\\\":{\\\"id\\\":\\\"AR ADJ-AR Adjustment Journal\\\"},\\\"arJournal\\\":{\\\"id\\\":\\\"RCPT-Receipts Journal\\\"},\\\"bankingTimeZone\\\":\\\"GMT+02:00 Central Europe Summer Time\\\",\\\"serviceChargeAccountLabel\\\":{\\\"key\\\":\\\"15\\\"},\\\"interestAccountLabel\\\":{\\\"id\\\":\\\"Sales\\\"},\\\"disableInterEntityTransfer\\\":false},\\\"checkPrinting\\\":{\\\"addressSettings\\\":{\\\"addressToPrint\\\":\\\"company\\\",\\\"printAddress\\\":true,\\\"printLogo\\\":true,\\\"address\\\":{\\\"addressLine1\\\":\\\"75688 Post st\\\",\\\"addressLine2\\\":\\\"456\\\",\\\"addressLine3\\\":\\\"West Cameron\\\",\\\"city\\\":\\\"San Jose\\\",\\\"country\\\":\\\"United States\\\",\\\"countryCode\\\":\\\"US\\\",\\\"postCode\\\":\\\"94536\\\",\\\"state\\\":\\\"CA\\\",\\\"phone\\\":\\\"6609336532\\\"},\\\"name\\\":\\\"Zine Inc.\\\"},\\\"micrSettings\\\":{\\\"regionalSettings\\\":{\\\"positionOfOnUsSymbol\\\":\\\"position31\\\",\\\"printCode45\\\":true,\\\"printOnUsSymbol\\\":true,\\\"printUSFundsUnderCheckAmount\\\":false},\\\"accountNumberAlignment\\\":\\\"right\\\",\\\"accountNumberPositioning\\\":1,\\\"minCheckNumberLength\\\":\\\"6\\\"},\\\"printSettings\\\":{\\\"additionalText\\\":\\\"Pay to check holder\\\",\\\"paperFormat\\\":\\\"top\\\",\\\"printLineItems\\\":true,\\\"printLocation\\\":\\\"id\\\",\\\"printingFormat\\\":\\\"standard\\\",\\\"nextCheckNumber\\\":\\\"1012\\\",\\\"numberOfChecksInPreview\\\":\\\"One\\\",\\\"printOn\\\":\\\"blankCheckStock\\\"},\\\"signatures\\\":{\\\"firstSignature\\\":\\\"sigimg1_g.gif\\\",\\\"limitForFirstSignatureAmount\\\":\\\"20\\\",\\\"limitForSecondSignatureAmount\\\":\\\"30\\\",\\\"secondSignature\\\":\\\"sigimg2_g.gif\\\",\\\"thresholdForSecondSignatureAmount\\\":\\\"99.00\\\"},\\\"disablePrinting\\\":false},\\\"reconciliation\\\":{\\\"matchSequence\\\":{\\\"key\\\":\\\"48\\\"},\\\"useMatchSequenceForAutoMatch\\\":true,\\\"useMatchSequenceForManualMatch\\\":true},\\\"ach\\\":{\\\"bankId\\\":\\\"ACH-1\\\",\\\"companyName\\\":\\\"origin\\\",\\\"companyIdentification\\\":\\\"originid\\\",\\\"originatingFinancialInstitution\\\":\\\"8909\\\",\\\"companyEntryDescription\\\":\\\"entry desc\\\",\\\"companyDiscretionaryData\\\":\\\"disc\\\",\\\"serviceClassCode\\\":\\\"220\\\",\\\"batchId\\\":\\\"BOA_ACH_BatchNo\\\",\\\"traceNumberSequence\\\":\\\"BOA_ACH_TraceNo\\\",\\\"paymentNumberSequence\\\":\\\"CONTINVOICE\\\",\\\"useTraceNumber\\\":\\\"T\\\",\\\"enableACH\\\":true,\\\"useRecommendedSetup\\\":true},\\\"ruleSet\\\":{\\\"key\\\":\\\"53\\\",\\\"id\\\":\\\"MatchDateAmountGrpbyDateRuleSet\\\"},\\\"restrictions\\\":{\\\"restrictionType\\\":\\\"restricted\\\",\\\"locations\\\":[\\\"1-United States of America\\\",\\\"200-My New Entity\\\"]},\\\"location\\\":{\\\"key\\\":\\\"1\\\",\\\"id\\\":\\\"1-United States of America\\\"}}\"\n\nheaders = { 'Content-Type': \"application/json\" }\n\nconn.request(\"POST\", \"/ia/api/v1/objects/cash-management/checking-account\", payload, headers)\n\nres = conn.getresponse()\ndata = res.read()\n\nprint(data.decode(\"utf-8\"))",
            "mimeType": "application/json",
            "isAutoGenerated": true
          },
          {
            "lang": "python_requests",
            "name": "Create a checking account",
            "source": "import requests\n\nurl = \"https://api.intacct.com/ia/api/v1/objects/cash-management/checking-account\"\n\npayload = {\n    \"id\": \"BOA\",\n    \"bankAccountDetails\": {\n        \"accountNumber\": \"890000088\",\n        \"bankName\": \"Bank of America\",\n        \"routingNumber\": \"123456789\",\n        \"branchId\": \"00-1355\",\n        \"phoneNumber\": \"9007780000\",\n        \"bankAddress\": {\n            \"addressLine1\": \"40714\",\n            \"addressLine2\": \"Grimmer Blvd\",\n            \"addressLine3\": \"West Cameron\",\n            \"city\": \"Fremont\",\n            \"country\": \"United States\",\n            \"postCode\": \"98765\",\n            \"state\": \"CA\"\n        },\n        \"currency\": \"USD\",\n        \"accountHolderName\": \"ACME Software\"\n    },\n    \"accounting\": {\n        \"glAccount\": {\"id\": \"9090.09.90-RestNextGenGL\"},\n        \"apJournal\": {\"id\": \"AR ADJ-AR Adjustment Journal\"},\n        \"arJournal\": {\"id\": \"RCPT-Receipts Journal\"},\n        \"bankingTimeZone\": \"GMT+02:00 Central Europe Summer Time\",\n        \"serviceChargeAccountLabel\": {\"key\": \"15\"},\n        \"interestAccountLabel\": {\"id\": \"Sales\"},\n        \"disableInterEntityTransfer\": False\n    },\n    \"checkPrinting\": {\n        \"addressSettings\": {\n            \"addressToPrint\": \"company\",\n            \"printAddress\": True,\n            \"printLogo\": True,\n            \"address\": {\n                \"addressLine1\": \"75688 Post st\",\n                \"addressLine2\": \"456\",\n                \"addressLine3\": \"West Cameron\",\n                \"city\": \"San Jose\",\n                \"country\": \"United States\",\n                \"countryCode\": \"US\",\n                \"postCode\": \"94536\",\n                \"state\": \"CA\",\n                \"phone\": \"6609336532\"\n            },\n            \"name\": \"Zine Inc.\"\n        },\n        \"micrSettings\": {\n            \"regionalSettings\": {\n                \"positionOfOnUsSymbol\": \"position31\",\n                \"printCode45\": True,\n                \"printOnUsSymbol\": True,\n                \"printUSFundsUnderCheckAmount\": False\n            },\n            \"accountNumberAlignment\": \"right\",\n            \"accountNumberPositioning\": 1,\n            \"minCheckNumberLength\": \"6\"\n        },\n        \"printSettings\": {\n            \"additionalText\": \"Pay to check holder\",\n            \"paperFormat\": \"top\",\n            \"printLineItems\": True,\n            \"printLocation\": \"id\",\n            \"printingFormat\": \"standard\",\n            \"nextCheckNumber\": \"1012\",\n            \"numberOfChecksInPreview\": \"One\",\n            \"printOn\": \"blankCheckStock\"\n        },\n        \"signatures\": {\n            \"firstSignature\": \"sigimg1_g.gif\",\n            \"limitForFirstSignatureAmount\": \"20\",\n            \"limitForSecondSignatureAmount\": \"30\",\n            \"secondSignature\": \"sigimg2_g.gif\",\n            \"thresholdForSecondSignatureAmount\": \"99.00\"\n        },\n        \"disablePrinting\": False\n    },\n    \"reconciliation\": {\n        \"matchSequence\": {\"key\": \"48\"},\n        \"useMatchSequenceForAutoMatch\": True,\n        \"useMatchSequenceForManualMatch\": True\n    },\n    \"ach\": {\n        \"bankId\": \"ACH-1\",\n        \"companyName\": \"origin\",\n        \"companyIdentification\": \"originid\",\n        \"originatingFinancialInstitution\": \"8909\",\n        \"companyEntryDescription\": \"entry desc\",\n        \"companyDiscretionaryData\": \"disc\",\n        \"serviceClassCode\": \"220\",\n        \"batchId\": \"BOA_ACH_BatchNo\",\n        \"traceNumberSequence\": \"BOA_ACH_TraceNo\",\n        \"paymentNumberSequence\": \"CONTINVOICE\",\n        \"useTraceNumber\": \"T\",\n        \"enableACH\": True,\n        \"useRecommendedSetup\": True\n    },\n    \"ruleSet\": {\n        \"key\": \"53\",\n        \"id\": \"MatchDateAmountGrpbyDateRuleSet\"\n    },\n    \"restrictions\": {\n        \"restrictionType\": \"restricted\",\n        \"locations\": [\"1-United States of America\", \"200-My New Entity\"]\n    },\n    \"location\": {\n        \"key\": \"1\",\n        \"id\": \"1-United States of America\"\n    }\n}\nheaders = {\"Content-Type\": \"application/json\"}\n\nresponse = requests.request(\"POST\", url, json=payload, headers=headers)\n\nprint(response.text)",
            "mimeType": "application/json",
            "isAutoGenerated": true
          }
        ]
      }
    }
  },
  "components": {
    "schemas": {
      "objects.cash-management.checking-account": {
        "type": "object",
        "description": "A checking account represents a specific type of cash account used to manage day-to-day transactions, such as vendor payments, customer deposits, payroll and reconciliations.",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned  unique key for the account.",
            "readOnly": true,
            "example": "2"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the account.",
            "example": "BOA"
          },
          "href": {
            "type": "string",
            "description": "URL endpoint for the checking account",
            "readOnly": true,
            "example": "/objects/cash-management/checking-account/62"
          },
          "bankAccountDetails": {
            "type": "object",
            "description": "Bank account details for the account.",
            "properties": {
              "accountNumber": {
                "type": "string",
                "description": "Bank account number for the account.",
                "example": "4356789402",
                "nullable": true
              },
              "bankName": {
                "type": "string",
                "description": "Bank name for the account.",
                "example": "Bank of America"
              },
              "accountHolderName": {
                "type": "string",
                "description": "Account holder name for the account. This is the official name that the bank has on file.",
                "maxLength": 100,
                "example": "ABC Software",
                "nullable": true
              },
              "routingNumber": {
                "type": "string",
                "description": "Routing number for the account, required for issuing payments from the account, regardless of the payment method.",
                "example": "121000358",
                "nullable": true
              },
              "branchId": {
                "type": "string",
                "description": "Identifier for the branch associated with the checking account.",
                "example": "89099",
                "nullable": true
              },
              "phoneNumber": {
                "type": "string",
                "description": "Phone number for the branch associated with the checking account.",
                "example": "5559878978",
                "nullable": true
              },
              "currency": {
                "type": "string",
                "description": "Currency for the account. The default is the base currency for the company or entity. If the account is with a foreign bank, the currency should match the country.",
                "example": "USD"
              },
              "bankAddress": {
                "type": "object",
                "properties": {
                  "city": {
                    "type": "string",
                    "description": "City for the branch associated with the checking account.",
                    "example": "Newark",
                    "nullable": true
                  },
                  "state": {
                    "type": "string",
                    "description": "State for the branch associated with the checking account.",
                    "example": "CA",
                    "nullable": true
                  },
                  "postCode": {
                    "type": "string",
                    "description": "Zip or postal code for the branch associated with the checking account.",
                    "example": "94560",
                    "nullable": true
                  },
                  "country": {
                    "type": "string",
                    "description": "Country for the branch associated with the checking account.",
                    "example": "United States",
                    "nullable": true
                  },
                  "addressLine1": {
                    "type": "string",
                    "description": "First line of the street for the branch associated with the checking account.",
                    "example": "36900 Neward Blvd",
                    "nullable": true
                  },
                  "addressLine2": {
                    "type": "string",
                    "description": "Second line of the street for the branch associated with the checking account.",
                    "example": "Suite 101",
                    "nullable": true
                  },
                  "addressLine3": {
                    "type": "string",
                    "description": "Third line of the street for the branch associated with the checking account.",
                    "example": "Western Industrial Area",
                    "nullable": true
                  }
                }
              }
            }
          },
          "accounting": {
            "type": "object",
            "description": "Specifies the accounting details for the checking account.",
            "properties": {
              "glAccount": {
                "type": "object",
                "description": "General Ledger (GL) account associated with the checking account.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the GL account.",
                    "example": "256"
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the GL account.",
                    "example": "9899 Expense GL Account 33"
                  },
                  "name": {
                    "type": "string",
                    "description": "Name for the GL account.",
                    "readOnly": true,
                    "example": "Expense Account"
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/256"
                  }
                }
              },
              "apJournal": {
                "type": "object",
                "description": "Specifies the default General Ledger (GL) journal for Accounts Payable (AP).",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the AP journal.",
                    "example": "3",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the AP journal.",
                    "example": "AP-ADJ AP Adjustment Journal",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the AP journal.",
                    "example": "/objects/general-ledger/journal/3",
                    "readOnly": true,
                    "nullable": true
                  }
                }
              },
              "arJournal": {
                "type": "object",
                "description": "Specifies the default General Ledger (GL) journal for Accounts Receivable (AR).",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the AR journal.",
                    "example": "3",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the AR journal.",
                    "example": "AR-ADJ AR Adjustment Journal",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the AR journal.",
                    "example": "/objects/general-ledger/journal/3",
                    "readOnly": true,
                    "nullable": true
                  }
                }
              },
              "disableInterEntityTransfer": {
                "type": "boolean",
                "description": "Excludes the checking account from inter-entity transfers (IET) even if IET is globally enabled for the entire multi-entity shared structure of companies.",
                "default": false,
                "example": false
              },
              "serviceChargeGLAccount": {
                "type": "object",
                "description": "Specifies the General Ledger (GL) journal for service charges, used for reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the service charge GL account.",
                    "example": "432",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the service charge GL account.",
                    "example": "0077  Service Charge GL Account 54",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the service charge GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/432",
                    "nullable": true
                  }
                }
              },
              "serviceChargeAccountLabel": {
                "type": "object",
                "description": "Specifies the account label for the service charge GL account.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the service charge GL account label.",
                    "example": "15",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the service charge GL account label.",
                    "example": "Car Payment",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the service charge GL account label.",
                    "readOnly": true,
                    "example": "/objects/accounts-payable/account-label/15",
                    "nullable": true
                  }
                }
              },
              "interestGLAccount": {
                "type": "object",
                "description": "Specifies the General Ledger (GL) journal for earned interest, used for reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the earned interest GL account.",
                    "example": "419",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the earned interest GL account.",
                    "example": "0099 Interest Earned GL Account 40",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the earned interest GL account.",
                    "readOnly": true,
                    "example": "/objects/general-ledger/account/419",
                    "nullable": true
                  }
                }
              },
              "interestAccountLabel": {
                "type": "object",
                "description": "Specifies the account label for the earned interest GL account.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the earned interest GL account label.",
                    "example": "35",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the earned interest GL account label.",
                    "example": "Sales Account",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the earned interest GL account label.",
                    "readOnly": true,
                    "example": "/objects/accounts-receivable/account-label/35",
                    "nullable": true
                  }
                }
              },
              "bankingTimeZone": {
                "type": "string",
                "description": "Determines the time stamp for transactions generated from creation rules and incoming bank feed transactions.",
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
              }
            }
          },
          "reconciliation": {
            "type": "object",
            "description": "Specifies the reconciliation details for the account.",
            "properties": {
              "lastReconciledBalance": {
                "type": "string",
                "description": "Balance of the last reconciliation.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "8970.98",
                "nullable": true
              },
              "lastReconciledDate": {
                "type": "string",
                "format": "date",
                "description": "Date the last reconciliation occurred.",
                "readOnly": true,
                "nullable": true,
                "example": "2019-03-22"
              },
              "cutOffDate": {
                "type": "string",
                "format": "date",
                "description": "Date after which the initial reconciliation can begin. Applies only to accounts not previously reconciled in Sage Intacct.",
                "readOnly": true,
                "example": "2018-01-01",
                "nullable": true
              },
              "inProgressBalance": {
                "type": "string",
                "description": "Balance of the in-progress reconciliation.",
                "format": "decimal-precision-2",
                "readOnly": true,
                "example": "-221021.61",
                "nullable": true
              },
              "inProgressDate": {
                "type": "string",
                "format": "date",
                "description": "Date the in-progress reconciliation occurred.",
                "readOnly": true,
                "example": "2023-01-05",
                "nullable": true
              },
              "matchSequence": {
                "type": "object",
                "description": "Reconciliation match sequence, a document sequence that tracks matches in reconciliation.",
                "properties": {
                  "key": {
                    "type": "string",
                    "description": "Unique key for the match sequence.",
                    "example": "2",
                    "nullable": true
                  },
                  "id": {
                    "type": "string",
                    "description": "Identifier for the match sequence.",
                    "example": "0022 CHASESQ 0033",
                    "nullable": true
                  },
                  "href": {
                    "type": "string",
                    "description": "URL endpoint for the match sequence.",
                    "readOnly": true,
                    "example": "/objects/company-config/document-sequence/2",
                    "nullable": true
                  }
                }
              },
              "useMatchSequenceForAutoMatch": {
                "type": "boolean",
                "default": true,
                "description": "Indicates whether to use sequence number for automatically matched transactions.",
                "example": false
              },
              "useMatchSequenceForManualMatch": {
                "type": "boolean",
                "default": true,
                "description": "Indicates whether to use sequence number for manually matched transactions.",
                "example": false
              }
            }
          },
          "checkPrinting": {
            "type": "object",
            "properties": {
              "disablePrinting": {
                "type": "boolean",
                "default": false,
                "description": "Indicates whether to disable check printing for the account.",
                "example": false
              },
              "addressSettings": {
                "type": "object",
                "description": "Defines the address settings for check printing.",
                "properties": {
                  "printAddress": {
                    "type": "boolean",
                    "default": false,
                    "description": "Indicates whether to print an address on checks:\n\n  - `true` - Prints the address on checks.\n  - `false` - Does not print the address on checks. Set to `false` if you don't want to include an address or when using pre-printed check stock that already includes the address.\n",
                    "example": false
                  },
                  "addressToPrint": {
                    "type": "string",
                    "description": "Specifies the address to print on checks:\n\n  - `company` - Uses the address set for the company.\n  - `custom` - Uses the address defined in the `name` and `address` fields.\n",
                    "enum": [
                      null,
                      "company",
                      "custom"
                    ],
                    "nullable": true,
                    "default": null,
                    "example": "company"
                  },
                  "name": {
                    "type": "string",
                    "description": "Specifies the company name to print on checks from this checking account if `addressToPrint` is set to `custom`.",
                    "maxLength": 100,
                    "example": "Zine Inc.",
                    "nullable": true
                  },
                  "address": {
                    "type": "object",
                    "description": "Specifies the address to print on checks from this checking account if `addressToPrint` is set to `custom`.",
                    "properties": {
                      "addressLine1": {
                        "type": "string",
                        "description": "First line of the street to print on qualifying checks.",
                        "example": "75688 Post st",
                        "nullable": true
                      },
                      "addressLine2": {
                        "type": "string",
                        "description": "Second line of the street to print on qualifying checks.",
                        "example": "East Gwalopak",
                        "nullable": true
                      },
                      "addressLine3": {
                        "type": "string",
                        "description": "Third line of the street to print on qualifying checks.",
                        "example": "456",
                        "nullable": true
                      },
                      "city": {
                        "type": "string",
                        "description": "City to print on qualifying checks.",
                        "example": "San Ramon",
                        "nullable": true
                      },
                      "state": {
                        "type": "string",
                        "description": "State to print on qualifying checks.",
                        "example": "CA",
                        "nullable": true
                      },
                      "postCode": {
                        "type": "string",
                        "description": "Zip or postal code to print on qualifying checks.",
                        "example": "94536",
                        "nullable": true
                      },
                      "country": {
                        "type": "string",
                        "description": "Country to print on qualifying checks.",
                        "example": "United States",
                        "nullable": true
                      },
                      "countryCode": {
                        "type": "string",
                        "description": "ISO country code to print on qualifying checks. When ISO country codes are enabled for a company, both `country` and `countryCode` must be provided.",
                        "example": "US",
                        "nullable": true
                      },
                      "phone": {
                        "type": "string",
                        "description": "Phone number to print on qualifying checks.",
                        "maxLength": 30,
                        "example": "6609336532",
                        "nullable": true
                      }
                    }
                  },
                  "printLogo": {
                    "type": "boolean",
                    "default": false,
                    "description": "Indicates whether to print the company logo on qualifying checks. Requires a logo image file to be uploaded in Sage Intacct.\n\nFor more information, read about [adding logos to checks](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Check_logos) in the Sage Intacct Help Center.\n",
                    "example": false
                  }
                }
              },
              "signatures": {
                "type": "object",
                "description": "Defines the uploaded signature images to print on qualifying checks for the account. By default, signatures are not included on blank or preprinted check stock. \n\nFor more information, read about [check signatures](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Uploading_Your_Check_Signature) in the Sage Intacct Help Center.\n",
                "properties": {
                  "firstSignature": {
                    "type": "string",
                    "description": "Image file name for the first signature.",
                    "example": "sigimg1_j.jpg",
                    "nullable": true
                  },
                  "limitForFirstSignatureAmount": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "Specifies the limit as an amount for printing the first signature on qualifying checks. The first signature is printed on checks for less than this amount, while a manual signature line is printed on checks for more than this amount, or when this field is `null`.\n",
                    "example": "50.00",
                    "nullable": true
                  },
                  "useSecondSignature": {
                    "type": "boolean",
                    "default": false,
                    "description": "Indicates whether to include second signature on qualifying checks. Set to `false` to always show just one signature.",
                    "example": true
                  },
                  "secondSignature": {
                    "type": "string",
                    "description": "Image file name for the second signature.",
                    "example": "sigimg2_j.jpg",
                    "nullable": true
                  },
                  "limitForSecondSignatureAmount": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "Specifies the limit as an amount for printing the second signature on qualifying checks. The second signature is printed on checks for less than this this amount, while a manual signature line is printed on checks for more than this amount, or when this field is `null`. Applies when `useSecondSignature` is set to `true`.\n",
                    "example": "60.00",
                    "nullable": true
                  },
                  "thresholdForSecondSignatureAmount": {
                    "type": "string",
                    "format": "decimal-precision-2",
                    "description": "Specifies the threshold as an amount for printing the second signature on qualifying checks. The second signature is printed on checks for more than this amount.",
                    "example": "60.00",
                    "nullable": true
                  }
                }
              },
              "printSettings": {
                "type": "object",
                "description": "Specifies the check printing settings for the account.",
                "properties": {
                  "printOn": {
                    "type": "string",
                    "description": "Indicates the check stock to use for printing checks for the account:  \n\n- `prePrintedCheckStock` - Uses check paper that is pre-printed with company information.\n- `blankCheckStock` - Uses blank check paper.\n",
                    "enum": [
                      "prePrintedCheckStock",
                      "blankCheckStock"
                    ],
                    "example": "blankCheckStock",
                    "default": "blankCheckStock"
                  },
                  "nextCheckNumber": {
                    "type": "string",
                    "description": "Specifies the starting check number to use when printing checks for the account, for example 1001. Check numbers increment by one for each succeeding check printed.\n",
                    "maxLength": 10,
                    "pattern": "^[0-9]{1,10}$",
                    "example": "1012",
                    "nullable": true
                  },
                  "printingFormat": {
                    "type": "string",
                    "description": "Specifies the format to use when printing checks for the account:\n\n\n\n\n\n  - `standard` - For pre-printed checks that already show the bank account number, routing number, and check numbers. Not available for CAD checking accounts.\n  - `business` - Prints amounts in a font that makes alterations difficult (for security).\n  - `highSecurity` - Same as `standard`, plus features that reduce fraud related to check washing, forgery, and copying. Not available for CAD checking accounts.\n  - `cadCheck` - Prints checks with dates formatted for Canadian companies, can be used with CAD and USD checking accounts.\n  - `jpmorganChaseBusiness` - For USD checking accounts with business checks where the Pay to the order of field is not above the Amount field but is next to the Vendor address.\n  - `jpmorganChaseStandard` - For USD checking accounts with standard checks where the Pay to the order of field is not above the Amount field but is next to the Vendor address.\n",
                    "enum": [
                      "standard",
                      "business",
                      "highSecurity",
                      "cadCheck",
                      "jpmorganChaseBusiness",
                      "jpmorganChaseStandard"
                    ],
                    "example": "standard",
                    "default": "standard"
                  },
                  "paperFormat": {
                    "type": "string",
                    "description": "Specifies the location for check printing on three-part forms: top, middle, or bottom panel. Pre-printed check stock is only compatible with the top and middle printing position.\n\nCanadian check stock is only compatible with the top printing position.\n",
                    "enum": [
                      "top",
                      "middle",
                      "bottom"
                    ],
                    "example": "top",
                    "default": "top"
                  },
                  "printLineItems": {
                    "type": "boolean",
                    "description": "Indicates whether to include additional fields in the non-remittance panel of checks. These fields include columns for the account, department, and location of each line item. A check can include up to 18 line items per page in either summary or detail mode.\n",
                    "default": false,
                    "example": true
                  },
                  "printLocation": {
                    "type": "string",
                    "description": "Specifies the location information to include on checks:\n\n  - `id` - Print the location identifier only in the location column.\n  - `name` - Print the location name only in the location column.\n  - `both`(default) - Print both the location identifier and the location name in the location column.\n",
                    "enum": [
                      "id",
                      "name",
                      "both"
                    ],
                    "example": "id",
                    "default": "id"
                  },
                  "additionalText": {
                    "type": "string",
                    "description": "Specifies additional text to print under the signatures.",
                    "example": "Pay to check holder",
                    "nullable": true
                  },
                  "numberOfChecksInPreview": {
                    "type": "string",
                    "description": "Specifies the number of checks per page to preview before printing.",
                    "enum": [
                      null,
                      "one",
                      "three"
                    ],
                    "example": "one",
                    "nullable": true,
                    "default": null
                  }
                }
              },
              "micrSettings": {
                "type": "object",
                "description": "Specifies the Magnetic Ink Character Recognition (MICR) settings. MICR format is a widely adopted bank standard for blank check stock, standardizing the appearance of the routing, account, and other numbers at the bottom of every check. Use these settings if your bank requires specific horizontal alignment of the account number on the MICR line on the printed check. \n\nFor more information, read the [MICR printing guidelines](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=MICR_information_on_checks) in the Sage Intacct Help Center.\n",
                "properties": {
                  "accountNumberAlignment": {
                    "type": "string",
                    "description": "Specifies how the bank account number is aligned on the MICR line.",
                    "enum": [
                      "left",
                      "right"
                    ],
                    "example": "right",
                    "default": "right"
                  },
                  "accountNumberPositioning": {
                    "type": "integer",
                    "description": "Specifies the positioning of the bank account number on the MICR line, defined by the number of spaces added before or after the account number.",
                    "example": 1,
                    "nullable": true
                  },
                  "minCheckNumberLength": {
                    "type": "string",
                    "description": "Specifies the required length for check numbers on the MICR line. Minimum check number length is six digits, shorter check numbers are left-padded with zeros.\n",
                    "example": "6",
                    "nullable": true
                  },
                  "regionalSettings": {
                    "type": "object",
                    "description": "Specifies the regional settings for MICR.",
                    "properties": {
                      "printCode45": {
                        "type": "boolean",
                        "default": false,
                        "description": "Indicates whether to print the transaction code 45 on the MICR line.",
                        "example": true
                      },
                      "printUSFundsUnderCheckAmount": {
                        "type": "boolean",
                        "default": false,
                        "description": "Indicates whether to print US funds under the check amount box for CPA member banks.",
                        "example": true
                      },
                      "printOnUsSymbol": {
                        "type": "boolean",
                        "default": false,
                        "description": "Indicates whether to print the ON-US symbol in front of the account number on the MICR line.",
                        "example": true
                      },
                      "positionOfOnUsSymbol": {
                        "type": "string",
                        "description": "Specifies the position of the ON-US symbol on the MICR line. To position the symbol before the checking account number, set to `position31` or `position32`.",
                        "enum": [
                          "position31",
                          "position32"
                        ],
                        "example": "position31",
                        "default": "position31"
                      }
                    }
                  }
                }
              }
            }
          },
          "department": {
            "type": "object",
            "description": "Specifies the department to use for General Ledger (GL) posting (optional).",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the department.",
                "example": "9",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Identifier for the department.",
                "example": "11-Accounting",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the department.",
                "readOnly": true,
                "example": "/objects/company-config/department/9",
                "nullable": true
              }
            }
          },
          "location": {
            "type": "object",
            "description": "Specifies the location to use for General Ledger (GL) posting (optional).",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the location.",
                "example": "1"
              },
              "id": {
                "type": "string",
                "description": "Identifier for the location.",
                "example": "001-United States of America"
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the location.",
                "readOnly": true,
                "example": "/objects/company-config/location/1"
              }
            }
          },
          "status": {
            "$ref": "#/components/schemas/status"
          },
          "ach": {
            "type": "object",
            "description": "Specifies the Automated Clearing House (ACH) details.\n\nFor more information, read about [setting up a checking account for ACH payments](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_account_setup) in Sage Intacct Help Center.\n",
            "properties": {
              "enableACH": {
                "type": "boolean",
                "description": "Indicates whether to to enable the account to make standard ACH or American Express ACH Payment Services payments.",
                "default": false,
                "example": true
              },
              "bankId": {
                "type": "string",
                "description": "Identifier for the bank, as specified in the ACH bank record.",
                "example": "BOA_ACH",
                "nullable": true
              },
              "companyName": {
                "type": "string",
                "description": "Name of the company, as specified in the ACH bank record.",
                "maxLength": 16,
                "example": "Ventura",
                "nullable": true
              },
              "companyIdentification": {
                "type": "string",
                "description": "Specifies the 10-digit identifier (including hyphens) for the company, as specified in the ACH bank record.",
                "maxLength": 10,
                "pattern": "^[\\w\\s_\\-\\.]{0,20}$",
                "example": "Ventura",
                "nullable": true
              },
              "originatingFinancialInstitution": {
                "type": "string",
                "description": "References the first eight digits of the routing number for the bank, as specified in the ACH bank record.",
                "maxLength": 8,
                "pattern": "^[0-9]{1,8}$",
                "example": "89096789",
                "nullable": true
              },
              "companyEntryDescription": {
                "type": "string",
                "description": "Indicates optional text that can be included with ACH payments.",
                "maxLength": 10,
                "example": "Investment",
                "nullable": true
              },
              "companyDiscretionaryData": {
                "type": "string",
                "description": "Specifies additional information that can be included with ACH payments. Typically this will consist of codes (unique to each bank) that describe any special handling of entries.",
                "maxLength": 20,
                "example": "89078900",
                "nullable": true
              },
              "useRecommendedSetup": {
                "type": "boolean",
                "description": "Indicates whether to automatically generate the ACH payment file, with `serviceClassCode` set to `220` (credits only), and set up numbering sequences for standard ACH payments.",
                "default": false,
                "example": true
              },
              "recordTypeCode": {
                "type": "string",
                "description": "Indicates the record type code for ACH payments.",
                "maxLength": 1,
                "default": "5",
                "readOnly": true,
                "example": "5",
                "nullable": true
              },
              "serviceClassCode": {
                "type": "string",
                "description": "Specifies the service class code for ACH payments. Use `220` for payments (credits) only, or `200` for both credits and debits. If using `200`, then `useRecommendedSetup` must be `false`.",
                "enum": [
                  null,
                  "220",
                  "200"
                ],
                "nullable": true,
                "default": null,
                "example": "220"
              },
              "originatorStatusCode": {
                "type": "string",
                "description": "Indicates the originator status code for ACH payments.",
                "default": "1",
                "readOnly": true,
                "maxLength": 1,
                "pattern": "^[0-9]{1}$",
                "example": "6",
                "nullable": true
              },
              "batchId": {
                "type": "string",
                "description": "Identifies the number sequence that automatically numbers payment batches. The batch number must be 7-digits, with no prefixes or suffixes. Required if `useRecommendedSetup` is `false`. \n",
                "example": "BOA_ACH_BatchNo",
                "nullable": true
              },
              "traceNumberSequence": {
                "type": "string",
                "description": "Identifies the number sequence that generates the trace number for ACH entries. The trace number is formed by concatenating the bank routing number with a 7-digit sequence number, and has no prefixes or suffixes. Required if `useRecommendedSetup` is `false`. \n",
                "example": "BOA_ACH_TraceNo",
                "nullable": true
              },
              "paymentNumberSequence": {
                "type": "string",
                "description": "Identifies the number sequence that assigns a unique payment number to confirmed Accounts Payable (AP) payments. You can use the same number sequence specified in `traceNumberSequence`. Required if `useRecommendedSetup` is `false`.\n",
                "example": "BOA_ACH_PayNo",
                "nullable": true
              },
              "useTraceNumber": {
                "type": "string",
                "description": "Indicates whether to use the trace number as a payment (`useAsPayment`) or as a numbering sequence (`useNumberSequence`).",
                "enum": [
                  null,
                  "useAsPayment",
                  "useNumberingSequence"
                ],
                "nullable": true,
                "default": "useAsPayment",
                "example": "useAsPayment"
              }
            }
          },
          "bankFile": {
            "type": "object",
            "description": "Specifies the bank file details for companies subscribed to Sage Cloud Services and enabled for bank file payments. A bank file is a standard file used by banks to make multiple payments, they enable your company to pay vendors using international checking accounts.\n\nFor more information, read about [setting up a checking account for bank file payments](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_account_setup) in Sage Intacct Help Center.\n",
            "properties": {
              "enableBankFile": {
                "type": "boolean",
                "description": "Indicates whether to enable bank file payments for checking accounts in supported countries.",
                "default": false,
                "example": true
              },
              "bankFileFormat": {
                "type": "string",
                "description": "Specifies the bank file format of the bank associated with the checking account. \n\nFor more information, read about [bank files](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Bank_file_payment) in the Sage Intacct Help Center.\n",
                "example": "ABA - Westpac",
                "nullable": true
              },
              "bankCode": {
                "type": "string",
                "description": "Specifies the bank code for the bank associated with the checking account.",
                "example": "Westpac Banking Corporation",
                "nullable": true
              },
              "apcaNumber": {
                "type": "string",
                "description": "Six-digit identifier for the company or individual, used by Australian banks to make direct payments.",
                "example": "865551",
                "nullable": true
              },
              "bsbNumber": {
                "type": "string",
                "description": "Six-digit identifier for an individual branch of a financial institution in Australia, expressed as two groups of three separated by a hyphen.",
                "example": "042-457",
                "nullable": true
              },
              "sunNumber": {
                "type": "string",
                "description": "Unique identifier (optional) for organizations that collect payment with bank files. The service user number, together with the bank file, creates a record of the transaction. For HSBC customers only.",
                "example": "6789",
                "nullable": true
              },
              "sortCode": {
                "type": "string",
                "description": "Six-digit identifier for the UK bank and branch where the checking account is held, expressed as three groups of two separated by hyphens.",
                "maxLength": 10,
                "nullable": true,
                "example": "23-44-16"
              },
              "seedValue": {
                "type": "string",
                "description": "Specifies the 32-character encryption key (seed value) issued by NedBank, used to generate and validate secure payment files.",
                "maxLength": 40,
                "nullable": true,
                "example": "ABFGHETOUFEH1234IOIADRTO78DD899"
              },
              "userReference": {
                "type": "string",
                "description": "Specifies the 10-character reference supplied by Standard Bank, this reference is used on bank statements.",
                "maxLength": 10,
                "nullable": true,
                "example": "SBXXSHRTNA"
              },
              "clientCode": {
                "type": "string",
                "description": "User code that identifies the client to Standard Bank.",
                "maxLength": 10,
                "nullable": true,
                "example": "ProLite"
              },
              "serviceType": {
                "type": "string",
                "description": "Indicates the type of Bulk Electronic Fund Transfer (BEFT) service to use.",
                "maxLength": 10,
                "nullable": true,
                "example": "PAYMENT"
              },
              "originatorId": {
                "type": "string",
                "description": "Identifier for the originator.",
                "maxLength": 20,
                "example": "IE26SCT803015",
                "nullable": true
              },
              "businessIdCode": {
                "type": "string",
                "description": "Identifier for the business.",
                "maxLength": 20,
                "example": "BOFIIE2DXXX",
                "nullable": true
              },
              "processingDataCenterCode": {
                "type": "string",
                "maxLength": 5,
                "description": "Specifies the 5-digit identifier for the originating direct clearer.",
                "example": "01674",
                "nullable": true
              },
              "debtorBankNumber": {
                "type": "string",
                "maxLength": 3,
                "description": "Identifier for the settlement institutional bank (processing bank).",
                "example": "674",
                "nullable": true
              },
              "branchTransitNumber": {
                "type": "string",
                "maxLength": 5,
                "description": "Branch transit number for the settlement institutional bank (processing bank).",
                "example": "43876",
                "nullable": true
              },
              "returnAccountNumber": {
                "type": "string",
                "maxLength": 12,
                "description": "Specifies the return account number for the checking account.",
                "example": "IE26SCT80301",
                "nullable": true
              },
              "messageIdPrefix": {
                "type": "string",
                "maxLength": 23,
                "description": "Specifies a 23-character identifier for each submitted payment file, specific to the Bank of Ireland SEPA bank file format, combining a customer-defined prefix with a system-generated 12-digit date/time stamp.\n",
                "example": "SEPA240212",
                "nullable": true
              },
              "immediateDestinationId": {
                "type": "string",
                "maxLength": 9,
                "description": "Bank routing number for the institution receiving the payment file.",
                "example": "984569845",
                "nullable": true
              },
              "immediateOriginId": {
                "type": "string",
                "maxLength": 9,
                "description": "Bank routing number for the institution sending the payment file.",
                "example": "878767675",
                "nullable": true
              },
              "immediateOriginName": {
                "type": "string",
                "maxLength": 50,
                "description": "Name of the company sending the payment file.",
                "example": "Investment Corporation",
                "nullable": true
              },
              "immediateDestinationName": {
                "type": "string",
                "maxLength": 50,
                "description": "Name of the company receiving the payment file.",
                "example": "BOA",
                "nullable": true
              },
              "companyEntryDescription": {
                "type": "string",
                "description": "Indicates optional text used to describe the transaction, for example, Payroll or Payables, to be included with payments.",
                "maxLength": 10,
                "example": "Investment",
                "nullable": true
              },
              "companyName": {
                "type": "string",
                "description": "Company name for the checking account.",
                "maxLength": 16,
                "example": "Ventura",
                "nullable": true
              },
              "paymentNumberSequence": {
                "type": "string",
                "description": "Specifies the number sequence that generates a unique payment number for payment files uploaded to the bank. You can use the same number sequence specified in `traceNumberSequence`. Required if `useRecommendedSetup` is `false`.\n",
                "maxLength": 7,
                "example": "0000078",
                "nullable": true
              },
              "fileIdSequence": {
                "type": "string",
                "description": "Specifies the sequence used to identify payment files generated each calendar day. The first file generated for each calendar day starts with `A`. Each subsequent file increments alphabetically, then numerically, and resets at the start of the next calendar day.\n",
                "maxLength": 1,
                "pattern": "^[A-Za-z0-9]$",
                "example": "B",
                "nullable": true
              },
              "postalAddress": {
                "type": "object",
                "description": "Postal address for the bank.",
                "properties": {
                  "addressLine1": {
                    "type": "string",
                    "description": "First address line for the bank.",
                    "example": "36900 Neward Blvd",
                    "nullable": true
                  },
                  "addressLine2": {
                    "type": "string",
                    "description": "Second address line for the bank.",
                    "example": "Suite 100",
                    "nullable": true
                  },
                  "postCode": {
                    "type": "string",
                    "description": "Postal code for the bank.",
                    "example": "94536",
                    "nullable": true
                  },
                  "county": {
                    "type": "string",
                    "description": "County for the bank.",
                    "example": "Alameda",
                    "nullable": true
                  },
                  "countryCode": {
                    "type": "string",
                    "description": "ISO country code for the bank.",
                    "readOnly": true,
                    "example": "US",
                    "nullable": true
                  }
                }
              }
            }
          },
          "audit": {
            "$ref": "#/components/schemas/audit.s1",
            "readOnly": true
          },
          "bankingCloudConnection": {
            "$ref": "#/components/schemas/banking-cloud-connection"
          },
          "financialInstitution": {
            "description": "financial-institutionref",
            "type": "object",
            "properties": {
              "id": {
                "type": "string",
                "description": "Identifier for the financial institution.",
                "readOnly": true,
                "example": "1",
                "nullable": true
              },
              "key": {
                "type": "string",
                "description": "Unique key for the financial institution.",
                "readOnly": true,
                "example": "FINTTEC4",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the financial institution.",
                "readOnly": true,
                "example": "/objects/cash-management/financial-institution/1",
                "nullable": true
              }
            },
            "readOnly": true
          },
          "entity": {
            "$ref": "#/components/schemas/entity-ref"
          },
          "ruleSet": {
            "type": "object",
            "description": "Specifies the rule set this account uses to match incoming transactions for reconciliation from a bank feed or import file. You can't reconcile an account with a bank feed or import file without a rule set.",
            "properties": {
              "key": {
                "type": "string",
                "description": "Unique key for the rule set.",
                "example": "36",
                "nullable": true
              },
              "id": {
                "type": "string",
                "description": "Identifier for the rule set.",
                "example": "36-RuleSetToMatch",
                "nullable": true
              },
              "name": {
                "type": "string",
                "description": "Name for the rule set.",
                "readOnly": true,
                "example": "RULE-SET-CHECKING-ACCOUNTS",
                "nullable": true
              },
              "href": {
                "type": "string",
                "description": "URL endpoint for the rule set.",
                "readOnly": true,
                "example": "/objects/cash-management/bank-txn-rule-set/2",
                "nullable": true
              }
            }
          },
          "restrictions": {
            "type": "object",
            "description": "Specifies the restriction type, along with the entities and locations allowed to use the checking account for making payments. \n\nFor more information, read about [restricting a bank account](https://www.intacct.com/ia/docs/en_US/help_action/Default.htm#cshid=Restrict_a_bank_account) in the Sage Intacct Help Center.\n",
            "properties": {
              "restrictionType": {
                "type": "string",
                "description": "Specify which entities and/or locations can access and use the checking account.\n\n- `unrestricted` (default) - the account is available to the top-level company and all entity-level locations.\n- `rootOnly` - Only the top-level company of a multi-entity structure can access the account.\n- `restricted` - Only specified locations, location groups, departments, or department groups can access the account.\n",
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
                "description": "List of locations that can access the checking account when `restrictionType` is set to `restricted`.",
                "items": {
                  "type": "string"
                },
                "example": [
                  "001-United States of America",
                  "002-United Kingdom"
                ]
              }
            }
          },
          "paymentProviderBankAccounts": {
            "type": "array",
            "items": {
              "$ref": "#/components/schemas/objects.cash-management.payment-provider-bank-account"
            }
          }
        }
      },
      "cash-management-checking-accountRequiredProperties": {
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
      "objects.cash-management.payment-provider-bank-account": {
        "type": "object",
        "description": "Links a bank account with the specified payment provider as part of electronic payments setup.",
        "properties": {
          "key": {
            "type": "string",
            "description": "System-assigned key for the provider bank account.",
            "readOnly": true,
            "example": "19"
          },
          "id": {
            "type": "string",
            "description": "Unique identifier for the provider bank account.",
            "readOnly": true,
            "example": "19"
          },
          "state": {
            "type": "string",
            "description": "Subscription status provided by the payment provider.",
            "readOnly": true,
            "example": "requestInitiated",
            "enum": [
              "requestInitiated",
              "inProgress",
              "requestReceived",
              "requestFailed",
              "awaitingAuthorization",
              "subscribed",
              "canceled",
              "suspended"
            ]
          },
          "providerReferenceNumber": {
            "type": "string",
            "description": "Reference number specific to the payment provider, which is a combination of cny#, bank account key, and timestamp.",
            "readOnly": true,
            "example": "44397977-49-1635423682"
          },
          "authenticationURL": {
            "type": "string",
            "description": "Authentication URL specific to the payment provider. This is sent by the payment provider after successful subscription.",
            "readOnly": true,
            "example": "https://myauthurl.example.com/subscription/authenticate"
          },
          "checkStartNumber": {
            "type": "string",
            "description": "Starting check number specific to the payment provider.",
            "example": "111999"
          },
          "isRebateAccount": {
            "type": "boolean",
            "description": "Indicates whether this is the account in which virtual card payment rebates are deposited.",
            "default": false,
            "example": true
          },
          "remittanceEmail": {
            "type": "string",
            "description": "Email address to receive the bank remittance.",
            "example": "jsmith@example.com"
          },
          "bankAccount": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the bank account.",
                "example": "72"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the bank account.",
                "example": "BOA"
              },
              "currency": {
                "type": "string",
                "description": "Type of currency for the bank account.",
                "example": "USD",
                "readOnly": true
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/cash-management/checking-account/72"
              }
            }
          },
          "paymentProvider": {
            "type": "object",
            "properties": {
              "key": {
                "type": "string",
                "description": "System-assigned key for the payment provider.",
                "example": "1"
              },
              "id": {
                "type": "string",
                "description": "Unique identifier for the payment provider.",
                "example": "CSI"
              },
              "href": {
                "type": "string",
                "readOnly": true,
                "example": "/objects/cash-management/payment-provider/1"
              }
            }
          },
          "href": {
            "type": "string",
            "readOnly": true,
            "example": "/objects/cash-management/payment-provider-bank-account/19"
          },
          "audit": {
            "$ref": "#/components/schemas/audit.s1"
          },
          "status": {
            "$ref": "#/components/schemas/status"
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
