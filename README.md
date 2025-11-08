# ELT-SC

{
    "name": "multiprocessing",
    "properties": {
        "activities": [
            {
                "name": "GetFileMetadata_copy1",
                "type": "GetMetadata",
                "dependsOn": [],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "dataset": {
                        "referenceName": "mutipipelineraw",
                        "type": "DatasetReference",
                        "parameters": {
                            "pathName": {
                                "value": "@pipeline().parameters.folderPath",
                                "type": "Expression"
                            },
                            "fileName": "@pipeline().parameters.fileName"
                        }
                    },
                    "fieldList": [
                        "lastModified"
                    ],
                    "storeSettings": {
                        "type": "AzureBlobStorageReadSettings",
                        "recursive": true,
                        "enablePartitionDiscovery": false
                    },
                    "formatSettings": {
                        "type": "DelimitedTextReadSettings"
                    }
                }
            },
            {
                "name": "CheckIfAlreadyProcessed_copy1",
                "type": "Lookup",
                "dependsOn": [
                    {
                        "activity": "GetFileMetadata_copy1",
                        "dependencyConditions": [
                            "Completed"
                        ]
                    }
                ],
                "policy": {
                    "timeout": "0.12:00:00",
                    "retry": 0,
                    "retryIntervalInSeconds": 30,
                    "secureOutput": false,
                    "secureInput": false
                },
                "userProperties": [],
                "typeProperties": {
                    "source": {
                        "type": "AzureSqlSource",
                        "sqlReaderQuery": {
                            "value": "@concat(\n    'SELECT * FROM [dbo].[ProcessedFilesLog] WHERE FileName = ''',\n    pipeline().parameters.fileName,\n    ''' AND Status = ''\"Processed\"'''\n)",
                            "type": "Expression"
                        },
                        "queryTimeout": "02:00:00",
                        "partitionOption": "None"
                    },
                    "dataset": {
                        "referenceName": "AzureSqlTable1",
                        "type": "DatasetReference"
                    },
                    "firstRowOnly": false
                }
            },
            {
                "name": "IsFileAlreadyProcessed_copy1",
                "type": "IfCondition",
                "dependsOn": [
                    {
                        "activity": "CheckIfAlreadyProcessed_copy1",
                        "dependencyConditions": [
                            "Completed"
                        ]
                    }
                ],
                "userProperties": [],
                "typeProperties": {
                    "expression": {
                        "value": "@greater(length(activity('CheckIfAlreadyProcessed_copy1').output.value), 0)",
                        "type": "Expression"
                    },
                    "ifFalseActivities": [
                        {
                            "name": "LogAsProcessed_copy1",
                            "type": "Script",
                            "dependsOn": [],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "linkedServiceName": {
                                "referenceName": "AzureSqlDatabase1",
                                "type": "LinkedServiceReference"
                            },
                            "typeProperties": {
                                "scripts": [
                                    {
                                        "parameters": [
                                            {
                                                "name": "fileName",
                                                "type": "String",
                                                "value": {
                                                    "value": "@pipeline().parameters.fileName",
                                                    "type": "Expression"
                                                },
                                                "direction": "Input"
                                            },
                                            {
                                                "name": "status",
                                                "type": "String",
                                                "value": "\"Processed\"",
                                                "direction": "Input"
                                            },
                                            {
                                                "name": "last_modified",
                                                "type": "Datetimeoffset",
                                                "value": {
                                                    "value": "@activity('GetFileMetadata_copy1').output.lastModified",
                                                    "type": "Expression"
                                                },
                                                "direction": "Input"
                                            }
                                        ],
                                        "type": "NonQuery",
                                        "text": {
                                            "value": "INSERT INTO [dbo].[ProcessedFilesLog] (FileName, LastModified, ProcessedTime, Status)\nVALUES (\n  @fileName,\n  @last_modified,\n  GETDATE(),\n  @status\n)",
                                            "type": "Expression"
                                        }
                                    }
                                ],
                                "scriptBlockExecutionTimeout": "02:00:00"
                            }
                        },
                        {
                            "name": "CopyToProcessed_copy1",
                            "type": "Copy",
                            "dependsOn": [
                                {
                                    "activity": "LogAsProcessed_copy1",
                                    "dependencyConditions": [
                                        "Completed"
                                    ]
                                }
                            ],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "source": {
                                    "type": "DelimitedTextSource",
                                    "storeSettings": {
                                        "type": "AzureBlobStorageReadSettings",
                                        "recursive": true,
                                        "wildcardFolderPath": "csv",
                                        "wildcardFileName": {
                                            "value": "@pipeline().parameters.fileName",
                                            "type": "Expression"
                                        },
                                        "enablePartitionDiscovery": false
                                    },
                                    "formatSettings": {
                                        "type": "DelimitedTextReadSettings",
                                        "skipLineCount": 0
                                    }
                                },
                                "sink": {
                                    "type": "DelimitedTextSink",
                                    "storeSettings": {
                                        "type": "AzureBlobStorageWriteSettings",
                                        "copyBehavior": "PreserveHierarchy"
                                    },
                                    "formatSettings": {
                                        "type": "DelimitedTextWriteSettings",
                                        "quoteAllText": true,
                                        "fileExtension": ".csv"
                                    }
                                },
                                "enableStaging": false,
                                "translator": {
                                    "type": "TabularTranslator",
                                    "typeConversion": true,
                                    "typeConversionSettings": {
                                        "allowDataTruncation": true,
                                        "treatBooleanAsNumber": false
                                    }
                                }
                            },
                            "inputs": [
                                {
                                    "referenceName": "mutipipelineraw",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "pathName": "@pipeline().parameters.folderPath",
                                        "fileName": {
                                            "value": "@pipeline().parameters.fileName",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ],
                            "outputs": [
                                {
                                    "referenceName": "multiprocessed",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "filePath": "processed/csv",
                                        "fileName": {
                                            "value": "@pipeline().parameters.fileName",
                                            "type": "Expression"
                                        }
                                    }
                                }
                            ]
                        },
                        {
                            "name": "transformation_notebook_copy1",
                            "type": "DatabricksNotebook",
                            "dependsOn": [
                                {
                                    "activity": "CopyToProcessed_copy1",
                                    "dependencyConditions": [
                                        "Completed"
                                    ]
                                }
                            ],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "notebookPath": "/Users/techionik1@gmail.com/transformation_book",
                                "baseParameters": {
                                    "file_name": {
                                        "value": "@pipeline().parameters.fileName",
                                        "type": "Expression"
                                    }
                                }
                            },
                            "linkedServiceName": {
                                "referenceName": "databricks_ls",
                                "type": "LinkedServiceReference"
                            }
                        }
                    ],
                    "ifTrueActivities": [
                        {
                            "name": "LogAlreadyProcessed_copy1",
                            "type": "Script",
                            "dependsOn": [],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "linkedServiceName": {
                                "referenceName": "AzureSqlDatabase1",
                                "type": "LinkedServiceReference"
                            },
                            "typeProperties": {
                                "scripts": [
                                    {
                                        "parameters": [
                                            {
                                                "name": "fileName",
                                                "type": "String",
                                                "value": {
                                                    "value": "@pipeline().parameters.fileName",
                                                    "type": "Expression"
                                                },
                                                "direction": "Input"
                                            },
                                            {
                                                "name": "status",
                                                "type": "String",
                                                "value": "\"Already Processed\"",
                                                "direction": "Input"
                                            },
                                            {
                                                "name": "last_modified",
                                                "type": "Datetimeoffset",
                                                "value": {
                                                    "value": "@activity('GetFileMetadata_copy1').output.lastModified",
                                                    "type": "Expression"
                                                },
                                                "direction": "Input"
                                            }
                                        ],
                                        "type": "NonQuery",
                                        "text": {
                                            "value": "INSERT INTO [dbo].[ProcessedFilesLog] (FileName, LastModified, ProcessedTime, Status)\nVALUES (\n  @fileName,\n  @last_modified,\n  GETDATE(),\n  @status\n)",
                                            "type": "Expression"
                                        }
                                    }
                                ],
                                "scriptBlockExecutionTimeout": "02:00:00"
                            }
                        },
                        {
                            "name": "Delete1_copy1",
                            "type": "Delete",
                            "dependsOn": [
                                {
                                    "activity": "LogAlreadyProcessed_copy1",
                                    "dependencyConditions": [
                                        "Completed"
                                    ]
                                }
                            ],
                            "policy": {
                                "timeout": "0.12:00:00",
                                "retry": 0,
                                "retryIntervalInSeconds": 30,
                                "secureOutput": false,
                                "secureInput": false
                            },
                            "userProperties": [],
                            "typeProperties": {
                                "dataset": {
                                    "referenceName": "mutipipelineraw",
                                    "type": "DatasetReference",
                                    "parameters": {
                                        "pathName": "@pipeline().parameters.folderPath",
                                        "fileName": "@pipeline().parameters.fileName"
                                    }
                                },
                                "enableLogging": false,
                                "storeSettings": {
                                    "type": "AzureBlobStorageReadSettings",
                                    "recursive": true,
                                    "enablePartitionDiscovery": false
                                }
                            }
                        }
                    ]
                }
            }
        ],
        "parameters": {
            "folderPath": {
                "type": "string"
            },
            "fileName": {
                "type": "string"
            }
        },
        "annotations": [],
        "lastPublishTime": "2025-05-29T13:06:48Z"
    },
    "type": "Microsoft.DataFactory/factories/pipelines"
}
