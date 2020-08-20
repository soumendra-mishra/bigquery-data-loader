import bqlib
import json

def bqDataload(event, context):
    with open('./config.json') as cf:
        configList = json.load(cf)
    
    bucketName = event['bucket']
    bqDataset = configList["datasetName"]
    bqExternalTable = configList["externalTableName"]
    bqTargetTable = configList["targetTableName"]
    bqColumnList = configList["targetColumnList"]
    fileLocation = "gs://" + bucketName + "/*.csv"

    # Delete External Table
    bqlib.deleteTable(bqDataset, bqExternalTable)

    # Create External Table
    bqlib.createTable(bqDataset, bqExternalTable, fileLocation)

    # Data Load from External Table
    bqlib.insertData(bqDataset, bqExternalTable, bqTargetTable, bqColumnList)

    print("Data loaded successfully...")