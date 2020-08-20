import json
from google.cloud import bigquery

def getSchema():
    bigquerySchema = []
    with open('./schema.json') as sf:
        bigqueryColumns = json.load(sf)
        for col in bigqueryColumns:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type']))
    return(bigquerySchema)

def createTable(bqDataset, bqExternalTable, fileLocation):
    bigqueryClient = bigquery.Client()
    extConfig = bigquery.ExternalConfig("CSV")
    extConfig.source_uris = [fileLocation]
    extConfig.options.skip_leading_rows = 1

    bqSchema = getSchema()
    tableRef = bigqueryClient.dataset(bqDataset).table(bqExternalTable)
    table = bigquery.Table(tableRef, schema=bqSchema)
    table.external_data_configuration = extConfig
    table = bigqueryClient.create_table(table, exists_ok=True)
    return(0)

def deleteTable(bqDataset, bqExternalTable):
    bigqueryClient = bigquery.Client()
    tableRef = bigqueryClient.dataset(bqDataset).table(bqExternalTable)
    table = bigquery.Table(tableRef)
    table = bigqueryClient.delete_table(table, not_found_ok=True)
    return(0)

def insertData(bqDataset, bqExternalTable, bqTargetTable, bqColumnList):
    bigqueryClient = bigquery.Client()
    bqExtTable = bqDataset + "." + bqExternalTable
    bqTxnTable = bqDataset + "." + bqTargetTable

    insertQry = """INSERT INTO {}({})
                   SELECT et.*, CURRENT_TIMESTAMP()
                   FROM {} et""".format(bqTxnTable, bqColumnList, bqExtTable)
    sqlJob = bigqueryClient.query(insertQry)
    sqlJob.result()
    return(0)