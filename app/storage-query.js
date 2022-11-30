const fs = require('fs')
const { TableClient, odata } = require('@azure/data-tables')
const { storageConnectionString, storageTableName } = require('./config')
const tableClient = TableClient.fromConnectionString(storageConnectionString, storageTableName, { allowInsecureConnection: true })
const EVENT_TYPE = 'payment-request-submission-batch'

const runStorageQuery = async () => {
  const events = []
  const eventResults = tableClient.listEntities({ queryOptions: { filter: odata`EventType eq ${EVENT_TYPE}` } })
  for await (const event of eventResults) {
    events.push(event)
  }

  const frns = []

  for (const event of events) {
    const payload = JSON.parse(event.Payload)
    frns.push(`(${payload.data.paymentRequest.frn}, '${payload.data.paymentRequest.invoiceNumber}')`)
  }

  const create = `CREATE TABLE "tempFrns"
    (
      "frn" BIGINT,
      "invoiceNumber" VARCHAR
    );`

  const insert = `INSERT INTO "tempFrns" (frn, "invoiceNumber") \n VALUES ${frns.join(',\n')};`

  const updatePaymentRequests = `UPDATE "paymentRequests" 
    SET frn = "tempFrns".frn 
    FROM "tempFrns" 
    WHERE "paymentRequests"."invoiceNumber" = "tempFrns"."invoiceNumber"
    AND "paymentRequests".frn IS NULL;`

  const updateSettlements = `UPDATE "settlements" 
    SET frn = "tempFrns".frn 
    FROM "tempFrns" 
    WHERE "settlements"."invoiceNumber" = "tempFrns"."invoiceNumber"
    AND "settlements".frn IS NULL;`

  const drop = 'DROP TABLE "tempFrns";'

  fs.writeFileSync('update-frns.sql', `${create}\n${insert}\n${updatePaymentRequests}\n${updateSettlements}\n${drop}`)
}

module.exports = runStorageQuery
