# Python file for RegionalSafetyGetAccountUsers
import json
from simple_salesforce import Salesforce, SalesforceLogin
import requests
import io
import time
import base64
from datetime import datetime, timedelta

# amountPaid
# balance
# billingAddress >
# contactName
# dueDate
# email
# inventoryStatus
# InvoiceDate
# isCancelled
# isCompleted
# isInvoiced
# isPrioritized
# lines
# orderDate
# orderFreight
# orderNumber
# orderRemarks
# paymentStatus
# phone
# requestedShipDate
# shippingAddress
# subTotal
# tax1
# tax1Name
# total

INFLOW_API_KEY = "F922915CB1ACB5D166B8DF914056E0AC4EB35B6155DE17232024CEE97BEE01F9"
sf = Salesforce(username="mensa-integration@mensatech.com.au.rss",password="gr@yMoney26", security_token="WlPZjZquTdRfFnCi8lAYvPP3",domain="test")

session_id, instance = SalesforceLogin(username="mensa-integration@mensatech.com.au.rss", password="gr@yMoney26", security_token="WlPZjZquTdRfFnCi8lAYvPP3",domain="test")

def lambda_handler(event, context):
    print("Event: ", event)
    global sf

    try:
        eventBody = json.loads(event["body"])
        print("EventBody: ", eventBody)
    except json.JSONDecodeError:
        print("Error: Invalid JSON Format")

    inflowSaleOrders = getInflowSaleOrders()
    salesforceProducts = getSFProducts()
    salesforceOrders = getSFSaleOrders()
    salesforcOrderItems = getSFSaleOrdersItems()
    checkForUpdates(inflowSaleOrders,salesforceProducts,salesforceOrders, salesforcOrderItems)

    response = {
        'statusCode': 200,
        'body': userList
    }
    
    return response




def checkForUpdates(inflowSaleOrders, salesforceProducts, salesforceOrders, salesforceOrderItems):
    # for inflowOrder in inflowSaleOrders:
    #     if inflowOrder['id'] not in salesforceOrders:
    #         processNewOrder(inflowOrder, salesforceProducts, salesforceOrderItems)
    #     else:
    #         processExistingOrder(inflowOrder, salesforceOrders, salesforceOrderItems)
    return

def processNewOrder(inflowOrder, salesforceProducts, salesforceOrderItems):
    # for lineItem in inflowOrder['lineItems']:
    #     if lineItem['product_id'] not in salesforceProducts:
    #         createProductInSalesforce(lineItem['product_id'])
    #     insertLineItemInSalesforce(lineItem)
    # insertOrderInSalesforce(inflowOrder)
    return

def processExistingOrder(inflowOrder, salesforceOrders, salesforceOrderItems):
    # if inflowOrder['timestamp'] != salesforceOrders[inflowOrder['id']]['timestamp']:
    #     updateOrderInSalesforce(inflowOrder)
    #     processLineItems(inflowOrder, salesforceOrderItems)
    return
def processLineItems(inflowOrder, salesforceOrderItems):
    # for lineItem in inflowOrder['lineItems']:
    #     sfOrderItem = salesforceOrderItems.get(lineItem['id'])
    #     if not sfOrderItem:
    #         insertLineItemInSalesforce(lineItem)
    #     elif lineItem['timestamp'] != sfOrderItem['timestamp']:
    #         updateLineItemInSalesforce(lineItem)
    return

def createProductInSalesforce(productId):
    # Logic to create product in Salesforce
    pass

def insertOrderInSalesforce(order):
    # Logic to insert order in Salesforce
    pass

def updateOrderInSalesforce(order):
    # Logic to update order in Salesforce
    pass

def insertLineItemInSalesforce(lineItem):
    # Logic to insert line item in Salesforce
    pass

def updateLineItemInSalesforce(lineItem):
    # Logic to update line item in Salesforce
    pass

# get all sales order
# https://cloudapi.inflowinventory.com/{companyId}/sales-orders
def getInflowSaleOrders():
    # Get sales order from inflow 
    return

# get all sf salesOrder
def getSFSaleOrders():
    # get id, rowVersion,
    return

# get all sf salesOrder
def getSFSaleOrdersItems(saleOrder):
    # get price, product, pricebook entry
    return

# get all sf products
def getSFProducts():

    return

#get all products
# https://cloudapi.inflowinventory.com/{companyId}/products
# def getInflowProducts():

#     return





body = {
    "body": "{\"OpportunityID\": \"0069t00000AJySZAA1\", \"OtherField\": \"OtherValue\"}"
}


lambda_handler(body, "context")
