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
keys_to_extract = [
    "amountPaid", "balance", "billingAddress", "contactName",
    "dueDate", "email", "inventoryStatus", "InvoiceDate",
    "isCancelled", "isCompleted", "isInvoiced", "isPrioritized",
    "lines", "orderDate", "orderFreight", "orderNumber",
    "orderRemarks", "paymentStatus", "phone", "requestedShipDate",
    "shippingAddress", "subTotal", "tax1", "tax1Name", "total","timestamp"
]

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

    for order in inflowSaleOrders:
        orderId = order["Id"]
        # search sf for this id
        # GET salesOrder where orderId = orderId
        if(idEXISTS):
            # id exists... update record
            processNewOrder
        else:
            # id doesnt exist... create record
            processExistingOrder

    response = {
        'statusCode': 200,
        'body': userList
    }
    
    return response

def processNewOrder(inflowOrder, salesforceProducts, salesforceOrderItems):
    for lineItem in inflowOrder['lineItems']:
        if lineItem['product_id'] not in salesforceProducts:
            # INSERT into products with lineItem
            pass
        # INSERT into salesOrderItem with lineItem
    #INSERT into saleOrder with inflowOrder
    return

def processExistingOrder(inflowOrder, salesforceOrders, salesforceOrderItems):
    if inflowOrder['timestamp'] != salesforceOrders[inflowOrder['id']]['timestamp']:
        #UPDATE salesOrder where external id = infowOrder["orderId"]
        for lineItem in inflowOrder['lineItems']:
            sfOrderItem = salesforceOrderItems.get(lineItem['id'])
            if not sfOrderItem:
                # INSERT into products with lineItem
                pass
            elif lineItem['timestamp'] != sfOrderItem['timestamp']:
                # UPDATE saleOrderItem 
                pass
    return

# get all sales order
# https://cloudapi.inflowinventory.com/{companyId}/sales-orders
def getInflowSaleOrders():
    
    requestURL = f'https://cloudapi.inflowinventory.com/a0584c15-81c0-431f-9989-3b51cdc42b0b/sales-orders'
    headers = {
        "Accept": "application/json;version=2021-04-26",
        "Authorization": f"Bearer {INFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    all_orders = []
    last_entity_id = None
    count = 100  # Maximum number of entities per request
    request_count = 0
    start_time = datetime.now()

    while True:
        current_time = datetime.now()
        if request_count >= 60:
            if (current_time - start_time).seconds < 60:
                wait_time = 60 - (current_time - start_time).seconds
                print(f"Rate limit close to being exceeded. Waiting for {wait_time} seconds.")
                time.sleep(wait_time)
                request_count = 0
                start_time = datetime.now()

        params = {
            "count": count,
            "after": last_entity_id,  # For pagination
            "sort": "salesOrderId"
        }
        response = requests.get(requestURL, headers=headers, params=params)
        request_count += 1

        if response.status_code == 429:
            print("Rate limit exceeded. Adjusting wait time...")
            wait_time = 60 - (datetime.now() - start_time).seconds
            time.sleep(wait_time)
            request_count = 0
            start_time = datetime.now()
            continue
        elif response.status_code != 200:
            print("Error fetching data:", response.status_code)
            break
        orders = response.json()

        # if orders:  # Check if orders is not empty
        #     print("Sample order:", orders[0])

        all_orders.extend(orders)

        if len(orders) < count:
            break  # Break the loop if we've got less than count orders

        last_entity_id = orders[-1]['salesOrderId']

    print(f"Total orders fetched: {len(all_orders)}")
    extracted_data = [{key: obj.get(key, None) for key in keys_to_extract} for obj in all_orders]
    print(extracted_data[0])
    return extracted_data


# get all sf products
def getSFProducts():
    # 
    return

#get all products
# https://cloudapi.inflowinventory.com/{companyId}/products
# def getInflowProducts():

#     return


yeet = getInflowSaleOrders()
print(len(yeet))

body = {
    "body": "{\"OpportunityID\": \"0069t00000AJySZAA1\", \"OtherField\": \"OtherValue\"}"
}


# lambda_handler(body, "context")
