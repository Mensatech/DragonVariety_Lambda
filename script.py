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
    "shippingAddress", "subTotal", "tax1", "tax1Name", "total","timestamp","salesOrderId"
]

INFLOW_API_KEY = "F922915CB1ACB5D166B8DF914056E0AC4EB35B6155DE17232024CEE97BEE01F9"
sf = Salesforce(username="velocity-integration@mensatech.com.au",password="&[8Hn4baZ8/u]_%007", security_token="SUQ9nMAwQmmHCGAWpganRXg35")

session_id, instance = SalesforceLogin(username="velocity-integration@mensatech.com.au", password="&[8Hn4baZ8/u]_%007", security_token="SUQ9nMAwQmmHCGAWpganRXg35")

def lambda_handler(event, context):
    print("Event:")
    print(event)
    global sf

    try:
        eventBody = json.loads(event["body"])
        print("EventBody: ", eventBody)
    except json.JSONDecodeError:
        print("Error: Invalid JSON Format")

    inflowSaleOrders = getInflowSaleOrders()
    salesforceProducts = getSFProducts()
    print("Infow Sale Orders ID: ", inflowSaleOrders)
    print("SalesForceProducts: ", salesforceProducts)

    for order in inflowSaleOrders:
        print(order)
        orderId = order["salesOrderId"]
        # search sf for this id
        sfOrder = getSFOrder(orderId)
        if(sfOrder):
            print("Updating record")
            # id exists... update record
            # processExistingOrder()
            pass
        else:
            print("Creating record")

            # id doesnt exist... create record
            # processNewOrder(order,salesforceProducts)            
            pass
        break

    response = {
        'statusCode': 200,
        'body': "hello"
    }
    
    return response

def getSFOrder(id):
    getOrdersQuery = f"select Id, Sales_Order_Id__c,Timestamp__c  from Order  where Sales_Order_Id__c = '{id}'"
    print("\nGet Orders query: ", getOrdersQuery)
    getOrdersResult = sf.query_all(getOrdersQuery)["records"]
    if len(getOrdersResult) < 1:
        print("\nGet Order returnted nothing")
        return None
    print("\nGet Orders Results: ", getOrdersResult[0])
    return getOrdersResult

def processNewOrder(inflowOrder, salesforceProducts):
    # print(inflowOrder)
    for lineItem in inflowOrder['lines']:
        if salesforceProducts == None or lineItem['productId'] not in salesforceProducts:
            # INSERT product2 with lineItem
            print(f'Product {lineItem["productId"]} was not found on SF')
            productInfo = getInflowProduct(lineItem["productId"])

            productToCreate = {
                "timestamp__c": productInfo["timestamp"],
                "Product_Id__c":productInfo["productId"],
                "isActive":productInfo["isActive"],
                "Name":productInfo["name"],
                "Description":productInfo["description"]
            }
            # createdProduct = sf.Product2.create(productToCreate)
            # print(f'Created product: {createdProduct}')
            print(salesforceProducts)

            continue
        print(salesforceProducts)

        # UPDATE product2 with lineItem
        if lineItem["timestamp"] != salesforceProducts["timestamp__c"]:
            pass
        break
    # INSERT into saleOrder with inflowOrder
    # Attach line items to saleOrder
    return

def processExistingOrder(inflowOrder, salesforceOrders,sfOrder):
    if inflowOrder['timestamp'] != salesforceOrders[inflowOrder['id']]['timestamp']:
        #UPDATE salesOrder where external id = infowOrder["orderId"]
        for lineItem in inflowOrder['lines']:
            # sfOrderItem = getSFOrder(id)#Get  salesforceOrderItems.get(lineItem['id'])

            if not sfOrderItem:
                productInfo = getInflowProduct(lineItem["productId"])
                # INSERT into products with productInfo
                pass
            elif lineItem['timestamp'] != sfOrderItem['timestamp']:
                # UPDATE saleOrderItem 
                pass
    return

# get all sales order
# https://cloudapi.inflowinventory.com/{companyId}/sales-orders
def getInflowSaleOrders():
    requestURL = 'https://cloudapi.inflowinventory.com/a0584c15-81c0-431f-9989-3b51cdc42b0b/sales-orders'
    headers = {
        "Accept": "application/json;version=2021-04-26",
        "Authorization": f"Bearer {INFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    all_orders = []
    last_entity_id = None
    count = 100  # Maximum number of entities per request
    start_time = datetime.now()
    params = {"count": count, "after": last_entity_id, "sort": "salesOrderId", "include":"lines"}

    while True:
        response = requests.get(requestURL, headers=headers, params=params)

        rate_limit_info = response.headers.get('X-inflow-api-rate-limit', '0/0')
        requests_left, max_requests = map(int, rate_limit_info.split('/'))

        if requests_left < 5:  # Threshold for remaining requests
            wait_time = 60 - (datetime.now() - start_time).seconds
            print(f"Approaching rate limit. Waiting for {wait_time} seconds.")
            time.sleep(wait_time)
            start_time = datetime.now()
            continue

        if response.status_code == 429:
            print("Rate limit exceeded. Adjusting wait time...")
            wait_time = 60 - (datetime.now() - start_time).seconds
            time.sleep(wait_time)
            start_time = datetime.now()
            continue
        elif response.status_code != 200:
            print("Error fetching data:", response.status_code)
            break

        orders = response.json()
        all_orders.extend(orders)

        if len(orders) < count:
            break  # Break the loop if we've got less than count orders
        # break
        last_entity_id = orders[-1]['salesOrderId']

    print(f"Total orders fetched: {len(all_orders)}")
    print(all_orders[0])
    extracted_data = [{key: obj.get(key, None) for key in keys_to_extract} for obj in all_orders]
    print(extracted_data[0])
    return extracted_data

def getInflowProduct(productId, max_retries=10):
    requestURL = f'https://cloudapi.inflowinventory.com/a0584c15-81c0-431f-9989-3b51cdc42b0b/products/{productId}'
    headers = {
        "Accept": "application/json;version=2021-04-26",
        "Authorization": f"Bearer {INFLOW_API_KEY}",
        "Content-Type": "application/json"
    }

    parms = {"include":"defaultPrice"}

    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(requestURL, headers=headers, params=parms)

        if response.status_code == 429:  # Rate limit exceeded
            print("Rate limit exceeded, waiting to retry...")
            time.sleep(1)  # Wait for 1 second before retrying
            retry_count += 1
            continue
        elif response.status_code != 200:
            print("Error fetching data:", response.status_code)
            return None

        product_data = response.json()
        print(f"Fetched product {productId}, data after {max_retries} retries.")

        return product_data

    print(f"Failed to fetch product data after {max_retries} retries.")
    return None

# get all sf products
def getSFProducts():
    getProductsQuery = f"select Id,Product_Id__c, Name, timestamp__c, IsActive, Description from Product2"
    print("\nGet Product query: ", getProductsQuery)
    getProductResult = sf.query_all(getProductsQuery)["records"]
    print("Number of products returned: ", len(getProductResult))
    if len(getProductResult) < 1:
        return None
    print("\nGet Product Results: ", getProductResult[0])
    return getProductResult

#get all products
# https://cloudapi.inflowinventory.com/{companyId}/products
# def getInflowProducts():

#     return


body = {
    "body": "{\"OpportunityID\": \"0069t00000AJySZAA1\", \"OtherField\": \"OtherValue\"}"
}

lambda_handler(body, "context")
