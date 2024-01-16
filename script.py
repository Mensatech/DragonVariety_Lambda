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
    "shippingAddress", "subTotal", "tax1", "tax1Name", "total","timestamp","salesOrderId","customer"
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
    # print("Infow Sale Orders ID: ", inflowSaleOrders)

    salesforceProducts = getSFProducts()

    if salesforceProducts is None:
        salesforceProducts = []
    print("SalesForceProducts: ", salesforceProducts)

    for order in inflowSaleOrders:
        print("______________________________________________________________")
        print("Processing order: ", order["orderNumber"])
        orderId = order["salesOrderId"]
        # search sf for this id
        sfOrder = getSFOrder(orderId)
        if(sfOrder):
            print("______________________________________________________________")
            print("Updating sales order")
            # id exists... update record
            processExistingOrder(order,sfOrder,salesforceProducts)
            pass
        else:
            print("______________________________________________________________")
            print("Creating sales order")

            # id doesnt exist... create record
            processNewOrder(order,salesforceProducts)            
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
    return getOrdersResult[0]

def getSFAccount(id):
    getAccountQuery = f"select Id,name from Account where Customer_Reference_Id__c = '{id}'"
    print("\nGet Account query: ", getAccountQuery)
    getAccountResult = sf.query_all(getAccountQuery)["records"]
    print("\nGet Account Results: ", getAccountResult[0])
    if len(getAccountResult) < 1:
        print("\nGet Account returnted nothing")
        return None
    
    return getAccountResult[0]

def createSFAccount(order):
    print("Creating Account")
    print(order["salesOrderId"])

    billingAddress = order["billingAddress"]
    shippingAddress = order["shippingAddress"]

    account = {
    "Name": order["customer"].get("name", ""),
    "BillingCity": billingAddress.get("city", ""),
    "BillingCountry": billingAddress.get("country", ""),
    "BillingPostalCode": billingAddress.get("postalCode", ""),
    "BillingState": billingAddress.get("state", ""),
    "BillingStreet": billingAddress.get("address", ""),
    "ShippingCity": shippingAddress.get("city", ""),
    "ShippingCountry": shippingAddress.get("country", ""),
    "ShippingPostalCode": shippingAddress.get("postalCode", ""),
    "ShippingState": shippingAddress.get("state", ""),
    "ShippingStreet": shippingAddress.get("address", ""),
    "Phone": order["customer"].get("phone", ""),
    # "Email": order.get("email", ""),
    "Website": order["customer"].get("website", ""),
    "Customer_Reference_Id__c": order.get("customer", {}).get("customerId", "") if order.get("customer") else "",
    "Timestamp__c":order["customer"].get("timestap","")
    }
    
    print("Account details: ",account)

    response = sf.Account.create(account)
    print("New Account created: ", response.get("id"))
    return response[0].get("Id")


        # Handle other errors if necessary
        # print("Error creating account: ", errors)
        # return None
    
    # print("New Account created: ", newAccount.get("id"))
    # return newAccount

def createProduct(productInfo,sfProducts):
    timestamp = ""
    if "timestamp" in productInfo:
        timestamp = productInfo["timestamp"]

    productId = ""
    if "productId" in productInfo:
        productId = productInfo["productId"]

    isActive = ""
    if "isActive" in productInfo:
        isActive = productInfo["isActive"]

    name = ""
    if "name" in productInfo:
        name = productInfo["name"]

    description = ""
    if "description" in productInfo:
        description = productInfo["description"]

    print("Timestamp:", timestamp)
    print("ProductId:", productId)
    print("isActive:", isActive)
    print("Name:", name)
    print("Description: ", description)

    productToCreate = {
        "timestamp__c": timestamp,
        "Product_Id__c":productId,
        "isActive":isActive,
        "Name":name,
        "Description":description,
        "isActive":True,
    }
    newProduct = sf.Product2.create(productToCreate)
    newProductId = newProduct.get("id")
    productToCreate["Id"] = newProductId

    print("Product created")
    print(newProduct)
    sfProducts.append(productToCreate)

    priceBookValues = {
        "Product2Id" :newProduct["id"], 
        "UnitPrice":productInfo["defaultPrice"]["unitPrice"],
        "Pricebook2Id":"01s5j00000Q9jTAAAZ",
        "IsActive": True
    }
    print("PriceBook Entry")
    print(priceBookValues)
    priceBook = sf.PricebookEntry.create(priceBookValues)
    return priceBook

def createOrder(order):
    print("Creating an Order")
    print(order)
    # Extracting order details with checks
    accountId = getSFAccount(order["customer"]["customerId"]).get("Id")
    if(accountId == None):
        accountId = createSFAccount(order)
    orderNumber = ""
    if "orderNumber" in order:
        orderNumber = order["orderNumber"]

    orderDate = ""
    if "orderDate" in order:
        orderDate = order["orderDate"]

    dueDate = ""
    if "dueDate" in order:
        dueDate = order["dueDate"]

    salesOrderId = ""
    if "salesOrderId" in order:
        salesOrderId = order["salesOrderId"]

    total = 0
    if "total" in order:
        total = order["total"]

    amountPaid = 0
    if "amountPaid" in order:
        amountPaid = order["amountPaid"]

    timestamp = ""
    if "timestamp" in order:
        timestamp = order["timestamp"]

    poNumber = ""
    if "poNumber" in order:
        poNumber = order["poNumber"]

    paidDate = ""
    if "paidDate" in order:
        paidDate = order["paidDate"]

    # Printing order details
    print("Order Reference Number:", orderNumber)
    print("Effective Date:", orderDate)
    print("End Date:", dueDate)
    print("Sales Order Id:", salesOrderId)
    print("Total Amount:", total)
    print("Amount Paid:", amountPaid)
    print("Timestamp:", timestamp)
    print("PO Number:", poNumber)
    print("Activated Date:", paidDate)
    print("Account Id:", accountId)

    # Creating the order
    newOrder = sf.Order.create({
        "OrderReferenceNumber": orderNumber,
        "EffectiveDate": orderDate,
        "EndDate": dueDate,
        "Pricebook2Id": "01s5j00000Q9jTAAAZ",
        "Sales_Order_Id__c": salesOrderId,
        # "TotalAmount": total,
        "Paid__c": amountPaid,
        "Timestamp__c": timestamp,
        "PO__c": poNumber,
        # "ActivatedDate": paidDate,
        "AccountId": accountId,
        "Status":"Draft"
    })

    newOrderId = newOrder["id"]
    return newOrderId

def updateOrder(order):
    print("Updating an Order")

    # Extracting order details
    salesOrderId = order.get("salesOrderId", "")
    orderNumber = order.get("orderNumber", "")
    orderDate = order.get("orderDate", "")
    dueDate = order.get("dueDate", "")
    total = order.get("total", 0)
    amountPaid = order.get("amountPaid", 0)
    timestamp = order.get("timestamp", "")
    poNumber = order.get("poNumber", "")
    paidDate = order.get("paidDate", "")
    accountId = getSFAccount(order.get("customerId", ""))

    # Printing extracted order details
    print("Sales Order Id:", salesOrderId)
    print("Order Reference Number:", orderNumber)
    print("Effective Date:", orderDate)
    print("End Date:", dueDate)
    print("Total Amount:", total)
    print("Amount Paid:", amountPaid)
    print("Timestamp:", timestamp)
    print("PO Number:", poNumber)
    print("Activated Date:", paidDate)
    print("Account Id:", accountId)

    # Update the existing order
    updatedOrder = sf.Order.update({
        "Sales_Order_Id__c": salesOrderId,
        "OrderReferenceNumber": orderNumber,
        "EffectiveDate": orderDate,
        "EndDate": dueDate,
        "Pricebook2Id": "01s5j00000Q9jTAAAZ",  # Example Pricebook2Id, replace with actual ID if necessary
        "TotalAmount": total,
        "Paid__c": amountPaid,
        "Timestamp__c": timestamp,
        "PO__c": poNumber,
        "ActivatedDate": paidDate,
        "AccountId": accountId
    })

    print("Updated Order:", updatedOrder)
     
def createLineItems(lineItem, newOrderId,sfProducts):
    print("Creating line item")
    print(lineItem)
    print("Current Sf products")
    print(sfProducts)
    for item in lineItem:
        productId = ""
        for product in sfProducts:
            if product["Product_Id__c"] == item["productId"]:
                productId = product["Id"]
                break

        description = ""
        if "description" in item:
            description = item["description"]

        quantity = 0
        if "quantity" in item:
            quantity = item["quantity"]["standardQuantity"]

        orderValue = 0
        if "unitPrice" in item:
            orderValue = item["unitPrice"]

        lineItemID = ""
        if "salesOrderLineId" in item:
            lineItemID = item["salesOrderLineId"]

        timestamp = ""
        if "timestamp" in item:
            timestamp = item["timestamp"]

        getPriceBookEntryQuery = f"select Id,name from PricebookEntry where Product2Id = '{productId}'"
        print("\nGet PriceBook Entry query: ", getPriceBookEntryQuery)
        getPriceBookEntryResult = sf.query_all(getPriceBookEntryQuery)["records"]
        print("\nGet PriceBook Entry Results: ", getPriceBookEntryResult[0])
        pricebookEntryId = getPriceBookEntryResult[0].get("Id")
        
        print("Product:", productId)
        print("Description:", description)
        print("Quantity:", quantity)
        print("Order Value:", orderValue)
        print("LineItemID: ", lineItemID)
        print("Timestamp: ", timestamp)
        print("PriceBook Entry id", pricebookEntryId)

        if quantity == 0:
            print('Product {productId} quantity was 0. It was not added to the order')
            pass
        else:
            createOrderItem = sf.OrderItem.create({
                "OrderId": newOrderId,
                "Product2Id": productId,
                "PricebookEntryId": pricebookEntryId,
                "Description": description,
                "Quantity": quantity,
                "UnitPrice": orderValue,
                "Inflow_External_line_Id__c": lineItemID,
                "Timestamp__c": timestamp,
            })
            print("Created Order Item: ", createOrderItem)
            print("Line Item: ", product, description, quantity, orderValue)

def updateLineItems(item, sfProducts):
    print("Updating line item")

    # Extracting product ID from sfProducts
    productId = ""
    for product in sfProducts:
        if product["Product_Id__c"] == item["productId"]:
            productId = product["Id"]
            break

    # Extracting other line item details
    description = item.get("description", "")
    quantity = item.get("quantity", {}).get("standardQuantity", 0)
    orderValue = item.get("unitPrice", 0)
    lineItemID = item.get("salesOrderLineId", "")
    timestamp = item.get("timestamp", "")

    # Printing line item details
    print("Product:", productId)
    print("Description:", description)
    print("Quantity:", quantity)
    print("Order Value:", orderValue)
    print("LineItemID: ", lineItemID)
    print("Timestamp: ", timestamp)

    # Check for non-zero quantity before updating
    if quantity > 0:
        updatedOrderItem = sf.OrderItem.update({
            "Id": lineItemID,  # Assuming this is the identifier for the line item in Salesforce
            "Product2Id": productId,
            "Description": description,
            "Quantity": quantity,
            "UnitPrice": orderValue,
            "Timestamp": timestamp,
            # Include any other fields that need to be updated
        })
        print("Updated Order Item: ", updatedOrderItem)
    else:
        print("Skipping update for zero quantity line item")

def productExists(productId, sfProducts):
    print("Product we are looking for: ", productId)
    if sfProducts == None or len(sfProducts) < 1:
        return False
    for product in sfProducts:
        print("Current Product: ", product["Product_Id__c"])
        if product["Product_Id__c"] == productId:
            print("Found Product")
            return True
        else: 
            continue
    return False

def processNewOrder(inflowOrder, salesforceProducts):
    # print(inflowOrder)
    for lineItem in inflowOrder['lines']:
        # if salesforceProducts == None or lineItem['productId'] not in salesforceProducts:
        if productExists(lineItem["productId"], salesforceProducts) == False:
            # INSERT product2 with lineItem
            print(f'Product {lineItem["productId"]} was not found on SF')
            print(f'Creating {lineItem["productId"]} in SF')

            productInfo = getInflowProduct(lineItem["productId"])
            
            createdProduct = createProduct(productInfo,salesforceProducts)
            print(f'Created product: {createdProduct}')
        pass
    # INSERT into saleOrder with inflowOrder
    orderId = createOrder(inflowOrder)
    # Attach line items to saleOrder
    createLineItems(inflowOrder['lines'],orderId,salesforceProducts)
    return

def processExistingOrder(inflowOrder, salesforceOrder,sfProducts):
    print(inflowOrder)
    if inflowOrder['timestamp'] != salesforceOrder['Timestamp__c']:
        #UPDATE salesOrder where external id = infowOrder["orderId"]
        orderId = updateOrder(inflowOrder)
        for lineItem in inflowOrder['lines']:
            sfOrderItem = getSFOrder(id)#Get  salesforceOrderItems.get(lineItem['id'])

            if not sfOrderItem:
                print(f'Product {lineItem["productId"]} was not found on SF')
                print(f'Creating {lineItem["productId"]} in SF')
                productInfo = getInflowProduct(lineItem["productId"])
                # INSERT into products with productInfo
                createdProduct = createProduct(productInfo)
                print(f'Created product: {createdProduct}')
                pass
            if lineItem['timestamp'] != sfOrderItem['timestamp']:
                # UPDATE saleOrderItem 
                updateLineItems(lineItem,sfProducts)
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
    print("Getting inflow saleOrders")
    all_orders = []
    last_entity_id = None
    count = 1  # Maximum number of entities per request
    start_time = datetime.now()
    
    total = 0
    while True:
        print(f'Requesting inflow orders')
        params = {"count": count, "after": last_entity_id, "sort": "salesOrderId", "include":"lines,customer", "includeCount":"true"}
        response = requests.get(requestURL, headers=headers, params=params)

        rate_limit_info = response.headers.get('X-inflow-api-rate-limit', '0/0')
        requests_left, max_requests = map(int, rate_limit_info.split('/'))

        if requests_left < 1:  # Threshold for remaining requests
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
        total += len(orders)
        listCount = response.headers.get('X-listCount')
        # print(orders)
        print(f'Received {total} out of {listCount} inflow orders')

        if len(orders) < count:
            break  # Break the loop if we've got less than count orders
        break
        last_entity_id = orders[-1]['salesOrderId']
        print(last_entity_id)

    print(f"Total orders fetched: {len(all_orders)}")
    extracted_data = [{key: obj.get(key, None) for key in keys_to_extract} for obj in all_orders]
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
