from dataclasses import dataclass
from typing import List
import time 
from datetime import datetime
# Simulator data
import uuid
import faker
import random 

# Second level imports :
from KafkaProducer.schema.example_customer import list_orders, list_customer, payment_methods, payment_status, order_status

# Third parties import : 
from KafkaProducer.producer import KafkaProducer


@dataclass
class OrderItem:
    productId: str
    quantity: int
    
@dataclass
class Order:
    orderId: str
    orderDate: str
    customerId: str
    shippingAddress: str
    totalAmount: float
    items: List[OrderItem]
    paymentMethod: str
    paymentStatus: str
    orderStatus: str
        
        
def generate_order() : 
    # Use Faker to generate random data : 
    fake = faker.Faker()
    
    # Order Data :
    orderId = str(uuid.uuid4())
    orderDate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    customerId = str(random.randint(1, 100))
    
    
    # In 15% of the case get a random number address, for fraud cases :
    if random.random() < 0.15:
        customer_details = list_customer[str(random.randint(1, 100))]
    else:
        customer_details = list_customer[customerId]
        
    # Get rest details
    shippingAddress = customer_details['address']
    
    # Items generation : 
    num_items = random.randint(1, 5)
    items = []
    totalAmount = 0
    for i in range(num_items):
        productId = str(random.randint(1, 100))
        quantity = random.randint(1, 10)
        # Get unity price from dict : 
        unit_price = list_orders[productId]
        totalAmount += quantity * unit_price['unit_price']
        items.append(OrderItem(productId= "P" + str(productId).zfill(3), quantity=quantity))
    
    # Payment & Order details
    paymentMethod = random.choice(payment_methods)
    paymentStatus = random.choice(payment_status)
    orderStatus = random.choice(order_status)

    # Create order obj : 
    return Order(
        orderId=orderId,
        orderDate=orderDate,
        customerId=customerId,
        shippingAddress=shippingAddress,
        totalAmount=totalAmount,
        items=items,
        paymentMethod=paymentMethod,
        paymentStatus=paymentStatus,
        orderStatus=orderStatus
    )

def to_dict(order: Order, ctx: any = None) -> dict:
    return dict(
        orderId=order.orderId,
        orderDate=order.orderDate,
        customerId=order.customerId,
        shippingAddress=order.shippingAddress,
        totalAmount=order.totalAmount,
        items=[dict(productId=item.productId, quantity=item.quantity) for item in order.items],
        paymentMethod=order.paymentMethod,
        paymentStatus=order.paymentStatus,
        orderStatus=order.orderStatus
    )
    
    
def main():
    my_producer = KafkaProducer(topic="my-orders", schema="src/KafkaProducer/schema/order.avsc", serializer_function=to_dict)
    
    num_of_orders = input("Enter the number of orders to generate: ")
    list_orders = []
    for i in range(int(num_of_orders)):
        if i % 100 == 0:
            print(f"Generated {i} orders")
        # Append to my list:
        list_orders.append(generate_order())
        
    # Produce to my kafka topic : 
    my_producer.produce(list_orders)
   
if __name__ == "__main__":
    main()