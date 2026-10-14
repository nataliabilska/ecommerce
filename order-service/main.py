from fastapi import FastAPI
from pydantic import BaseModel
import aio_pika
import json

app = FastAPI(title="Order Service API")

RABBITMQ_URL = "amqp://guest:guest@localhost/"

class OrderRequest(BaseModel):
    product_id: int
    quantity: int
    user_id: int

@app.post("/orders")
async def create_order(order: OrderRequest):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue("order_events", durable=True)
        
        message_body = json.dumps({
            "event_name": "OrderCreated",
            "status": "PENDING",
            "order_details": order.model_dump()
        }).encode()
        
        await channel.default_exchange.publish(
            aio_pika.Message(body=message_body),
            routing_key="order_events"
        )
        
    return {"message": "Order accepted", "status": "PENDING"}