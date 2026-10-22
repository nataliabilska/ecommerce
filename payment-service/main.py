from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
import aio_pika
import json
import asyncio

app = FastAPI(title="Payment Service API")

DATABASE_URL = "postgresql+asyncpg://payment_user:payment_pass@127.0.0.1:5434/payment_db"
RABBITMQ_URL = "amqp://guest:guest@127.0.0.1/"

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class PaymentDB(Base):
    __tablename__ = "payments"
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    order_id: Mapped[int]
    status: Mapped[str] = mapped_column(default="PROCESSED") 

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    asyncio.create_task(consume_messages())

async def publish_event(event_name: str, payload: dict):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue("order_events", durable=True)
        message_body = json.dumps({"event_name": event_name, **payload}).encode()
        await channel.default_exchange.publish(
            aio_pika.Message(body=message_body),
            routing_key="order_events"
        )

async def process_payment(order_data: dict):
    async with AsyncSessionLocal() as session:
        new_payment = PaymentDB(order_id=order_data["order_id"], status="PROCESSED")
        session.add(new_payment)
        await session.commit()
        print(f"Płatność za zamówienie {order_data['order_id']} zaksięgowana!")
        
        await publish_event("PaymentProcessed", {
            "order_id": order_data["order_id"],
            "product_id": order_data["product_id"],
            "quantity": order_data["quantity"]
        })

async def refund_payment(order_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(PaymentDB).where(PaymentDB.order_id == order_id))
        payment = result.scalar_one_or_none()
        if payment:
            payment.status = "REFUNDED"
            await session.commit()
            print(f"KOMPENSACJA: Zwrócono środki dla zamówienia {order_id}.")
            
            await publish_event("PaymentRefunded", {"order_id": order_id})

async def consume_messages():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue("order_events", durable=True)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body)
                event_name = data.get("event_name")
                
                if event_name == "OrderCreated":
                    await process_payment(data)
                elif event_name == "InventoryFailed":
                    print(f"Odmowa z magazynu dla zamówienia {data['order_id']}!")
                    await refund_payment(data["order_id"])