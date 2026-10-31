from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
import aio_pika
import json
import asyncio

app = FastAPI(title="Inventory Service API")

DATABASE_URL = "postgresql+asyncpg://inventory_user:inventory_pass@127.0.0.1:5435/inventory_db"
RABBITMQ_URL = "amqp://guest:guest@127.0.0.1/"

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class ProductDB(Base):
    __tablename__ = "products"
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    name: Mapped[str]
    stock: Mapped[int]

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        
    async with AsyncSessionLocal() as session:
        result = await session.execute(select(ProductDB).where(ProductDB.id == 1))
        if not result.scalar_one_or_none():
            session.add(ProductDB(id=1, name="Klawiatura Mechaniczna", stock=5))
            await session.commit()
            
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

async def process_inventory(data: dict):
    order_id = data["order_id"]
    product_id = data["product_id"]
    quantity = data["quantity"]

    async with AsyncSessionLocal() as session:
        result = await session.execute(select(ProductDB).where(ProductDB.id == product_id))
        product = result.scalar_one_or_none()

        if product and product.stock >= quantity:
            product.stock -= quantity
            await session.commit()
            print(f"Zarezerwowano {quantity} szt. produktu {product_id} dla zamówienia {order_id}. Pozostało: {product.stock}")
            
            await publish_event("InventoryReserved", {"order_id": order_id})
        else:
            print(f"Brak wystarczającej ilości produktu {product_id} dla zamówienia {order_id}!")
            await publish_event("InventoryFailed", {"order_id": order_id})

async def consume_messages():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue("order_events", durable=True)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body)
                if data.get("event_name") == "PaymentProcessed":
                    await process_inventory(data)