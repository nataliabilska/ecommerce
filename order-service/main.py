from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
import aio_pika
import json

app = FastAPI(title="Order Service API")

DATABASE_URL = "postgresql+asyncpg://order_user:order_pass@127.0.0.1:5433/order_db"
RABBITMQ_URL = "amqp://guest:guest@127.0.0.1:5672/"

engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase):
    pass

class OrderDB(Base):
    __tablename__ = "orders"
    id: Mapped[int] = mapped_column(primary_key=True, index=True)
    product_id: Mapped[int]
    quantity: Mapped[int]
    user_id: Mapped[int]
    status: Mapped[str] = mapped_column(default="PENDING")

class OrderRequest(BaseModel):
    product_id: int
    quantity: int
    user_id: int

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@app.post("/orders")
async def create_order(order: OrderRequest):
    new_order = OrderDB(
        product_id=order.product_id,
        quantity=order.quantity,
        user_id=order.user_id,
        status="PENDING"
    )
    
    async with AsyncSessionLocal() as session:
        session.add(new_order)
        await session.commit()
        await session.refresh(new_order)

    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue("order_events", durable=True)
        
        message_body = json.dumps({
            "event_name": "OrderCreated",
            "order_id": new_order.id, 
            "status": new_order.status,
            "product_id": new_order.product_id,
            "quantity": new_order.quantity,
            "user_id": new_order.user_id
        }).encode()
        
        await channel.default_exchange.publish(
            aio_pika.Message(body=message_body),
            routing_key="order_events"
        )
        
    return {"message": "Order created", "order_id": new_order.id, "status": new_order.status}