from fastapi import FastAPI
from src.db import Base, engine
from src.task_routes import router
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from src.db import engine

with engine.connect() as conn:
    try:
        conn.execute(text("SELECT 1"))
        print("✅ Database connected successfully!")
    except Exception as e:
        print("❌ Database connection failed:", e)
Base.metadata.create_all(bind=engine)

app = FastAPI(title="FastAPI To-Do List with SQL Server")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
