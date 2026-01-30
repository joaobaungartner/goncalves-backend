import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "InsperJr")

client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]

fatos = db["fatos_pedidos"]
polpa = db["polpa_metricas"]
manteiga = db["manteiga_metricas"]
users = db["users"]
