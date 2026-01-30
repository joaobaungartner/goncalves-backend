from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routes.pedidos import router as pedidos_router
from app.routes.analytics import router as analytics_router
from app.routes.dashboard import router as dashboard_router

app = FastAPI(title="Dashboard Abramides API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # ajuste conforme seu front
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(pedidos_router)
app.include_router(analytics_router)
app.include_router(dashboard_router)