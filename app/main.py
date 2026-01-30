from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from app.routes.pedidos import router as pedidos_router
from app.routes.analytics import router as analytics_router
from app.routes.dashboard import router as dashboard_router
from app.routes.upload import router as upload_router
from app.routes.auth import router as auth_router, get_current_user

app = FastAPI(title="Dashboard Abramides API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],  # ajuste conforme seu front
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas públicas (sem login)
app.include_router(auth_router)

# Rotas protegidas (exigem Bearer token)
app.include_router(pedidos_router, dependencies=[Depends(get_current_user)])
app.include_router(analytics_router, dependencies=[Depends(get_current_user)])
app.include_router(dashboard_router, dependencies=[Depends(get_current_user)])
app.include_router(upload_router, dependencies=[Depends(get_current_user)])