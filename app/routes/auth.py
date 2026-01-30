"""
Rotas de autenticação: criar usuário (Postman), login e me.
"""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

from app.db import users
from app.auth import (
    hash_password,
    verify_password,
    create_access_token,
    decode_access_token,
)

router = APIRouter(prefix="/auth", tags=["auth"])
security = HTTPBearer(auto_error=False)


# ---------- Schemas ----------
class CriarUsuarioBody(BaseModel):
    username: str
    password: str


class LoginBody(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ---------- Dependency: usuário atual ----------
async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
):
    if not credentials or credentials.credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token não informado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = credentials.credentials
    payload = decode_access_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido ou expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    username = payload.get("sub")
    if not username:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = await users.find_one({"username": username}, {"_id": 0, "password_hash": 0})
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário não encontrado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


# ---------- Rotas ----------
@router.post("/criar-usuario", response_model=dict)
async def criar_usuario(body: CriarUsuarioBody):
    """
    Cria um usuário (para uso via Postman; não há página de cadastro).
    """
    if not body.username or not body.username.strip():
        raise HTTPException(400, "username é obrigatório.")
    if not body.password or len(body.password) < 4:
        raise HTTPException(400, "password deve ter pelo menos 4 caracteres.")

    existing = await users.find_one({"username": body.username.strip().lower()})
    if existing:
        raise HTTPException(400, "Usuário já existe.")

    doc = {
        "username": body.username.strip().lower(),
        "password_hash": hash_password(body.password),
    }
    await users.insert_one(doc)
    return {"message": "Usuário criado.", "username": doc["username"]}


@router.post("/login", response_model=TokenResponse)
async def login(body: LoginBody):
    """
    Autentica usuário e retorna JWT.
    """
    if not body.username or not body.password:
        raise HTTPException(400, "username e password são obrigatórios.")

    user = await users.find_one({"username": body.username.strip().lower()})
    if not user or not verify_password(body.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha inválidos",
        )

    access_token = create_access_token(data={"sub": user["username"]})
    return TokenResponse(access_token=access_token)


@router.get("/me", response_model=dict)
async def me(current_user: dict = Depends(get_current_user)):
    """
    Retorna o usuário atual (requer Bearer token).
    """
    return current_user
