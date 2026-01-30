"""
Utilitários de autenticação: JWT e hash de senha.
Usa bcrypt diretamente para evitar bug do passlib em detect_wrap_bug.
"""
import os
from datetime import datetime, timedelta
from typing import Optional

import bcrypt
from jose import JWTError, jwt
from dotenv import load_dotenv

load_dotenv()

SECRET_KEY = os.getenv("JWT_SECRET", "sua-chave-secreta-mude-em-producao")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))


def _password_72_bytes(password: str) -> bytes:
    """Bcrypt aceita no máximo 72 bytes; trunca se necessário."""
    pwd_bytes = password.encode("utf-8")
    return pwd_bytes[:72] if len(pwd_bytes) > 72 else pwd_bytes


def hash_password(password: str) -> str:
    pwd = _password_72_bytes(password)
    hashed = bcrypt.hashpw(pwd, bcrypt.gensalt())
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    pwd = _password_72_bytes(plain_password)
    try:
        return bcrypt.checkpw(pwd, hashed_password.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None
