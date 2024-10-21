from datetime import datetime, timedelta, timezone
from pydantic import BaseModel
from jose import JWTError, jwt
from .driver import User
# to get a string like this run:
# openssl rand -hex 32
SECRET_KEY = "2d5e5297ab8872ed8f8ca64f4eadcb5f9ad3e4c269297c91b751012c8575e541"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 400000

users = [{'code':'V12446697','name':'RAMON ALEXIS SUAREZ RUSA', 'password':'12345','admin':False},
        {'code':'V12877074','name':'CARRERO PRATO ROBERTO','password':'12345','admin':False},
        {'code':'V9171390','name':'GONZALEZ RAMIREZ EMIGDIO JOSE','password':'12345','admin':False},
        {'code':'V28288988','name':'Oswaldo Yepez','password':'12345','admin':True},]

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: str | None = None
    password: str | None = None

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(days=1)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_user(code: str, password: str):
        for user in users:
            if user["code"] == code and user["password"]==password:
                 return User(username=user["name"], code=user["code"],password=user["password"], admin=user["admin"])
                