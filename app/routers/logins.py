from datetime import timedelta
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from services.adempiere import AdempiereConnect
from models.tokenModels import Token, TokenData, User, create_access_token, get_user
router = APIRouter()

SECRET_KEY = "2d5e5297ab8872ed8f8ca64f4eadcb5f9ad3e4c269297c91b751012c8575e541"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 400000


pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        code: str = payload.get("sub")
        password: str = payload.get("password")
        if code is None:
            raise credentials_exception
        token_data = TokenData(username=code, password=password)
    except JWTError:
        raise credentials_exception
    user = get_user(code=token_data.username, password=password)
    if user is None:
        raise credentials_exception
    return user

@router.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()]
) -> Token:
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        user = conn_adempiere.login(form_data.username)
        if user is None:
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Credenciales Invalidas") 
        if user:
          
            access_token = create_access_token(
            data={"sub": form_data.username, "password": form_data.password},
                )
            return Token(access_token=access_token, token_type="bearer")
        else:
             return HTTPException(status_code=404, detail="No autorizado") 
    finally:
        conn_adempiere.closeConnection()

#login
@router.get("/login",tags=["logins"], response_model=User)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_user)]
):
    return current_user



#Vefica que exista la placa en Adempiere
@router.get("/platelog/{plate}",tags=["logins"], response_model=User)
async def read_users_me(
    current_user: Annotated[User, Depends(get_current_user)], plate:str
):
    credentials_exception = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No se encontro la placa",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        verify_plate = conn_adempiere.queryVehicle(plate)
        if verify_plate:
            current_user.plate_vehicle = verify_plate.plate
        if not verify_plate:
            raise credentials_exception
    finally:
        conn_adempiere.closeConnection()
    return current_user

#Vefica que exista la placa en Adempiere
@router.get("/logindriver/{value}",tags=["logins"], response_model=User)
def read_users_me(value:str):
    credentials_exception = HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="No se encontro el usuario",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        conn_adempiere = AdempiereConnect()
        conn_adempiere.connect()
        driver = conn_adempiere.loginDriver(value)
        if driver:
            return driver
        if not driver:
            raise credentials_exception
    finally:
        conn_adempiere.closeConnection()
    return driver

