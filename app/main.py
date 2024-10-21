from fastapi import Depends, FastAPI
#from .internal import admin
from .routers import logins, driver, stops


app = FastAPI()


app.include_router(logins.router)
app.include_router(driver.router)
app.include_router(stops.router)

@app.get("/")
async def root():
    return {"message": "Hello Bigger Applications!"}
