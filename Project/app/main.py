from fastapi import FastAPI
from app.models.schema import create_tables
from app.api.routes import router

app = FastAPI()
create_tables()
app.include_router(router)
