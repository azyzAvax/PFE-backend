from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.endpoints import router
from dotenv import load_dotenv

 


def create_app() -> FastAPI:
    """Initialize the FastAPI application with middleware."""
    load_dotenv(override=True)

    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:3000", "http://192.168.1.16:3000","http://172.29.80.1:3000"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app

app = create_app()