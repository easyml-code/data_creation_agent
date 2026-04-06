"""
Entry point — create FastAPI app, include router, run with uvicorn.
"""

import uvicorn
from fastapi import FastAPI
import config
from routes import router


def create_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    return app


app = create_app()

if __name__ == "__main__":
    print(f"Starting Invoice → GRN agent on port {config.PORT}")
    uvicorn.run("main:app", host="0.0.0.0", port=config.PORT, reload=True)
