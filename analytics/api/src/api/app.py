import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from starlette.types import Lifespan

from api.metrics import metrics_router
from api.repository import MetricsRepository, RepositoryUnavailableError
from api.status import status_router

logger = logging.getLogger(__name__)


async def unavailable(request: Request, error: Exception) -> JSONResponse:
    logger.warning("%s %s: %s", request.method, request.url.path, error.__cause__ or error)
    return JSONResponse({"detail": str(error)}, status_code=status.HTTP_503_SERVICE_UNAVAILABLE)


async def internal_error(request: Request, error: Exception) -> JSONResponse:
    return JSONResponse(
        {"detail": "Internal server error"}, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


def create_app(repository: MetricsRepository, lifespan: Lifespan[FastAPI] | None = None) -> FastAPI:
    app = FastAPI(
        title="Gold metrics API",
        summary="The four business metrics and the freshness of the gold layer.",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(metrics_router(repository))
    app.include_router(status_router(repository))
    app.add_exception_handler(RepositoryUnavailableError, unavailable)
    # Starlette still re-raises the error after this handler, so the server logs its traceback.
    app.add_exception_handler(Exception, internal_error)
    return app
