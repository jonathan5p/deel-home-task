from fastapi import APIRouter, Response, status

from api.metrics import UNAVAILABLE, Responses
from api.models import Freshness, Health
from api.repository import MetricsRepository, RepositoryUnavailableError

UNREACHABLE: Responses = {
    503: {"model": Health, "description": "The analytics database is unreachable"}
}


def status_router(repository: MetricsRepository) -> APIRouter:
    router = APIRouter(tags=["status"])

    @router.get(
        "/health",
        summary="Whether the analytics database is reachable",
        responses=UNREACHABLE,
    )
    async def health(response: Response) -> Health:
        try:
            await repository.ping()
        except RepositoryUnavailableError:
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return Health(status="unavailable", database="unreachable")
        return Health(status="ok", database="reachable")

    @router.get("/freshness", summary="How up to date gold is", responses=UNAVAILABLE)
    async def freshness() -> Freshness:
        """Per entity, the source commit time of the newest change gold holds and its age at the
        database's clock."""
        return await repository.freshness()

    return router
