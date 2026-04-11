"""HTTP client for the Cloud Control-Plane API."""

import httpx
import structlog

from app.models.config import CollectorConfig
from app.settings import get_settings
from app.utils.backoff import retry_with_backoff

__all__ = ["fetch_collector_config", "ApiClientError"]

log = structlog.get_logger()


class ApiClientError(Exception):
    """Raised when the Cloud API returns an unrecoverable error."""


async def fetch_collector_config() -> CollectorConfig:
    """GET ``/api/v1/collector/config`` with exponential-backoff retries.

    Retries on network errors and 5xx responses. Raises
    :class:`ApiClientError` on 4xx (non-retryable) or after exhausting
    retries.
    """
    settings = get_settings()

    url = f"{settings.CLOUD_API_URL}api/v1/collector/config"
    headers = {
        "X-API-Key-ID": settings.X_API_KEY_ID,
        "X-API-Key-Secret": settings.X_API_KEY_SECRET.get_secret_value(),
    }

    async def _do_request() -> CollectorConfig:
        async with httpx.AsyncClient(timeout=30.0) as client:
            log.info("fetching collector config", url=url)
            resp = await client.get(url, headers=headers)

            if 400 <= resp.status_code < 500:
                body = resp.text
                log.error(
                    "non-retryable API error",
                    status=resp.status_code,
                    body=body[:500],
                )
                raise ApiClientError(
                    f"Cloud API returned {resp.status_code}: {body[:200]}"
                )

            resp.raise_for_status()  # raises on 5xx → triggers retry

            config = CollectorConfig.model_validate(resp.json())
            log.info("body", config=config)
            log.info(
                "collector config loaded",
                collector_id=str(config.id),
                collector_name=config.name,
                sensor_count=len(config.sensors),
            )
            return config

    return await retry_with_backoff(
        _do_request,
        max_retries=settings.BACKOFF_MAX_RETRIES,
        base_delay=settings.BACKOFF_BASE_S,
        max_delay=settings.BACKOFF_MAX_S,
        operation_name="fetch_collector_config",
        fatal_exceptions=(ApiClientError,),
    )
