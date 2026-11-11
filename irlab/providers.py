"""HTTP clients for the providers' REST APIs (outbox dispatcher, reconciliation).

Two wirings, same code path:

* Docker Compose / real deployments: `POS_API_URL`, `PAYMENTS_API_URL`,
  `LOYALTY_API_URL` point at the provider services.
* Local runs and tests with MOUNT_MOCKS=true: the mocks live inside the hub app,
  reached in-process through `httpx.ASGITransport` at /mock/{name}.

ASGITransport ignores httpx timeouts, so callers enforce deadlines with
`asyncio.timeout` (see `irlab.outbox`). The httpx timeout below is the
belt-and-braces for real sockets.
"""

from collections.abc import Callable

import httpx

from irlab.config import SOURCES, Settings


class ProviderNotConfigured(RuntimeError):
    pass


class ProviderClients:
    def __init__(self, settings: Settings, asgi_app_factory: Callable[[], object] | None = None):
        self._settings = settings
        self._asgi_app_factory = asgi_app_factory
        self._clients: dict[str, httpx.AsyncClient] = {}

    def configured(self, name: str) -> bool:
        return bool(self._settings.provider_api_url(name)) or (
            self._settings.mount_mocks and self._asgi_app_factory is not None
        )

    def get(self, name: str) -> httpx.AsyncClient:
        if name not in SOURCES:
            raise ProviderNotConfigured(f"unknown provider {name}")
        client = self._clients.get(name)
        if client is not None:
            return client
        url = self._settings.provider_api_url(name)
        timeout = httpx.Timeout(self._settings.outbound_timeout_s)
        if url:
            client = httpx.AsyncClient(base_url=url.rstrip("/"), timeout=timeout)
        elif self._settings.mount_mocks and self._asgi_app_factory is not None:
            client = httpx.AsyncClient(
                transport=httpx.ASGITransport(app=self._asgi_app_factory()),
                base_url=f"http://hub/mock/{name}",
                timeout=timeout,
            )
        else:
            raise ProviderNotConfigured(f"{name.upper()}_API_URL is not set and the mocks are not mounted")
        self._clients[name] = client
        return client

    async def aclose(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        self._clients.clear()
