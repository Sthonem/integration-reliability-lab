"""Wires the hub's background components; shared by the API process and `irlab.worker`."""

import random
from collections.abc import Callable
from dataclasses import dataclass

from irlab.alerts import AlertEvaluator
from irlab.clock import Clock, SystemClock
from irlab.config import Settings
from irlab.outbox import Dispatcher
from irlab.processing import Processor
from irlab.providers import ProviderClients
from irlab.reconcile import Reconciler
from irlab.replay import ReplayService
from irlab.triage import TriageProvider, build_triage_provider
from irlab.worker import Worker


@dataclass
class Runtime:
    settings: Settings
    clock: Clock
    providers: ProviderClients
    processor: Processor
    dispatcher: Dispatcher
    replay: ReplayService
    worker: Worker
    reconciler: Reconciler
    alerts: AlertEvaluator
    triage: TriageProvider

    async def aclose(self) -> None:
        await self.providers.aclose()


def build_runtime(
    settings: Settings,
    *,
    clock: Clock | None = None,
    asgi_app_factory: Callable[[], object] | None = None,
    rng: random.Random | None = None,
) -> Runtime:
    clock = clock or SystemClock()
    providers = ProviderClients(settings, asgi_app_factory)
    processor = Processor(settings, clock)
    dispatcher = Dispatcher(settings, providers, clock, rng)
    replay = ReplayService(processor, dispatcher, clock)
    reconciler = Reconciler(settings, providers, clock)
    alerts = AlertEvaluator(settings, clock)
    worker = Worker(
        settings,
        processor,
        dispatcher,
        reconcile=lambda: reconciler.run(triggered_by="worker"),
        evaluate_alerts=alerts.evaluate,
    )
    return Runtime(settings, clock, providers, processor, dispatcher, replay, worker, reconciler, alerts,
                   build_triage_provider(settings))
