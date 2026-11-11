"""Mock external systems: POS, payment provider, loyalty platform."""

from irlab.mocks.common import MockConfig
from irlab.mocks.loyalty import create_loyalty_app
from irlab.mocks.payments import create_payments_app
from irlab.mocks.pos import create_pos_app

MOCK_FACTORIES = {
    "pos": create_pos_app,
    "payments": create_payments_app,
    "loyalty": create_loyalty_app,
}

__all__ = ["MOCK_FACTORIES", "MockConfig", "create_loyalty_app", "create_payments_app", "create_pos_app"]
