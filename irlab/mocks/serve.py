"""Standalone entrypoints, one process per provider (how Docker Compose runs them in M5):

    MOCK_POS_WEBHOOK_URL=http://localhost:8000/webhooks/pos \\
    MOCK_POS_WEBHOOK_SECRET=<first POS_WEBHOOK_SECRETS entry> \\
    uvicorn irlab.mocks.serve:pos_app --port 8101

For local development it is simpler to let the hub mount all three
(MOUNT_MOCKS=true, the default): they then live under /mock/{pos,payments,loyalty}.
"""

from irlab.mocks import MockConfig, create_loyalty_app, create_payments_app, create_pos_app

pos_app = create_pos_app(MockConfig.from_env("pos"))
payments_app = create_payments_app(MockConfig.from_env("payments"))
loyalty_app = create_loyalty_app(MockConfig.from_env("loyalty"))
