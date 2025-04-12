from src.messaging.middleware import Middleware
from src.utils.middleware_config import MiddlewareConfig

def test_init_not_none():
    middleware = Middleware(MiddlewareConfig())

    assert middleware is not None

