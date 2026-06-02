import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_rate_limit_module():
    path = ROOT / "demo-api" / "rate_limit.py"
    spec = importlib.util.spec_from_file_location("rate_limit", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_incident_rate_limiter_enforces_minimum_interval():
    rate_limit = load_rate_limit_module()
    now = [100.0]
    limiter = rate_limit.IncidentRateLimiter(
        min_interval_seconds=3,
        window_seconds=60,
        max_requests=10,
        clock=lambda: now[0],
    )

    assert limiter.check() == (True, 0)
    now[0] = 101.0

    allowed, retry_after = limiter.check()

    assert allowed is False
    assert retry_after == 2


def test_incident_rate_limiter_enforces_window_limit():
    rate_limit = load_rate_limit_module()
    now = [100.0]
    limiter = rate_limit.IncidentRateLimiter(
        min_interval_seconds=0,
        window_seconds=10,
        max_requests=2,
        clock=lambda: now[0],
    )

    assert limiter.check() == (True, 0)
    now[0] = 101.0
    assert limiter.check() == (True, 0)
    now[0] = 102.0

    allowed, retry_after = limiter.check()

    assert allowed is False
    assert retry_after == 8


def test_incident_rate_limiter_expires_old_events():
    rate_limit = load_rate_limit_module()
    now = [100.0]
    limiter = rate_limit.IncidentRateLimiter(
        min_interval_seconds=0,
        window_seconds=10,
        max_requests=2,
        clock=lambda: now[0],
    )

    assert limiter.check() == (True, 0)
    now[0] = 101.0
    assert limiter.check() == (True, 0)
    now[0] = 111.0

    assert limiter.check() == (True, 0)
