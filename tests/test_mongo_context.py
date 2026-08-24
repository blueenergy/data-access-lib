from __future__ import annotations

import pytest

from stock_data_access.mongo_context import _build_client, pool_options


@pytest.fixture(autouse=True)
def _real_client(monkeypatch):
    monkeypatch.delenv("USE_MOCK_MONGO", raising=False)
    monkeypatch.delenv("MONGO_MAX_IDLE_TIME_MS", raising=False)
    monkeypatch.delenv("MONGO_MAX_POOL_SIZE", raising=False)


# --------------------------------------------------------------------------
# pool_options
# --------------------------------------------------------------------------
def test_pool_options_defaults_bound_both_dimensions():
    opts = pool_options()
    assert opts["maxIdleTimeMS"] == 60000
    assert opts["maxPoolSize"] == 50


def test_pool_options_reads_env_overrides(monkeypatch):
    monkeypatch.setenv("MONGO_MAX_IDLE_TIME_MS", "15000")
    monkeypatch.setenv("MONGO_MAX_POOL_SIZE", "20")
    opts = pool_options()
    assert opts["maxIdleTimeMS"] == 15000
    assert opts["maxPoolSize"] == 20


# --------------------------------------------------------------------------
# _build_client
# --------------------------------------------------------------------------
def test_build_client_reclaims_idle_connections():
    # Regression: pymongo's default maxIdleTimeMS=None never reclaims, so a
    # long-lived process ratchets to maxPoolSize and exhausts mongod's fds.
    client = _build_client("mongodb://localhost:27017")
    try:
        pool = client.options.pool_options
        assert pool.max_idle_time_seconds == 60
        assert pool.max_pool_size == 50
    finally:
        client.close()


def test_build_client_honours_env_overrides(monkeypatch):
    monkeypatch.setenv("MONGO_MAX_IDLE_TIME_MS", "30000")
    monkeypatch.setenv("MONGO_MAX_POOL_SIZE", "10")
    client = _build_client("mongodb://localhost:27017")
    try:
        pool = client.options.pool_options
        assert pool.max_idle_time_seconds == 30
        assert pool.max_pool_size == 10
    finally:
        client.close()
