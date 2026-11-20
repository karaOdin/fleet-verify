"""The control panel on the public page. Behind nginx every request reaches the app from 127.0.0.1,
so the token is the only thing that opens the controls.

The action used here is "nope": past the token check it answers 404 (unknown action), refused it
answers 403, so no test ever starts, stops or resets anything.
"""
import pytest
from fastapi.testclient import TestClient

from viz import server

GOOD = "s3cret-token-for-tests"
# what a request through the reverse proxy on the same host looks like
PROXIED = {"X-Forwarded-For": "127.0.0.1", "X-Real-IP": "127.0.0.1", "X-Forwarded-Proto": "https"}


@pytest.fixture
def client():
    return TestClient(server.app, client=("127.0.0.1", 50000))


@pytest.mark.parametrize("headers", [{}, PROXIED, {"X-Admin-Token": ""}, {"X-Admin-Token": "anything"}])
@pytest.mark.parametrize("action", ["nope", "stop-replay"])
def test_no_token_configured_refuses_everything(client, monkeypatch, headers, action):
    monkeypatch.setattr(server, "ADMIN", None)
    r = client.post(f"/api/control/{action}", headers=headers)
    assert r.status_code == 403
    assert "VIZ_ADMIN_TOKEN is not set" in r.json()["detail"]
    assert client.get("/api/auth", headers=headers).status_code == 403


def test_empty_token_configured_refuses_everything(client, monkeypatch):
    monkeypatch.setattr(server, "ADMIN", "")
    assert client.post("/api/control/nope", headers={"X-Admin-Token": ""}).status_code == 403


@pytest.mark.parametrize("headers", [PROXIED, {**PROXIED, "X-Admin-Token": "wrong"},
                                     {**PROXIED, "X-Admin-Token": GOOD + "x"}, {"X-Admin-Token": "توكن".encode()}])
def test_client_address_is_never_trusted(client, monkeypatch, headers):
    monkeypatch.setattr(server, "ADMIN", GOOD)
    assert client.post("/api/control/nope", headers=headers).status_code == 403
    assert client.get("/api/auth", headers=headers).status_code == 403


def test_right_token_passes(client, monkeypatch):
    monkeypatch.setattr(server, "ADMIN", GOOD)
    assert client.post("/api/control/nope", headers={**PROXIED, "X-Admin-Token": GOOD}).status_code == 404
    assert client.get("/api/auth", headers={"X-Admin-Token": GOOD}).json() == {"ok": True}


def test_panel_hidden_for_viewers(client, monkeypatch):
    monkeypatch.setattr(server, "ADMIN", GOOD)
    html = client.get("/").text
    assert '<div class="panel ctl" id="ctl" hidden>' in html       # hidden until /api/auth says yes
    assert "[hidden]{display:none!important}" in html              # .panel's display:grid cannot undo it
    assert client.get("/api/graph", headers=PROXIED).json()["controls"] is False
    assert client.get("/api/graph", headers={"X-Admin-Token": "wrong"}).json()["controls"] is False
    assert client.get("/api/graph", headers={"X-Admin-Token": GOOD}).json()["controls"] is True
    monkeypatch.setattr(server, "ADMIN", None)
    assert client.get("/api/graph", headers={"X-Admin-Token": GOOD}).json()["controls"] is False
