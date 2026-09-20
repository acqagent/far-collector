"""Tests for the LLM client layer against a stub OpenAI-compatible server.

No GPU and no real backend: a threaded http.server plays the role of SGLang so
the auth, model-id, constrained-decoding and fallback paths are all exercised
in CI.
"""
import importlib
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest


# --------------------------------------------------------------- stub server

class _State:
    def __init__(self):
        self.served_ids = ["qwen3.8-27b"]
        self.require_auth = None      # set to a token to enforce it
        self.reject_json_schema = False
        self.reply = {"agency": "DOE"}
        self.requests = []            # parsed bodies of /chat/completions calls


class _Handler(BaseHTTPRequestHandler):
    state: _State

    def log_message(self, *a):        # keep pytest output clean
        pass

    def _send(self, code, payload):
        body = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authed(self):
        want = self.state.require_auth
        if want is None:
            return True
        return self.headers.get("Authorization") == f"Bearer {want}"

    def do_GET(self):
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})
        if self.path.endswith("/models"):
            return self._send(200, {"data": [{"id": i} for i in self.state.served_ids]})
        self._send(404, {"error": "not found"})

    def do_POST(self):
        if not self._authed():
            return self._send(401, {"error": "unauthorized"})
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n) or b"{}")
        self.state.requests.append(body)
        fmt = (body.get("response_format") or {}).get("type")
        if fmt == "json_schema" and self.state.reject_json_schema:
            return self._send(400, {"error": {"message": "json_schema unsupported"}})
        content = self.state.reply if isinstance(self.state.reply, str) else json.dumps(self.state.reply)
        self._send(200, {
            "id": "stub", "object": "chat.completion", "created": 0, "model": "qwen3.8-27b",
            "choices": [{
                "index": 0, "finish_reason": "stop",
                "message": {"role": "assistant", "content": content},
            }],
        })


@pytest.fixture
def backend(monkeypatch):
    """A stub server plus config/models reloaded to point at it."""
    state = _State()
    handler = type("H", (_Handler,), {"state": state})
    srv = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}/v1"

    import config
    import models
    monkeypatch.setenv("FAR_LLM_BASE_URL", base)
    monkeypatch.setenv("FAR_LLM_MODEL", "qwen3.8-27b")
    monkeypatch.setenv("FAR_LLM_API_KEY", "stub-key")
    monkeypatch.setenv("FAR_LLM_PROBE_TIMEOUT", "5")
    importlib.reload(config)
    importlib.reload(models)
    try:
        yield state, models
    finally:
        srv.shutdown()
        srv.server_close()
        monkeypatch.undo()
        importlib.reload(config)
        importlib.reload(models)


# --------------------------------------------------------------- pure helpers

def test_strip_thinking_removes_complete_block():
    import models
    assert models.strip_thinking('<think>weighing it up</think>{"a": 1}') == '{"a": 1}'


def test_strip_thinking_removes_unterminated_prefix():
    import models
    assert models.strip_thinking('<think>cut off mid-thought') == ""


def test_strip_thinking_leaves_plain_json_alone():
    import models
    assert models.strip_thinking('{"a": 1}') == '{"a": 1}'


def test_json_slice_unwraps_code_fence():
    import models
    assert json.loads(models.json_slice('```json\n{"a": 1}\n```')) == {"a": 1}


def test_json_slice_finds_object_in_prose():
    import models
    assert json.loads(models.json_slice('Here you go:\n{"a": 1}\nHope that helps.')) == {"a": 1}


def test_api_key_prefers_env_over_file(monkeypatch, tmp_path):
    import config
    key_file = tmp_path / "api-key"
    key_file.write_text("from-file\n")
    monkeypatch.setenv("FAR_LLM_API_KEY", "from-env")
    assert config.read_api_key(key_file) == "from-env"


def test_api_key_falls_back_to_file_then_placeholder(monkeypatch, tmp_path):
    import config
    monkeypatch.delenv("FAR_LLM_API_KEY", raising=False)
    key_file = tmp_path / "api-key"
    key_file.write_text("  from-file \n")
    assert config.read_api_key(key_file) == "from-file"
    assert config.read_api_key(tmp_path / "nope") == "local"


def test_thinking_mode_rejects_junk(monkeypatch):
    import config
    try:
        monkeypatch.setenv("FAR_LLM_THINKING", "banana")
        importlib.reload(config)
        assert config.LLM_THINKING == "off"
        monkeypatch.setenv("FAR_LLM_THINKING", "MEDIUM")
        importlib.reload(config)
        assert config.LLM_THINKING == "medium"
    finally:
        monkeypatch.undo()
        importlib.reload(config)


# ------------------------------------------------------------------- probe

def test_probe_accepts_matching_model(backend):
    state, models = backend
    ok, detail = models.probe()
    assert ok, detail


def test_probe_rejects_model_id_mismatch(backend):
    state, models = backend
    state.served_ids = ["RadixArk/Qwen3.8-27B-NVFP4"]
    ok, detail = models.probe()
    assert not ok
    assert "not served here" in detail


def test_probe_reports_auth_failure_rather_than_down(backend):
    state, models = backend
    state.require_auth = "a-different-key"
    ok, detail = models.probe()
    assert not ok
    assert "401" in detail and "FAR_LLM_API_KEY" in detail


def test_probe_sends_bearer_token(backend):
    state, models = backend
    state.require_auth = "stub-key"
    ok, detail = models.probe()
    assert ok, detail


# ------------------------------------------------------------ complete_json

def _run(coro):
    import asyncio
    return asyncio.run(coro)


def test_complete_json_validates_into_the_schema_model(backend):
    state, models = backend
    import extract
    state.reply = {"deviations": [{
        "agency": "DOE", "deviation_number": "CD-2025-04", "title": "Part 15",
        "effective_date": "2025-10-15", "scope": "Applies to all solicitations.",
        "link": None,
    }]}
    page = _run(models.complete_json(
        extract.ClassDeviationPage, [{"role": "system", "content": "x"}, {"role": "user", "content": "y"}]
    ))
    assert page.deviations[0].deviation_number == "CD-2025-04"


def test_complete_json_requests_constrained_decoding_and_no_thinking(backend):
    state, models = backend
    import extract
    state.reply = {"deviations": []}
    _run(models.complete_json(extract.ClassDeviationPage, [{"role": "system", "content": "x"}]))
    sent = state.requests[-1]
    assert sent["response_format"]["type"] == "json_schema"
    assert sent["response_format"]["json_schema"]["name"] == "ClassDeviationPage"
    assert sent["chat_template_kwargs"] == {"enable_thinking": False}
    assert sent["max_tokens"] > 0


def test_complete_json_falls_back_when_schema_mode_rejected(backend):
    state, models = backend
    import extract
    state.reject_json_schema = True
    state.reply = {"deviations": []}
    page = _run(models.complete_json(extract.ClassDeviationPage, [{"role": "system", "content": "x"}]))
    assert page.deviations == []
    assert state.requests[-1]["response_format"]["type"] == "json_object"
    # The schema still reaches the model, in the prompt.
    assert "JSON Schema" in state.requests[-1]["messages"][0]["content"]


def test_complete_json_raises_on_unparseable_output(backend):
    state, models = backend
    import extract
    state.reply = "not json at all"
    with pytest.raises(models.LLMError):
        _run(models.complete_json(extract.ClassDeviationPage, [{"role": "system", "content": "x"}]))


def test_complete_json_strips_inline_thinking(backend):
    state, models = backend
    import extract
    state.reply = '<think>let me see</think>{"deviations": []}'
    page = _run(models.complete_json(extract.ClassDeviationPage, [{"role": "system", "content": "x"}]))
    assert page.deviations == []


def test_thinking_effort_is_forwarded_when_enabled(backend, monkeypatch):
    state, models = backend
    import config
    import extract
    monkeypatch.setenv("FAR_LLM_THINKING", "medium")
    importlib.reload(config)
    state.reply = {"deviations": []}
    _run(models.complete_json(extract.ClassDeviationPage, [{"role": "system", "content": "x"}]))
    sent = state.requests[-1]
    assert sent["reasoning_effort"] == "medium"
    assert sent["chat_template_kwargs"] == {"enable_thinking": True}
