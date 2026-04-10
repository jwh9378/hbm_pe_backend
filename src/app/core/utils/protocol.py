import json
from typing import Any

JsonDict = dict[str, Any]


class ProtocolError(Exception):
    """Invalid protocol payload."""


# =========================
# Generic JSON line helpers
# =========================
def encode_json_line(payload: JsonDict) -> bytes:
    return (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")


def decode_json_line(line: bytes | str) -> JsonDict:
    try:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        data = json.loads(str(line).strip())
        if not isinstance(data, dict):
            raise SyntaxError("JSON payload must be an object")
        return data
    except json.JSONDecodeError as e:
        raise SyntaxError(f"Invalid JSON line: {e}") from e


# =========================
# Backend -> Bridge Pi
# =========================
def cmd_health_check() -> JsonDict:
    return {"type": "HEALTH_CHECK"}


def cmd_start_test(test_plan: str) -> JsonDict:
    return {"type": "START_TEST", "payload": {"test_plan": test_plan}}


def cmd_abort_test() -> JsonDict:
    return {"type": "ABORT_TEST"}


# =========================
# Bridge Pi -> Backend
# =========================
def health_check_response(response: JsonDict) -> str:
    if response.get("ok", False):
        if "data" not in response:
            ProtocolError("Invalid protocol : 'data' not in response")
        if "b_status" not in response.get("data", {}):
            ProtocolError("Invalid protocol : 'b_status' not in response.data")
        return str(response.get("data", {}).get("b_status", "not ready"))
    return "not ready"
