from __future__ import annotations

import json
import os
import sys
import uuid
from typing import Any

_SAFE_ENV_NAME = "COPILOTD_ACCEPTANCE_VALUE"


def _send(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def _result(request_id: Any, result: Any) -> None:
    _send({"jsonrpc": "2.0", "id": request_id, "result": result})


def _error(request_id: Any, code: int, message: str) -> None:
    _send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": code, "message": message},
        }
    )


def _request_client(method: str, params: dict[str, Any]) -> dict[str, Any]:
    request_id = f"copilotd-{uuid.uuid4()}"
    _send(
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
    )
    while line := sys.stdin.readline():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if message.get("id") == request_id and ("result" in message or "error" in message):
            return message
        if "method" in message and "id" in message:
            _handle_request(message)
    return {
        "jsonrpc": "2.0",
        "id": request_id,
        "error": {"code": -32000, "message": "client disconnected"},
    }


def _handle_request(message: dict[str, Any]) -> None:
    request_id = message.get("id")
    method = message.get("method")
    params = message.get("params")
    if method == "initialize":
        _result(
            request_id,
            {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {
                    "name": "copilotd-disposable-acceptance",
                    "version": "1.0.0",
                },
            },
        )
        return
    if method == "ping":
        _result(request_id, {})
        return
    if method == "tools/list":
        _result(
            request_id,
            {
                "tools": [
                    {
                        "name": "echo",
                        "description": "Returns the supplied acceptance text.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"text": {"type": "string"}},
                            "required": ["text"],
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "get_acceptance_env",
                        "description": "Returns one fixed non-secret acceptance value.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "request_elicitation",
                        "description": "Requests a bounded test form from the host.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    },
                    {
                        "name": "request_sampling",
                        "description": "Requests one MCP sampling response from the host.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {},
                            "additionalProperties": False,
                        },
                    },
                ]
            },
        )
        return
    if method != "tools/call" or not isinstance(params, dict):
        _error(request_id, -32601, f"unsupported method: {method}")
        return
    name = params.get("name")
    arguments = params.get("arguments")
    arguments = arguments if isinstance(arguments, dict) else {}
    if name == "echo":
        text = str(arguments.get("text") or "")
        _result(request_id, {"content": [{"type": "text", "text": text}]})
        return
    if name == "get_acceptance_env":
        _result(
            request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": os.environ.get(_SAFE_ENV_NAME, ""),
                    }
                ]
            },
        )
        return
    if name == "request_elicitation":
        response = _request_client(
            "elicitation/create",
            {
                "message": "Provide disposable acceptance values.",
                "requestedSchema": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "label": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 32,
                        },
                        "enabled": {"type": "boolean"},
                    },
                    "required": ["label", "enabled"],
                },
            },
        )
        _result(
            request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            response.get("result", response.get("error", {})),
                            sort_keys=True,
                        ),
                    }
                ]
            },
        )
        return
    if name == "request_sampling":
        response = _request_client(
            "sampling/createMessage",
            {
                "messages": [
                    {
                        "role": "user",
                        "content": {
                            "type": "text",
                            "text": "Reply with COPILOTD_SAMPLE_OK.",
                        },
                    }
                ],
                "maxTokens": 16,
            },
        )
        _result(
            request_id,
            {
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            response.get("result", response.get("error", {})),
                            sort_keys=True,
                        ),
                    }
                ]
            },
        )
        return
    _error(request_id, -32602, f"unknown tool: {name}")


def main() -> None:
    while line := sys.stdin.readline():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "method" in message and "id" in message:
            _handle_request(message)


if __name__ == "__main__":
    main()
