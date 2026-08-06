from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_TOKEN_ENV = "COPILOTD_OAUTH_ACCEPTANCE_TOKEN"


class _Handler(BaseHTTPRequestHandler):
    server_version = "copilotd-disposable-oauth/1"

    def log_message(self, _format: str, *_args: object) -> None:
        return

    @property
    def base_url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def do_GET(self) -> None:
        if self.path == "/.well-known/oauth-protected-resource":
            self._json(
                HTTPStatus.OK,
                {
                    "resource": f"{self.base_url}/mcp",
                    "authorization_servers": [self.base_url],
                    "scopes_supported": ["mcp.read"],
                    "bearer_methods_supported": ["header"],
                },
            )
            return
        if self.path == "/.well-known/oauth-authorization-server":
            self._json(
                HTTPStatus.OK,
                {
                    "issuer": self.base_url,
                    "authorization_endpoint": f"{self.base_url}/authorize",
                    "token_endpoint": f"{self.base_url}/token",
                    "response_types_supported": ["code"],
                    "grant_types_supported": ["authorization_code"],
                },
            )
            return
        if self.path == "/health":
            self._json(HTTPStatus.OK, {"ok": True})
            return
        self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})

    def do_POST(self) -> None:
        if self.path != "/mcp":
            self._json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        expected = os.environ.get(_TOKEN_ENV, "")
        if not expected or self.headers.get("Authorization") != f"Bearer {expected}":
            self.send_response(HTTPStatus.UNAUTHORIZED)
            self.send_header(
                "WWW-Authenticate",
                (
                    "Bearer "
                    f'resource_metadata="{self.base_url}/.well-known/'
                    'oauth-protected-resource", '
                    'scope="mcp.read", error="invalid_token"'
                ),
            )
            self.send_header("Content-Type", "application/json")
            body = b'{"error":"invalid_token"}'
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        length = int(self.headers.get("Content-Length", "0"))
        try:
            message = json.loads(self.rfile.read(length) or b"null")
        except json.JSONDecodeError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid_json"})
            return
        response = self._handle_message(message)
        if response is None:
            self.send_response(HTTPStatus.ACCEPTED)
            self.send_header("Mcp-Session-Id", "copilotd-oauth-acceptance")
            self.end_headers()
            return
        self._json(
            HTTPStatus.OK,
            response,
            headers={"Mcp-Session-Id": "copilotd-oauth-acceptance"},
        )

    def _handle_message(self, message: Any) -> dict[str, Any] | None:
        if not isinstance(message, dict) or "id" not in message:
            return None
        request_id = message["id"]
        method = message.get("method")
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "copilotd-oauth-acceptance",
                        "version": "1.0.0",
                    },
                },
            }
        if method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "whoami",
                            "description": "Returns the disposable test principal.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {},
                                "additionalProperties": False,
                            },
                        }
                    ]
                },
            }
        if method == "tools/call":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "copilotd-oauth-acceptance",
                        }
                    ]
                },
            }
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"unsupported method: {method}"},
        }

    def _json(
        self,
        status: HTTPStatus,
        payload: Any,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)

    host, port = server.server_address
    print(f"Listening: http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
