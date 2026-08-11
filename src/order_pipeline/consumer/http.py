"""A minimal HTTP server exposing the consumer's folded state (R1.31, D13).

This exists so state loss across a restart is a direct before/after diff of two
``curl`` outputs, rather than something inferred from log volume:

    curl -s localhost:8001/state > before.json
    # restart the consumer
    curl -s localhost:8001/state > after.json
    diff before.json after.json

It uses the standard library rather than FastAPI — the consumer is not a web
service, and one read-only endpoint does not justify an ASGI stack.
"""

import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from order_pipeline.consumer.state import OrderStateStore

logger = logging.getLogger(__name__)


def _make_handler(store: OrderStateStore) -> type[BaseHTTPRequestHandler]:
    """Build a request handler bound to a state store.

    Args:
        store: The store to expose.

    Returns:
        A handler class serving ``GET /state`` and ``GET /health``.
    """

    class StateHandler(BaseHTTPRequestHandler):
        """Serves the consumer's folded state as JSON."""

        def do_GET(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
            """Handle a GET request."""
            if self.path.rstrip("/") in ("/state", ""):
                body = json.dumps(store.snapshot(), indent=2).encode("utf-8")
                status = 200
            elif self.path.rstrip("/") == "/health":
                body = json.dumps({"status": "ok"}).encode("utf-8")
                status = 200
            else:
                body = json.dumps({"detail": f"no route {self.path}"}).encode("utf-8")
                status = 404

            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            """Route access logs through the module logger instead of stderr.

            Args:
                format: Printf-style format string.
                *args: Format arguments.
            """
            logger.debug("state server: " + format, *args)

    return StateHandler


def start_state_server(
    store: OrderStateStore, host: str, port: int
) -> ThreadingHTTPServer:
    """Serve the state store on a background thread.

    Args:
        store: The store to expose.
        host: Bind address.
        port: Bind port.

    Returns:
        The running server, so the caller can shut it down.
    """
    server = ThreadingHTTPServer((host, port), _make_handler(store))
    thread = threading.Thread(
        target=server.serve_forever, name="consumer-state-server", daemon=True
    )
    thread.start()
    logger.info("state server listening on %s:%d/state", host, port)
    return server
