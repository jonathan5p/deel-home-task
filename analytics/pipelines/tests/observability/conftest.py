import threading
from collections.abc import Iterator
from http.server import ThreadingHTTPServer

import pytest

from analytics.pipelines.tests.observability.stage_exporter_setup import EchoPath


@pytest.fixture
def history_server() -> Iterator[str]:
    """The base URL of a local HTTP server that echoes the requested path."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), EchoPath)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/"
    finally:
        server.shutdown()
        server.server_close()
