import threading
import pytest
import requests
import socket
from node_ipmi_exporter import NodeIPMIExporter, ThreadingSimpleServer


def find_free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port


@pytest.fixture(scope="module")
def http_server():
    port = find_free_port()
    server = ThreadingSimpleServer(('localhost', port), NodeIPMIExporter)
    server_thread = server._thread = threading.Thread(target=server.serve_forever)
    server_thread.start()
    yield f'http://localhost:{port}'
    server.shutdown()
    server_thread.join()


# if no target specified, should return empty metrics
def test_describe_metrics(http_server):
    response = requests.get(http_server)
    assert response.status_code == 200
    assert "node_ipmi_health_status" in response.text
    assert "node_ipmi_info" in response.text
    assert "node_ipmi_memory_installed" in response.text
    assert "node_thermals_fan" in response.text
    assert "node_thermals_thermometer" in response.text
