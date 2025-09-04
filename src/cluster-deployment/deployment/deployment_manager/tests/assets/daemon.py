#!/usr/bin/env python3
import cgi
import contextlib
import logging
import os
import pathlib
import shlex
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
import typing
from http.server import BaseHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)

LOGFILE = "/var/log/mock"
CONTAINER_MOCK_DIR = "/run/mock"
HOST_MOCK_DIR = os.getenv("HOST_MOCK_DIR")


def must_exec(cmd):
    subprocess.check_call(shlex.split(cmd))


def read_next_mock() -> str:
    """
    Return a mock response from the CONTAINER_MOCK_DIR/sys.argv[0]/. Mock response
    files are sorted and the returned val sorts after the previously written
    response in .last. If there is no next file, repeat the last one infinitely
    """
    cmd = sys.argv[0].split("/")[-1]
    args = " ".join(sys.argv)
    mocks_dir = pathlib.Path(CONTAINER_MOCK_DIR) / cmd
    if not mocks_dir.is_dir():
        logger.warning(f"{args} (path={mocks_dir} not found) -> ''")
        return ""

    prev_resp = ""
    prev_resp_path = pathlib.Path(mocks_dir) / ".prev_response"
    if prev_resp_path.is_file():
        prev_resp = prev_resp_path.read_text().strip()

    mock_responses = sorted([
        f for f in mocks_dir.iterdir()
        if f.is_file() and not f.name.startswith(".") and f.name >= prev_resp
    ])

    if not mock_responses:
        logger.info(f"{args} -> path=not_exists contents=''")
        return ""

    idx = min(2, len(mock_responses)) - 1
    prev_resp_path.write_text(mock_responses[idx].name)
    rs = mock_responses[idx].read_text()
    logger.info(f"{args} -> path={mock_responses[idx].name} contents={rs}")
    return rs


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write("".encode('utf-8'))

    def do_POST(self):
        logger.info(f"Received request for {self.path}")

        ctype, pdict = cgi.parse_header(self.headers.get('content-type', ""))
        if ctype == 'multipart/form-data':
            pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
            fields = cgi.parse_multipart(self.rfile, pdict)
            for field in fields:
                for item in fields[field]:
                    # Assuming a file is uploaded with the field name 'updateFile',
                    # adjust the field names as necessary
                    if field == 'updateFile':
                        # Write file data to a file
                        with open(f"./uploaded_files/{field}.json", "wb") as f:
                            f.write(item)
                        logger.info(f"Saved file for field {field}")
        self.send_response(202)
        self.end_headers()
        response_string = "Files uploaded successfully"
        self.wfile.write(response_string.encode('utf-8'))


def install_mock_commands():
    """ Symlinks mock commands to this script and copies the responses to local filesystem """
    if HOST_MOCK_DIR != "" and pathlib.Path(HOST_MOCK_DIR).is_dir():
        for dir_name in [d.name for d in pathlib.Path(HOST_MOCK_DIR).iterdir() if d.is_dir()]:
            logger.info(f"mocking command {dir_name}")
            p = pathlib.Path(f"/bin/{dir_name}")
            if p.is_file():
                p.rename(f"/bin/{dir_name}.original")
            p.touch(mode=0o755)
            p.write_text(pathlib.Path(sys.argv[0]).read_text())
        logger.info(f"copy host mock responses to container {HOST_MOCK_DIR} -> {CONTAINER_MOCK_DIR}")
        shutil.copytree(HOST_MOCK_DIR, CONTAINER_MOCK_DIR)


@contextlib.contextmanager
def run_ssh_server():
    proc = None
    try:
        proc = subprocess.Popen(["/usr/sbin/sshd", "-D", "-e"])
        yield
    finally:
        if proc:
            proc.kill()


def run_mock_server(port=443):
    """ Runs mock HTTPS server """
    os.makedirs('uploaded_files', exist_ok=True)
    httpd = HTTPServer(('', port), SimpleHTTPRequestHandler)
    if port == 443:
        must_exec(f"openssl req -new -key server.key -out server.csr -subj '/CN={socket.gethostname()}'")
        must_exec("openssl x509 -req -in server.csr -signkey server.key -out system.crt -days 365")
        httpd.socket = ssl.wrap_socket(httpd.socket, keyfile="server.key", certfile='system.crt', server_side=True)
    logger.info(f"Starting httpd server on port {port}")
    httpd.serve_forever()


def tail(fp: typing.IO) -> typing.Generator[str, None, None]:
    fp.seek(0, 2)
    while True:
        line = fp.readline().strip()
        if not line:
            time.sleep(0.1)
            continue
        yield line


def tail_logs_forever():
    must_exec(f"touch {LOGFILE}")
    with open(LOGFILE, 'r') as fp:
        for line in tail(fp):
            logging.info(line)


def main():
    """
    Usage
      run_mock_server PORT - runs a mock HTTPS server and starts sshd
      tail_mock_logs - installs mock shell commands and tails mock logs infinitely
      * - executes mock commands
    """
    cmd = "" if len(sys.argv) < 2 else sys.argv[1]
    if cmd == "run_mock_server":
        logging.basicConfig(level=logging.DEBUG)
        port = 443 if len(sys.argv) < 3 else int(sys.argv[2])
        install_mock_commands()
        with run_ssh_server():
            srv = threading.Thread(target=run_mock_server, args=(port,))
            srv.start()
            tail_logs_forever()

    elif cmd == "tail_mock_logs":
        logging.basicConfig(level=logging.DEBUG)
        install_mock_commands()
        tail_logs_forever()

    else:
        # serve a mock response
        logging.basicConfig(filename=LOGFILE, level=logging.DEBUG)
        print(read_next_mock())

    return 0


if __name__ == "__main__":
    sys.exit(main())
