#!/usr/bin/env python3
"""Run the packaged native executable against synthetic HTTP/SQLite fixtures."""

import argparse
import contextlib
import http.server
import json
import os
from pathlib import Path
import platform
import re
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


MIGRATION = '''migrate((app) => {
  const collection = new Collection({
    type: "base", name: "documents",
    listRule: '@request.auth.id != "" && owner = @request.auth.id',
    viewRule: '@request.auth.id != "" && owner = @request.auth.id',
    createRule: '@request.auth.id != "" && owner = @request.auth.id',
    updateRule: '@request.auth.id != "" && owner = @request.auth.id',
    deleteRule: '@request.auth.id != "" && owner = @request.auth.id',
    fields: [
      {type: "text", name: "owner", required: true},
      {type: "text", name: "title", required: true},
      {type: "text", name: "body"},
      {type: "json", name: "metrics"},
      {type: "text", name: "secret", hidden: true}
    ]
  });
  app.save(collection);
}, (app) => { app.delete(app.findCollectionByNameOrId("documents")); });
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    with tempfile.TemporaryDirectory(prefix="pocketcontext-release-smoke-") as temporary, contextlib.ExitStack() as cleanup:
        root = Path(temporary)
        environment = {"PATH": os.environ.get("PATH", os.defpath), "HOME": str(root), "TMPDIR": str(root)}
        empty = root / "version-only"
        empty.mkdir()
        result = subprocess.run([binary, "--version"], cwd=empty, env=environment, text=True, capture_output=True, check=True)
        assert re.fullmatch(r"pocketcontext " + re.escape(args.version) + r" \(PocketBase v[^;\s]+; commit " + re.escape(args.commit) + r"\)\n", result.stdout), result.stdout
        assert not list(empty.iterdir()), "--version created application files"
        app = root / "application"
        app.mkdir()
        migrations, hooks = app / "pb_migrations", app / "pb_hooks"
        migrations.mkdir()
        hooks.mkdir()
        # Exercise the static Linux binary's hostname resolution and trusted TLS
        # client path without a public network dependency. macOS uses its native
        # trust store, so this SSL_CERT_FILE fixture applies only to Linux.
        if platform.system() == "Linux":
            class TLSHandler(http.server.BaseHTTPRequestHandler):
                def do_GET(self):
                    data = b'{"message":"synthetic TLS response"}'
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)

                def log_message(self, *_):
                    pass

            fixtures = Path(__file__).resolve().parent / "fixtures"
            certificate = fixtures / "release_tls.crt"
            tls = http.server.ThreadingHTTPServer(("127.0.0.1", 0), TLSHandler)
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(certificate, fixtures / "release_tls.key")
            tls.socket = context.wrap_socket(tls.socket, server_side=True)
            thread = threading.Thread(target=tls.serve_forever, daemon=True)
            thread.start()
            cleanup.callback(tls.server_close)
            cleanup.callback(thread.join, 5)
            cleanup.callback(tls.shutdown)
            environment["SSL_CERT_FILE"] = str(certificate)
            trust_directory = root / "empty-trust-directory"
            trust_directory.mkdir()
            environment["SSL_CERT_DIR"] = str(trust_directory)
            url = json.dumps(f"https://localhost:{tls.server_port}/synthetic")
            (hooks / "release_tls.pb.js").write_text('''routerAdd("GET", "/release-client-probe", (e) => {
  const response = $http.send({url: URL, method: "GET", timeout: 5});
  return e.json(200, {status: response.statusCode, body: response.json});
});
'''.replace("URL", url), encoding="utf-8")
        (migrations / "1700000000_release_fixture.js").write_text(MIGRATION, encoding="utf-8")
        config = {
            "authCollection": "users", "tables": {"documents": ["id", "owner", "title", "body", "metrics"]},
            "timeoutMs": 2000, "maxRows": 50, "maxBytes": 65536,
            "snapshot": {"filters": {"documents": "documents.owner = :requester"}},
            "search": {"indexes": {"documents": {"isolation": "private", "collection": "documents",
                                                    "columns": ["title", "body"], "snippetColumn": "body"}}},
        }
        configuration = app / "pocketcontext.json"
        configuration.write_text(json.dumps(config), encoding="utf-8")
        common = [binary, "--dir", str(app / "pb_data"), "--migrationsDir", str(migrations),
                  "--hooksDir", str(hooks), "--contextConfig", str(configuration)]
        subprocess.run(common + ["superuser", "upsert", "release-admin@example.invalid", "SyntheticReleasePassword123!"],
                       cwd=app, env=environment, capture_output=True, check=True)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        base = f"http://127.0.0.1:{port}"
        with (root / "server.log").open("w+") as log:
            server = subprocess.Popen(common + ["serve", "--http", f"127.0.0.1:{port}"],
                                      cwd=app, env=environment, stdout=log, stderr=log)

            def request(method, path, body=None, token=None, expected=200):
                headers = {"Content-Type": "application/json"}
                if token:
                    headers["Authorization"] = token
                req = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                             method=method, headers=headers)
                try:
                    with urllib.request.urlopen(req, timeout=10) as response:
                        status, data, response_headers = response.status, response.read(), response.headers
                except urllib.error.HTTPError as error:
                    status, data, response_headers = error.code, error.read(), error.headers
                assert status == expected, (method, path, status, data.decode())
                return json.loads(data) if data else None, response_headers

            try:
                for _ in range(150):
                    try:
                        request("GET", "/api/health")
                        break
                    except (OSError, AssertionError):
                        if server.poll() is not None:
                            log.seek(0)
                            raise RuntimeError("Synthetic server exited: " + log.read()[-4000:])
                        time.sleep(0.1)
                else:
                    raise RuntimeError("Synthetic server did not become healthy")
                if platform.system() == "Linux":
                    probe, _ = request("GET", "/release-client-probe")
                    assert probe == {"status": 200, "body": {"message": "synthetic TLS response"}}, probe
                admin, _ = request("POST", "/api/collections/_superusers/auth-with-password",
                                   {"identity": "release-admin@example.invalid", "password": "SyntheticReleasePassword123!"})
                users, tokens = {}, {}
                for name in ("alice", "bob"):
                    user, _ = request("POST", "/api/collections/users/records", {
                        "email": f"{name}@example.invalid", "password": "SyntheticUserPassword123!",
                        "passwordConfirm": "SyntheticUserPassword123!", "name": name}, admin["token"])
                    auth, _ = request("POST", "/api/collections/users/auth-with-password",
                                      {"identity": f"{name}@example.invalid", "password": "SyntheticUserPassword123!"})
                    users[name], tokens[name] = user, auth["token"]
                documents = {}
                for name in users:
                    documents[name], _ = request("POST", "/api/collections/documents/records", {
                        "owner": users[name]["id"], "title": f"{name} rocket", "body": "Synthetic rocket instructions",
                        "metrics": [10, 20, 90], "secret": "Synthetic hidden text"}, tokens[name])

                def sql(query, who="alice", expected=200):
                    result, headers = request("POST", "/api/context/query", {"sql": query}, tokens[who], expected)
                    if expected == 200:
                        assert headers.get("X-Context-Scope") == "authorized-snapshot"
                        assert not result["truncated"]
                    return result

                query = "SELECT sqrt(81), median(j.value), percentile_cont(j.value, 0.95) FROM documents, json_each(documents.metrics) j"
                assert sql(query)["rows"] == [[9, 20, 83]]
                for name in users:
                    assert sql("SELECT id, title FROM documents", name)["rows"] == [[documents[name]["id"], f"{name} rocket"]]
                    found, _ = request("POST", "/api/context/search", {"index": "documents", "query": "rocket"}, tokens[name])
                    assert [hit["id"] for hit in found["hits"]] == [documents[name]["id"]], found
                for query in ("SELECT email FROM users", "SELECT secret FROM documents", "SELECT sql FROM sqlite_schema",
                              "DELETE FROM documents", "SELECT load_extension('anything')"):
                    sql(query, expected=400)
                request("POST", "/api/context/query", {"sql": "SELECT id FROM documents"}, expected=401)
                request("GET", "/api/collections/documents/records/" + documents["bob"]["id"], token=tokens["alice"], expected=404)
                request("PATCH", "/api/collections/documents/records/" + documents["alice"]["id"],
                        {"title": "Updated synthetic title", "metrics": [30, 40, 50]}, tokens["alice"])
                assert sql("SELECT title FROM documents")["rows"] == [["Updated synthetic title"]]
                assert sql("SELECT median(j.value) FROM documents, json_each(documents.metrics) j")["rows"] == [[40]]
            finally:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait()
    print("Release smoke passed: version, HTTP, REST, filtered SQL, JSON/math/percentiles, FTS5 and authorization"
          + (", localhost resolution and trusted TLS" if platform.system() == "Linux" else ""))


if __name__ == "__main__":
    main()
