"""serve --metrics command-line validation tests."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
import unittest
from http.client import HTTPConnection
from tempfile import TemporaryDirectory

TOKEN = "full-token"


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class MetricsArgumentsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.config = os.path.join(self.tmp.name, "auth.jsonl")
        with open(self.config, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(
                ["full", _digest(TOKEN), None, "*", "*", "*"]) + "\n")
        self.journal = os.path.join(self.tmp.name, "audit.json")

    def _run(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "carbon_market", "serve", *args],
            capture_output=True, timeout=10)

    def test_metrics_requires_audit_and_auth(self) -> None:
        for args in (
            ("--metrics",),
            ("--metrics", "--audit", self.journal),
            ("--metrics", "--audit", self.journal, "--token", TOKEN),
            ("--metrics", "--auth", self.config),
        ):
            with self.subTest(args=args):
                # A serve usage error exits with argparse's status 2 and
                # never binds the port.
                self.assertEqual(self._run(*args).returncode, 2)

    def test_repeated_or_valued_metrics_is_a_usage_error(self) -> None:
        for args in (
            ("--audit", self.journal, "--auth", self.config,
             "--metrics", "--metrics"),
            ("--audit", self.journal, "--auth", self.config,
             "--metrics=on"),
        ):
            with self.subTest(args=args):
                self.assertEqual(self._run(*args).returncode, 2)

    def test_metrics_with_token_pair_is_rejected_even_with_auth_absent(
            self) -> None:
        # --metrics may never ride on the single-token method.
        result = self._run("--audit", self.journal, "--token", TOKEN,
                           "--metrics")
        self.assertEqual(result.returncode, 2)

    def test_valid_metrics_starts_and_serves(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", self.journal, "--auth", self.config,
             "--metrics"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    connection.request("GET", "/metrics",
                                       headers={"X-Audit-Token": TOKEN})
                    response = connection.getresponse()
                    status = response.status
                    body = response.read()
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            self.assertEqual(status, 200)
            snapshot = json.loads(body)
            self.assertEqual(snapshot["version"], 1)
            self.assertEqual(snapshot["total"], 0)
            self.assertEqual(snapshot["routes"], {})
            self.assertGreaterEqual(snapshot["started"], 0)
        finally:
            process.terminate()
            process.wait(timeout=10)

    def test_disabled_metrics_is_not_listened_as_a_feature(self) -> None:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        process = subprocess.Popen(
            [sys.executable, "-m", "carbon_market", "serve",
             "--host", "127.0.0.1", "--port", str(port),
             "--audit", self.journal, "--auth", self.config],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.time() + 10
            status = None
            while time.time() < deadline:
                try:
                    connection = HTTPConnection("127.0.0.1", port,
                                                timeout=1)
                    connection.request("GET", "/metrics",
                                       headers={"X-Audit-Token": TOKEN})
                    status = connection.getresponse().status
                    connection.close()
                    break
                except OSError:
                    time.sleep(0.05)
            self.assertEqual(status, 404)
        finally:
            process.terminate()
            process.wait(timeout=10)


if __name__ == "__main__":
    unittest.main()
