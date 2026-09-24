"""Unit tests for the hot-rotatable multi-token authorization config."""

from __future__ import annotations

import hashlib
import json
import os
import unittest
from tempfile import TemporaryDirectory

from carbon_market import auth


def digest(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def line(name: str = "svc-a", token: str = "tok-a",
         deadline: int | None = None, ops: object = "*",
         stages: object = "*", keys: object = "*") -> str:
    return json.dumps([name, digest(token), deadline, ops, stages, keys],
                      ensure_ascii=False)


class LoadConfigTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "auth.jsonl")

    def load(self, text: str) -> auth.Config:
        with open(self.path, "w", encoding="utf-8") as handle:
            handle.write(text)
        return auth.load_config(self.path)

    def load_bytes(self, raw: bytes) -> auth.Config:
        with open(self.path, "wb") as handle:
            handle.write(raw)
        return auth.load_config(self.path)

    def test_single_wildcard_record_loads(self) -> None:
        config = self.load(line() + "\n")
        self.assertEqual(len(config.records), 1)
        record = config.records[0]
        self.assertEqual(record.name, "svc-a")
        self.assertEqual(record.digest, digest("tok-a"))
        self.assertIsNone(record.deadline)
        self.assertEqual(record.ops, frozenset())
        self.assertEqual(record.stages, frozenset())
        self.assertEqual(record.keys, frozenset())

    def test_blank_lines_and_trailing_whitespace_are_ignored(self) -> None:
        text = "\n  \n" + line("a", "tok-a") + "\n\t\n" \
            + line("b", "tok-b") + "\n"
        config = self.load(text)
        self.assertEqual([r.name for r in config.records], ["a", "b"])

    def test_chinese_scope_values_round_trip_as_utf8(self) -> None:
        config = self.load(line(stages=["成功", "校验"]))
        self.assertEqual(config.records[0].stages,
                         frozenset({"成功", "校验"}))

    def test_utf8_token_digest_uses_original_bytes(self) -> None:
        token = "令牌α"
        config = self.load(line(token=token))
        self.assertEqual(config.records[0].digest,
                         hashlib.sha256(token.encode("utf-8")).hexdigest())

    def test_missing_file_is_config_error(self) -> None:
        with self.assertRaises(auth.AuthConfigError):
            auth.load_config(os.path.join(self.tmp.name, "missing"))

    def test_empty_and_whitespace_only_files_are_config_error(self) -> None:
        with self.assertRaises(auth.AuthConfigError):
            self.load("")
        with self.assertRaises(auth.AuthConfigError):
            self.load("  \n\t\n")

    def test_non_utf8_bytes_are_config_error(self) -> None:
        with self.assertRaises(auth.AuthConfigError):
            self.load_bytes(b"\xff\xfe" + line().encode("utf-8"))

    def test_invalid_json_line_is_config_error(self) -> None:
        with self.assertRaises(auth.AuthConfigError):
            self.load(line() + "\n{not json\n")

    def test_wrong_shape_is_config_error(self) -> None:
        cases = [
            json.dumps({}),  # not an array
            json.dumps([]),  # too few
            json.dumps(["a", digest("t"), None, "*", "*", "*", "x"]),  # 7
            json.dumps(["a", digest("t"), None, "*", "*"]),  # 5
        ]
        for text in cases:
            with self.subTest(text=text):
                with self.assertRaises(auth.AuthConfigError):
                    self.load(text + "\n")

    def test_bad_name_digest_and_deadline_are_config_error(self) -> None:
        bad = [
            ["", digest("t"), None, "*", "*", "*"],
            [1, digest("t"), None, "*", "*", "*"],
            ["a", "abc", None, "*", "*", "*"],
            ["a", digest("t").upper(), None, "*", "*", "*"],
            ["a", digest("t")[:-1] + "g", None, "*", "*", "*"],
            ["a", digest("t"), -1, "*", "*", "*"],
            ["a", digest("t"), True, "*", "*", "*"],
            ["a", digest("t"), 1.5, "*", "*", "*"],
            ["a", digest("t"), "10", "*", "*", "*"],
        ]
        for entry in bad:
            with self.subTest(entry=entry):
                with self.assertRaises(auth.AuthConfigError):
                    self.load(json.dumps(entry) + "\n")

    def test_zero_and_large_deadline_are_accepted(self) -> None:
        self.assertEqual(self.load(line(deadline=0)).records[0].deadline, 0)
        big = 10 ** 15
        self.assertEqual(
            self.load(line(deadline=big)).records[0].deadline, big)

    def test_bad_scopes_are_config_error(self) -> None:
        bad = [
            ["a", digest("t"), None, "**", "*", "*"],
            ["a", digest("t"), None, [], "*", "*"],
            ["a", digest("t"), None, ["copy", "copy"], "*", "*"],
            ["a", digest("t"), None, ["", "restore"], "*", "*"],
            ["a", digest("t"), None, [1], "*", "*"],
            ["a", digest("t"), None, ["delete"], "*", "*"],
            ["a", digest("t"), None, "*", ["done"], "*"],
            ["a", digest("t"), None, "*", "*", [""]],
            ["a", digest("t"), None, "*", "*", ["k", "k"]],
        ]
        for entry in bad:
            with self.subTest(entry=entry):
                with self.assertRaises(auth.AuthConfigError):
                    self.load(json.dumps(entry, ensure_ascii=False) + "\n")

    def test_duplicate_name_or_digest_is_config_error(self) -> None:
        text_a = line("svc-a", "tok-a")
        text_b = line("svc-b", "tok-b")
        with self.assertRaises(auth.AuthConfigError):
            self.load(text_a + "\n" + line("svc-a", "tok-b") + "\n")
        with self.assertRaises(auth.AuthConfigError):
            self.load(text_a + "\n" + line("svc-b", "tok-a") + "\n")
        # Distinct name and distinct digest is fine.
        config = self.load(text_a + "\n" + text_b + "\n")
        self.assertEqual(len(config.records), 2)


class AuthenticateTest(unittest.TestCase):
    def _config(self) -> auth.Config:
        entries = "\n".join([
            line("current", "alive", deadline=None),
            line("grace", "old", deadline=100),
        ]) + "\n"
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "auth.jsonl")
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(entries)
        return auth.load_config(path)

    def test_known_token_matches_its_record(self) -> None:
        config = self._config()
        record = auth.authenticate(config, "alive", now=50)
        self.assertIsNotNone(record)
        self.assertEqual(record.name, "current")

    def test_unknown_token_returns_none(self) -> None:
        self.assertIsNone(auth.authenticate(self._config(), "nope", now=50))

    def test_deadline_is_inclusive_but_expires_afterwards(self) -> None:
        config = self._config()
        self.assertEqual(auth.authenticate(config, "old", now=100).name,
                         "grace")
        self.assertIsNone(auth.authenticate(config, "old", now=101))

    def test_null_deadline_never_expires(self) -> None:
        config = self._config()
        self.assertIsNotNone(
            auth.authenticate(config, "alive", now=10 ** 12))


class ScopeTest(unittest.TestCase):
    def make(self, ops: object = "*", stages: object = "*",
             keys: object = "*") -> auth.Record:
        return auth._record_from(
            ["svc", digest("t"), None, ops, stages, keys], 1)

    def test_wildcard_allows_omission_and_any_value(self) -> None:
        record = self.make()
        self.assertTrue(auth.check_scope(record, None, None, None))
        self.assertTrue(auth.check_scope(record, "restore", "回滚", "k"))

    def test_array_scope_requires_explicit_filter_in_scope(self) -> None:
        record = self.make(ops=["copy"], stages=["成功"], keys=["hist-1"])
        self.assertTrue(
            auth.check_scope(record, "copy", "成功", "hist-1"))
        # Any omitted filter fails.
        self.assertFalse(auth.check_scope(record, None, "成功", "hist-1"))
        self.assertFalse(auth.check_scope(record, "copy", None, "hist-1"))
        self.assertFalse(auth.check_scope(record, "copy", "成功", None))
        # An in-range value outside the list fails.
        self.assertFalse(
            auth.check_scope(record, "restore", "成功", "hist-1"))
        self.assertFalse(
            auth.check_scope(record, "copy", "回滚", "hist-1"))
        self.assertFalse(
            auth.check_scope(record, "copy", "成功", "other"))

    def test_partial_scopes_only_constrain_their_filter(self) -> None:
        record = self.make(ops=["copy"])
        self.assertTrue(auth.check_scope(record, "copy", None, None))
        self.assertFalse(auth.check_scope(record, "restore", None, None))


if __name__ == "__main__":
    unittest.main()
