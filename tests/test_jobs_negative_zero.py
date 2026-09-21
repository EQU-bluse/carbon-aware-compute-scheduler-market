from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market.jobs import register


def _job(job_id: str = "j-1", **overrides: object) -> dict[str, object]:
    job: dict[str, object] = {
        "job_id": job_id,
        "deadline": 100,
        "energy_wh": 250,
        "residency_regions": ["eu-north"],
    }
    job.update(overrides)
    return job


class NegativeZeroTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "registry.json")

    def test_negative_zero_literals_rejected(self) -> None:
        templates = [
            '{"version":1,"jobs":{"j":{"job_id":"j","deadline":%s,'
            '"energy_wh":1,"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{}}',
            '{"version":1,"jobs":{"j":{"job_id":"j","deadline":0,'
            '"energy_wh":%s,"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{}}',
        ]
        for template in templates:
            for literal in ("-0", "-0.0", "-0e2", "-0E+0"):
                with self.subTest(literal=literal):
                    Path(self.path).write_text(template % literal,
                                               encoding="utf-8")
                    with self.assertRaises(ValueError):
                        register(self.path, _job(job_id="other"), "k")

    def test_negative_zero_inside_strings_ignored(self) -> None:
        seed = {
            "version": 1,
            "jobs": {
                "j -0": {"job_id": "j -0", "deadline": 0, "energy_wh": 1,
                         "residency_regions": ["-0.0"], "state": "queued"},
            },
            "idempotency": {"-0e2": "j -0"},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        record, created = register(self.path, _job(job_id="b"), "k2")
        self.assertTrue(created)
        replay, replayed = register(
            self.path,
            {"job_id": "j -0", "deadline": 0, "energy_wh": 1,
             "residency_regions": ["-0.0"]}, "-0e2")
        self.assertFalse(replayed)
        self.assertEqual(replay["state"], "queued")

    def test_nonzero_negative_numbers_still_load_as_before(self) -> None:
        # deadline -1 is structurally invalid -> ValueError either way,
        # but a negative *nonzero* value must not trip the -0 check with a
        # different exception type.
        seed = {
            "version": 1,
            "jobs": {
                "j": {"job_id": "j", "deadline": -1, "energy_wh": 1,
                      "residency_regions": ["a"], "state": "queued"},
            },
            "idempotency": {},
        }
        Path(self.path).write_text(json.dumps(seed), encoding="utf-8")
        with self.assertRaises(ValueError):
            register(self.path, _job(job_id="b"), "k")

    def test_exponent_with_positive_mantissa_allowed(self) -> None:
        from carbon_market._json import loads as strict_loads

        self.assertEqual(strict_loads("1e-0"), 1.0)
        self.assertEqual(strict_loads("-1.5"), -1.5)
        self.assertEqual(strict_loads('{"a":"-0","b":"-0.0"}'),
                         {"a": "-0", "b": "-0.0"})
        for literal in ("-0", "-0.0", "-0e2"):
            with self.subTest(literal):
                with self.assertRaises(ValueError):
                    strict_loads(literal)


if __name__ == "__main__":
    unittest.main()
