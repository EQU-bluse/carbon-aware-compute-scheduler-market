from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from carbon_market._jsonio import strict_loads
from carbon_market.jobs import register as register_job
from carbon_market.offers import register as register_offer


class StrictLoadsTest(unittest.TestCase):
    def test_negative_zero_int_literals_rejected(self) -> None:
        for text in ["-0", "[-0]", '{"a":-0}', "-000"]:
            with self.subTest(text):
                with self.assertRaises(ValueError):
                    strict_loads(text)

    def test_negative_zero_float_literals_rejected(self) -> None:
        for text in ["-0.0", "-0.000", "-0e2", "-0E2", "-0.0e0",
                     "[1,-0.0,2]", '{"a":-0e2}']:
            with self.subTest(text):
                with self.assertRaises(ValueError):
                    strict_loads(text)

    def test_negative_zero_inside_strings_allowed(self) -> None:
        self.assertEqual(strict_loads('"-0"'), "-0")
        self.assertEqual(strict_loads('["-0.0","-0e2"]'),
                         ["-0.0", "-0e2"])
        self.assertEqual(strict_loads('{"a":"-0"}'), {"a": "-0"})

    def test_other_numbers_unaffected(self) -> None:
        self.assertEqual(strict_loads("0"), 0)
        self.assertEqual(strict_loads("-1"), -1)
        self.assertEqual(strict_loads("-0.5"), -0.5)
        self.assertEqual(strict_loads("0.0"), 0.0)
        self.assertEqual(strict_loads("1e2"), 100.0)
        self.assertEqual(strict_loads("-1e2"), -100.0)

    def test_ordinary_json_still_parses(self) -> None:
        text = '{"a":[1,2.5],"b":"x","c":null,"d":true}'
        self.assertEqual(strict_loads(text), json.loads(text))


class RegistryIntegrationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def _write(self, name: str, text: str) -> str:
        path = Path(self.tmp.name) / name
        path.write_text(text, encoding="utf-8")
        return str(path)

    def test_jobs_register_rejects_negative_zero_file(self) -> None:
        text = (
            '{"version":1,'
            '"jobs":{"j":{"job_id":"j","deadline":-0,"energy_wh":2,'
            '"residency_regions":["a"],"state":"queued"}},'
            '"idempotency":{}}'
        )
        path = self._write("jobs.json", text)
        job = {"job_id": "x", "deadline": 1, "energy_wh": 2,
               "residency_regions": ["a"]}
        with self.assertRaises(ValueError):
            register_job(path, job, "k")

    def test_offers_register_rejects_negative_zero_file(self) -> None:
        text = (
            '{"version":1,'
            '"offers":{"r":{"resource_id":"r","region":"x","capacity_wh":1,'
            '"unit_cost":-0.0,"carbon_intensity":2}},'
            '"idempotency":{}}'
        )
        path = self._write("offers.json", text)
        offer = {"resource_id": "x", "region": "y", "capacity_wh": 1,
                 "unit_cost": 1, "carbon_intensity": 2}
        with self.assertRaises(ValueError):
            register_offer(path, offer, "k")

    def test_negative_zero_in_string_field_accepted(self) -> None:
        text = (
            '{"version":1,'
            '"offers":{"r":{"resource_id":"r","region":"-0","capacity_wh":1,'
            '"unit_cost":0,"carbon_intensity":0}},'
            '"idempotency":{"k":{"resource_id":"r"}}}'
        )
        path = self._write("offers2.json", text)
        replay, created = register_offer(
            path,
            {"resource_id": "r", "region": "-0", "capacity_wh": 1,
             "unit_cost": 0, "carbon_intensity": 0}, "k")
        self.assertFalse(created)
        self.assertEqual(replay["region"], "-0")


if __name__ == "__main__":
    unittest.main()
