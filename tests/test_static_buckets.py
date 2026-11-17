"""Taules estàtiques: `pk = md5(id) % buckets`, amb `buckets` al missatge i fix per a cada taula."""
import hashlib
import importlib.util
import os
import sys
import unittest
from collections import namedtuple
from unittest.mock import MagicMock

sys.modules.setdefault("happybase", MagicMock())
sys.modules.setdefault("load_dotenv", MagicMock())
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, HERE)
spec = importlib.util.spec_from_file_location("raw_store_main", os.path.join(HERE, "__main__.py"))
raw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(raw)

Row = namedtuple("Row", "comment")


def md5(i):
    return int(hashlib.md5(i.encode()).hexdigest(), 16)


class TransformTests(unittest.TestCase):
    def test_default_is_the_old_hundred(self):
        out = raw.transform_data_static([{"id": "a", "x": 1}], "id")
        self.assertEqual(out, [{"pk": md5("a") % 100, "id": "a", "x": "1"}])

    def test_buckets_come_from_the_message(self):
        out = raw.transform_data_static([{"id": f"id{i}"} for i in range(200)], "id", 4096)
        self.assertTrue(all(o["pk"] == md5(o["id"]) % 4096 for o in out))
        self.assertGreater(max(o["pk"] for o in out), 100)            # ja no es queda a 0..99

    def test_options_comment_only_for_non_default_buckets(self):
        self.assertNotIn("comment", raw.get_static_options())
        self.assertNotIn("comment", raw.get_static_options(100))
        self.assertEqual(raw.get_static_options(4096)["comment"], "buckets=4096")

    def test_valid_buckets(self):
        self.assertTrue(raw.valid_buckets(64))
        for bad in (0, -1, "64", 1.5, None, True):
            self.assertFalse(raw.valid_buckets(bad), bad)


class MessageBucketsTests(unittest.TestCase):
    def test_a_message_without_buckets_is_a_hundred(self):
        self.assertEqual(raw.message_buckets({"tables": ["t"], "data": []}), 100)

    def test_a_message_with_buckets_uses_them(self):
        self.assertEqual(raw.message_buckets({"buckets": 4096}), 4096)

    def test_an_invalid_value_is_refused_even_if_it_looks_like_a_number(self):
        for bad in (0, -4, "100", None, 2.0, True):
            with self.assertRaises(ValueError):
                raw.message_buckets({"buckets": bad})

    def test_a_message_without_buckets_is_saved_in_a_table_without_comment_or_with_a_hundred(self):
        for comment in (None, "", "buckets=100"):
            session = MagicMock()
            session.execute.return_value.one.return_value = Row(comment)
            self.assertIsNone(raw.check_static_buckets(session, "manttest.t", raw.message_buckets({}), set()), comment)

    def test_a_message_without_buckets_is_refused_in_a_table_of_another_size(self):
        session = MagicMock()
        session.execute.return_value.one.return_value = Row("buckets=4096")
        self.assertIsNotNone(raw.check_static_buckets(session, "manttest.t", raw.message_buckets({}), set()))


class UnreadableSchemaTests(unittest.TestCase):
    """Si no es pot llegir `system_schema`, els missatges antics es desen com sempre."""

    def broken(self):
        session = MagicMock()
        session.execute.side_effect = RuntimeError("Unauthorized")
        return session

    def test_an_old_message_is_saved_anyway_and_warned_once(self):
        session, verified = self.broken(), set()
        with self.assertLogs(raw.logger, level="WARNING") as logs:
            for _ in range(3):
                self.assertIsNone(raw.static_buckets_problem(session, "manttest.static_fullorders_", {"tables": ["t"]},
                                                             100, verified))
        self.assertEqual(len(logs.records), 1)
        self.assertEqual(session.execute.call_count, 1)

    def test_a_message_with_buckets_fails_if_the_table_cannot_be_checked(self):
        with self.assertRaises(RuntimeError):
            raw.static_buckets_problem(self.broken(), "manttest.api_orders_full", {"buckets": 4096}, 4096, set())

    def test_a_working_schema_still_refuses_a_mismatch(self):
        session = MagicMock()
        session.execute.return_value.one.return_value = Row("buckets=4096")
        self.assertIsNotNone(raw.static_buckets_problem(session, "manttest.t", {}, 100, set()))


class CheckBucketsTests(unittest.TestCase):
    def session(self, row):
        session = MagicMock()
        session.execute.return_value.one.return_value = row
        return session

    def test_new_table_is_fine(self):
        verified = set()
        self.assertIsNone(raw.check_static_buckets(self.session(None), "manttest.api_orders_full", 4096, verified))
        self.assertIn("manttest.api_orders_full", verified)

    def test_same_buckets_is_fine_and_is_asked_once(self):
        session, verified = self.session(Row("buckets=4096")), set()
        for _ in range(3):
            self.assertIsNone(raw.check_static_buckets(session, "manttest.api_orders_full", 4096, verified))
        self.assertEqual(session.execute.call_count, 1)

    def test_other_buckets_is_refused_and_not_remembered(self):
        session, verified = self.session(Row("buckets=4096")), set()
        self.assertIn("4096", raw.check_static_buckets(session, "manttest.api_orders_full", 8192, verified))
        self.assertEqual(verified, set())

    def test_old_table_without_comment_is_a_hundred(self):
        verified = set()
        self.assertIsNone(raw.check_static_buckets(self.session(Row("")), "manttest.static_fullorders_", 100, verified))
        self.assertIn("100", raw.check_static_buckets(self.session(Row(None)), "manttest.static_fullorders_", 256, set()))

    def test_splits_keyspace_and_table(self):
        session = self.session(None)
        raw.check_static_buckets(session, "manttest.api_orders_full", 64, set())
        self.assertEqual(session.execute.call_args.args[1], ("manttest", "api_orders_full"))


if __name__ == "__main__":
    unittest.main()
