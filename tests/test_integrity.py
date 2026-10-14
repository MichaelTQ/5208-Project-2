import hashlib
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from common import RAW_COLUMNS, download, export_size
from fetch_data import ordered_columns


class IntegrityTests(unittest.TestCase):
    def test_existing_file_wrong_size_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "data.csv"
            path.write_bytes(b"abc")
            with self.assertRaisesRegex(RuntimeError, "size mismatch"):
                download("unused", path, expected_size=4)

    def test_existing_file_wrong_checksum_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "data.csv"
            path.write_bytes(b"abc")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                download("unused", path, expected_sha="0" * 64)

    def test_completed_partial_promoted_only_after_hash_verification(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "data.csv"
            part = path.with_name(path.name + ".part")
            part.write_bytes(b"abc")
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                download("unused", path, expected_size=3, expected_sha="0" * 64)
            self.assertFalse(path.exists())
            expected = hashlib.sha256(b"abc").hexdigest()
            result = download("unused", path, expected_size=3, expected_sha=expected)
            self.assertEqual(result["sha256"], expected)
            self.assertFalse(part.exists())

    def test_range_total_used_instead_of_one_byte_response_length(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "headers"
            def request(*args, **kwargs):
                path.write_text("HTTP/1.1 206 Partial Content\nContent-Length: 1\nContent-Range: bytes 0-0/881206891\n")
                return type("Result", (), {"returncode": 0, "stderr": ""})()
            with patch("common.subprocess.run", side_effect=request):
                self.assertEqual(export_size("https://example.test", path), 881206891)

    def test_metadata_order_is_explicit(self):
        metadata = {"columnMetadata": {"order": [str(i) for i in range(12)], "map": {str(i): value for i, value in reversed(list(enumerate(RAW_COLUMNS)))}}}
        self.assertEqual(ordered_columns(metadata), RAW_COLUMNS)


if __name__ == "__main__":
    unittest.main()
