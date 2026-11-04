import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from bronze_ingestion.handoff import verify_handoff


class DriveHandoffTests(unittest.TestCase):
    def test_detects_partial_or_changed_local_release(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            file = root / "source.csv"
            file.write_bytes(b"A\n")
            sources = [{"relative_path": "source.csv"}]
            self.assertFalse(verify_handoff(root, sources))
            marker = root / ".drive-handoff.in-progress"
            marker.write_text("running")
            with self.assertRaisesRegex(ValueError, "still in progress"):
                verify_handoff(root, sources)
            marker.unlink()
            state = {"source.csv": {"sha256": hashlib.sha256(b"A\n").hexdigest()}}
            (root / ".drive-handoff.json").write_text(json.dumps(state))
            self.assertTrue(verify_handoff(root, sources))
            file.write_bytes(b"B\n")
            with self.assertRaisesRegex(ValueError, "source changed"):
                verify_handoff(root, sources)
            file.write_bytes(b"A\n")
            with self.assertRaisesRegex(ValueError, "complete Bronze manifest"):
                verify_handoff(root, sources + [{"relative_path": "other.csv"}])


if __name__ == "__main__":
    unittest.main()
