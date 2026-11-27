import json
import os


def append_jsonl(path: str, record: dict) -> None:
    """Append one record as a JSON line, creating its folder if it is missing."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps(record) + "\n")
