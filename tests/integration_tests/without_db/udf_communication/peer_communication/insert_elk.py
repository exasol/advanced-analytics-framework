import json
import time
from pathlib import Path

from elasticsearch import Elasticsearch


def insert(log_file_path: Path, es: Elasticsearch):
    with open(log_file_path) as f:
        index_name = str(log_file_path).replace("/", "_").replace(".", "_")
        index_name = index_name + "_" + str(time.monotonic_ns())
        index_name = index_name[1:].lower()
        print("index_name: " + index_name)
        for line in iter(f.readline, ""):
            json_line = json.loads(line)
            es.index(index=index_name, document=json_line)


if __name__ == "__main__":
    root = Path(__file__).parent
    log_file_path = root / "test_add_peer_forward.log"
    es = Elasticsearch("http://localhost:9200", basic_auth=("elastic", "changeme"))
    insert(log_file_path, es)
