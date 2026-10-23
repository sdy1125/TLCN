"""Register orphaned Silver Iceberg tables from the layered warehouse layout.

The warehouse object copy must be verified before --apply. No data files are
deleted or changed; this only recreates catalog pointers to existing metadata.
"""

import argparse
import os
import re
import requests
from minio import Minio

NAMESPACES = ("silver_meta", "silver_ref", "silver_stage", "silver_core")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    store = Minio(
        "minio:9000",
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        secure=False,
    )
    endpoint = "http://iceberg-rest:8181/v1"
    r = requests.get(f"{endpoint}/namespaces", timeout=20)
    r.raise_for_status()
    namespaces = r.json().get("namespaces", [])
    tables = {}
    for obj in store.list_objects("warehouse", recursive=True):
        match = re.fullmatch(
            r"silver/([^/]+)/([^/]+)/metadata/(\d+)-[^/]+\.metadata\.json", obj.object_name
        )
        if not match:
            continue
        namespace, table, version = match.group(1), match.group(2), int(match.group(3))
        if namespace not in NAMESPACES:
            continue
        candidate = (version, obj.object_name)
        key = (namespace, table)
        if key not in tables or candidate > tables[key]:
            tables[key] = candidate
    for (namespace, table), (_, path) in sorted(tables.items()):
        if [namespace] not in namespaces and args.apply:
            created = requests.post(
                f"{endpoint}/namespaces",
                json={"namespace": [namespace], "properties": {
                    "location": f"s3://warehouse/silver/{namespace}"
                }}, timeout=20
            )
            created.raise_for_status()
            namespaces.append([namespace])
        metadata_location = f"s3://warehouse/{path}"
        print(f"{namespace}.{table}: {metadata_location}", flush=True)
        if not args.apply:
            continue
        existing = requests.get(f"{endpoint}/namespaces/{namespace}/tables/{table}", timeout=20)
        if existing.status_code == 200:
            print("  already registered", flush=True)
            continue
        if existing.status_code != 404:
            existing.raise_for_status()
        registered = requests.post(
            f"{endpoint}/namespaces/{namespace}/register",
            json={"name": table, "metadata-location": metadata_location},
            timeout=30,
        )
        registered.raise_for_status()
        print("  registered", flush=True)
    print(f"tables_found={len(tables)}", flush=True)


if __name__ == "__main__":
    main()
