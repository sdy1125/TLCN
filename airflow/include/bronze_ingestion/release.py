"""Immutable, fully verified Bronze release manifests."""

import hashlib
import io
import json
import re
from urllib.parse import urlparse


def _read_json(client, bucket, key):
    try:
        response = client.get_object(bucket, key)
    except Exception as exc:
        if getattr(exc, "code", None) in {"NoSuchKey", "NoSuchObject", "NotFound"} or isinstance(exc, KeyError):
            return None
        raise
    try:
        return json.load(response)
    finally:
        response.close()
        response.release_conn()


def _write_json(client, bucket, key, value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    client.put_object(bucket, key, io.BytesIO(data), len(data),
                      content_type="application/json")


def _fingerprint(sources):
    payload = json.dumps(sources, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def create_release(client, bucket, dataset_ids, release_id):
    """Pin one complete source set; refuse missing or corrupted immutable objects."""
    if not re.fullmatch(r"bronze-[a-z0-9][a-z0-9_-]{2,95}", release_id):
        raise ValueError("Invalid Bronze release ID")
    ids = sorted(dataset_ids)
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("Bronze release requires unique source IDs")
    sources = {}
    for dataset in ids:
        metadata = _read_json(
            client, bucket, f"_metadata/latest_success/{dataset}.json")
        if metadata is None or metadata.get("status") != "SUCCESS" or (
                metadata.get("dataset_id") != dataset):
            raise ValueError(f"No verified Bronze success: {dataset}")
        checksum = metadata.get("checksum_sha256")
        uri = urlparse(str(metadata.get("bronze_path") or ""))
        if (not isinstance(checksum, str) or
                not re.fullmatch(r"[0-9a-f]{64}", checksum) or
                uri.scheme != "s3" or uri.netloc != bucket or
                not uri.path.lstrip("/") or uri.query or uri.fragment):
            raise ValueError(f"Invalid Bronze source metadata: {dataset}")
        response = client.get_object(bucket, uri.path.lstrip("/"))
        digest = hashlib.sha256()
        size = 0
        try:
            while chunk := response.read(1024 * 1024):
                digest.update(chunk)
                size += len(chunk)
        finally:
            response.close()
            response.release_conn()
        if digest.hexdigest() != checksum or size != metadata.get("file_size_bytes"):
            raise ValueError(f"Bronze object integrity failure: {dataset}")
        sources[dataset] = metadata
    manifest = {"release_id": release_id, "source_count": len(ids),
                "sources": sources, "fingerprint": _fingerprint(sources)}
    key = f"_metadata/releases/{release_id}.json"
    existing = _read_json(client, bucket, key)
    if existing is not None and existing != manifest:
        raise ValueError("Bronze release ID already pins different source versions")
    if existing is None:
        _write_json(client, bucket, key, manifest)
    _write_json(client, bucket, "_metadata/latest_verified_release.json",
                {"release_id": release_id, "fingerprint": manifest["fingerprint"]})
    return manifest
