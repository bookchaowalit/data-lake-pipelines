"""Portable public snapshot contract; no consumer repository imports or I/O."""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

SCHEMA = 'solo-empire.platform-integrations.v1'
MAX_SNAPSHOT_BYTES = 4 * 1024 * 1024
IDENTIFIER = re.compile(r'[a-z0-9]+(?:-[a-z0-9]+)*\Z')
ENTRY = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z')
CHECKSUM = re.compile(r'[a-f0-9]{64}\Z')


def fields(value, names):
    if not isinstance(value, dict) or set(value) != set(names.split()):
        raise ValueError('invalid_fields')


def identifier(value):
    if not isinstance(value, str) or len(value) > 80 or not IDENTIFIER.fullmatch(value):
        raise ValueError('invalid_identifier')


def timestamp(value, now=None):
    if not isinstance(value, str) or len(value) > 40 or not value.endswith(('Z', '+00:00')):
        raise ValueError('invalid_timestamp')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.utcoffset() != timedelta(0) or parsed > (now or datetime.now(timezone.utc)) + timedelta(minutes=5):
        raise ValueError('invalid_timestamp')
    return parsed


def https_url(value):
    if not isinstance(value, str) or len(value) > 2048 or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise ValueError('invalid_url')
    value.encode('utf-8', errors='strict')
    parts = urlsplit(value)
    if parts.scheme != 'https' or not parts.hostname or parts.username is not None or parts.password is not None or '?' in value or '#' in value or '\\' in value:
        raise ValueError('invalid_url')
    if parts.port is not None and not 1 <= parts.port <= 65535:
        raise ValueError('invalid_url')


def platform(value):
    fields(value, 'id name mcp_transports docs_url')
    identifier(value['id'])
    name = value['name']
    if not isinstance(name, str) or not 1 <= len(name) <= 80 or any(ord(c) < 32 for c in name):
        raise ValueError('invalid_platform')
    name.encode('utf-8', errors='strict')
    transports = value['mcp_transports']
    if not isinstance(transports, list) or any(t not in ('stdio', 'sse', 'streamable_http') for t in transports) or len(set(transports)) != len(transports):
        raise ValueError('invalid_transports')
    https_url(value['docs_url'])


def manifest(value, source=False):
    fields(value, ('source_id' if source else 'dataset source_manifests') + ' manifest_key manifest_sha256 raw_sha256 bronze_sha256')
    if source:
        identifier(value['source_id'])
    elif value['dataset'] != 'platform_integrations':
        raise ValueError('invalid_dataset')
    key = value['manifest_key']
    if not isinstance(key, str) or len(key) > 512 or not key.startswith('control/manifests/') or not re.fullmatch(r'[A-Za-z0-9_=./-]+', key) or any(p in ('', '.', '..') for p in key.split('/')):
        raise ValueError('invalid_manifest_key')
    for name in ('manifest_sha256', 'raw_sha256', 'bronze_sha256'):
        if not isinstance(value[name], str) or not CHECKSUM.fullmatch(value[name]):
            raise ValueError('invalid_checksum')


def validate_snapshot(data, now=None):
    fields(data, 'schema_version captured_at platforms capabilities lineage')
    if data['schema_version'] != SCHEMA:
        raise ValueError('invalid_schema')
    captured = timestamp(data['captured_at'], now)
    if not isinstance(data['platforms'], list) or not 1 <= len(data['platforms']) <= 50:
        raise ValueError('invalid_platforms')
    platforms = set()
    for value in data['platforms']:
        platform(value)
        if value['id'] in platforms:
            raise ValueError('duplicate_platform')
        platforms.add(value['id'])
    if not isinstance(data['capabilities'], list) or len(data['capabilities']) > 20000:
        raise ValueError('invalid_capabilities')
    seen = set()
    for value in data['capabilities']:
        fields(value, 'platform_id app_id channel availability entry_id endpoint_url evidence_url captured_at')
        identifier(value['platform_id'])
        identifier(value['app_id'])
        if value['platform_id'] not in platforms or value['channel'] not in ('plugin', 'connector', 'manual_mcp') or value['availability'] not in ('available', 'unavailable', 'unknown'):
            raise ValueError('invalid_capability')
        entry = value['entry_id']
        if entry is not None and (not isinstance(entry, str) or not ENTRY.fullmatch(entry)):
            raise ValueError('invalid_entry')
        grain = (value['platform_id'], value['app_id'], value['channel'], entry)
        if grain in seen:
            raise ValueError('duplicate_capability')
        seen.add(grain)
        if value['endpoint_url'] is not None:
            https_url(value['endpoint_url'])
        https_url(value['evidence_url'])
        if timestamp(value['captured_at'], now) > captured + timedelta(minutes=5):
            raise ValueError('invalid_capability_capture')
    manifest(data['lineage'])
    sources = data['lineage']['source_manifests']
    if not isinstance(sources, list) or not 1 <= len(sources) <= 100:
        raise ValueError('invalid_sources')
    seen = set()
    for value in sources:
        manifest(value, True)
        if value['source_id'] in seen:
            raise ValueError('duplicate_source')
        seen.add(value['source_id'])
    return data


def validate_snapshot_bytes(raw, now=None):
    if not isinstance(raw, bytes) or len(raw) > MAX_SNAPSHOT_BYTES:
        raise ValueError('snapshot_too_large')
    def unique(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('duplicate_field')
            result[key] = value
        return result
    def constant(value):
        raise ValueError('invalid_json_constant')
    return validate_snapshot(json.loads(raw, object_pairs_hook=unique, parse_constant=constant), now)
