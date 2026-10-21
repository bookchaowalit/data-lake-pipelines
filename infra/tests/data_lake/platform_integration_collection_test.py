"""Offline public collection tests with real immutable lake artifacts."""
import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'infra/scripts'))
from data_lake import collect_platform_integrations as collector
from data_lake.storage import ObjectStore

STAMP = '2026-10-13T00:00:00Z'


class CollectionTests(unittest.TestCase):
    def setUp(self):
        self.config = collector.load_config(ROOT / 'systems/ai-data/platform-integration-sources.json')
        self.raws = {}
        for s in self.config['sources']:
            payload = {'mcpServers': {next(iter(s['aliases'])): {'url':'https://example.com/mcp','type':'http'}}} if s['parser'] == 'mcp_config' else {'plugins': [{'name': 'github'}, {'name': 'unmatched-github'}]}
            self.raws[s['id']] = json.dumps(payload).encode()

    def test_partial_exact_alias_and_desktop_restriction(self):
        spec = self.config['sources'][0]
        rows = collector.parse_capture(spec, self.raws[spec['id']], STAMP)
        self.assertEqual({r['app_id'] for r in rows}, {'github'})
        self.assertEqual({r['platform_id'] for r in rows}, {'codex-app', 'codex-cli'})
        raw = json.dumps({'plugins': [{'name': 'github', 'platforms': ['desktop']}]}).encode()
        rows = collector.parse_capture(spec, raw, STAMP)
        self.assertEqual([(r['platform_id'], r['availability']) for r in rows], [('codex-app', 'available'), ('codex-cli', 'unknown')])

    def test_duplicate_and_conflicting_json_fail_closed(self):
        spec = self.config['sources'][0]
        for raw in [b'{"plugins":[],"plugins":[]}', b'{"plugins":[{"name":"github"},{"name":"github"}]}', b'{"plugins":[{"name":42}]}']:
            with self.assertRaises(collector.CollectionError):
                collector.parse_capture(spec, raw, STAMP)

    def test_manual_reference_projects_only_explicit_remote(self):
        spec = copy.deepcopy(self.config['sources'][0])
        spec.update(parser='mcp_config', platforms=['claude-code'], aliases={'github': ['github']})
        raw = b'{"mcpServers":{"github":{"url":"https://api.githubcopilot.com/mcp/","type":"http"},"ignored":{"command":"do-not-run","args":[]}}}'
        rows = collector.parse_capture(spec, raw, STAMP)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['channel'], 'manual_mcp')
        self.assertEqual(rows[0]['endpoint_url'], 'https://api.githubcopilot.com/mcp/')
        self.assertNotIn('command', json.dumps(rows))
        direct = b'{"github":{"url":"https://api.githubcopilot.com/mcp/","type":"http"}}'
        self.assertEqual(collector.parse_capture(spec,direct,STAMP), rows)
        placeholder = b'{"github":{"url":"https://api.githubcopilot.com/mcp/","type":"http","headers":{"Authorization":"Bearer ${PUBLIC_VARIABLE}"}}}'
        self.assertEqual(collector.parse_capture(spec,placeholder,STAMP), rows)

    def test_official_claude_entry_metadata_is_not_projected(self):
        spec = self.config['sources'][1]
        raw = b'{"plugins":[{"name":"github","displayName":"GitHub","keywords":["developer"],"lspServers":{},"skills":[],"strict":true}]}'
        rows = collector.parse_capture(spec,raw,STAMP)
        self.assertEqual(rows[0]['app_id'],'github')
        self.assertNotIn('skills',rows[0])

    def test_replay_preserves_bytes_age_identity_and_public_shape(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(collector, 'fetch_public', side_effect=AssertionError('network')):
            output = Path(tmp) / 'snapshot.json'
            first = collector.publish(self.config, self.raws, STAMP, str(Path(tmp)/'lake'), output)
            second = collector.publish(self.config, self.raws, STAMP, str(Path(tmp)/'lake'), output)
            self.assertEqual(first, second)
            self.assertEqual(first['captured_at'], STAMP)
            self.assertEqual(output.stat().st_mode & 0o777, 0o644)
            self.assertEqual(set(first), {'schema_version','captured_at','platforms','capabilities','lineage'})
            store = ObjectStore(str(Path(tmp)/'lake'))
            for source in first['lineage']['source_manifests']:
                manifest = json.loads(store.get_bytes(source['manifest_key']))
                self.assertEqual(store.get_bytes(manifest['raw']['key']), self.raws[source['source_id']])
                self.assertEqual(hashlib.sha256(store.get_bytes(manifest['bronze']['key'])).hexdigest(), source['bronze_sha256'])
                import pyarrow.parquet as pq
                rows = pq.read_table(store.object_uri(manifest['bronze']['key'])).to_pylist()
                self.assertTrue(rows)
            text = output.read_text()
            for forbidden in ['authentication','installation','health','headers','env']:
                self.assertNotIn('"'+forbidden+'"', text)

    def test_corruption_and_failure_keep_previous_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            lake = str(Path(tmp)/'lake')
            first = collector.publish(self.config, self.raws, STAMP, lake, output)
            before = output.read_bytes()
            manifest = json.loads(ObjectStore(lake).get_bytes(first['lineage']['source_manifests'][0]['manifest_key']))
            (Path(lake) / manifest['bronze']['key']).write_bytes(b'corrupt')
            with self.assertRaises(collector.CollectionError):
                collector.publish(self.config, self.raws, STAMP, lake, output)
            self.assertEqual(output.read_bytes(), before)
            with patch.object(collector, 'ingest_payload', side_effect=RuntimeError('private details')):
                with self.assertRaisesRegex(collector.CollectionError, '^collection_failed$'):
                    collector.publish(self.config, self.raws, STAMP, lake, output)
            self.assertEqual(output.read_bytes(), before)

    def test_capture_timestamp_and_urls_are_strict(self):
        for stamp in ['2026-10-13', '2026-10-13T00:00:00+07:00', 'bad']:
            with self.assertRaises(collector.CollectionError): collector.capture_timestamp(stamp)
        for url in ['http://example.com', 'https://user:pass@example.com/', 'https://127.0.0.1/', 'https://example.com/?token=secret', 'https://example.com/#x']:
            with self.assertRaises(collector.CollectionError): collector.public_url(url)

    def test_missing_and_malformed_capture_keep_previous_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            output.write_bytes(b'previous')
            for captures in [{}, {**self.raws, 'codex-plugins':b'{broken'}]:
                with self.assertRaises(collector.CollectionError): collector.publish(self.config,captures,STAMP,str(Path(tmp)/'lake'),output)
                self.assertEqual(output.read_bytes(), b'previous')
                self.assertFalse((Path(tmp)/'lake').exists())

    def test_source_config_and_private_manual_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'config.json'
            bad = copy.deepcopy(self.config)
            bad['sources'][0]['url'] = 'https://example.com/catalog.json'
            path.write_text(json.dumps(bad))
            with self.assertRaises(collector.CollectionError): collector.load_config(path)
        spec = next(s for s in self.config['sources'] if s['parser'] == 'mcp_config')
        with self.assertRaises(collector.CollectionError): collector.parse_capture(spec,b'{"mcpServers":{"github":{"url":"https://example.com/mcp","headers":{"Authorization":"denied"}}}}',STAMP)

    def test_distinct_capture_timestamp_does_not_reuse_old_observation(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            lake = str(Path(tmp)/'lake')
            one = collector.publish(self.config,self.raws,STAMP,lake,output)
            two = collector.publish(self.config,self.raws,'2026-10-12T00:00:00Z',lake,output)
            self.assertNotEqual(one['lineage']['manifest_key'],two['lineage']['manifest_key'])
            self.assertEqual(two['capabilities'][0]['captured_at'],'2026-10-12T00:00:00Z')

    def test_fetch_bounds_redirect_deadline_and_size(self):
        from urllib.error import HTTPError
        url = self.config['sources'][0]['url']
        class Response:
            headers = {}
            status = 200
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, size): return b'x' * size
        class Opener:
            def open(self, *args, **kwargs): return Response()
        with patch.object(collector, 'build_opener', return_value=Opener()):
            with self.assertRaisesRegex(collector.CollectionError, 'source_too_large'): collector.fetch_public(url)
        with patch.object(collector, 'build_opener', return_value=Opener()), patch.object(collector.time, 'monotonic', side_effect=[0, 20]):
            with self.assertRaisesRegex(collector.CollectionError, 'source_deadline'): collector.fetch_public(url)
        self.assertIsNone(collector.NoRedirect().redirect_request(None, None, 302, '', {}, url))
        with patch.object(collector, 'build_opener') as build:
            build.return_value.open.side_effect = HTTPError(url,302,'redirect',{},None)
            with self.assertRaisesRegex(collector.CollectionError, '^source_fetch_failed$'): collector.fetch_public(url)

    def test_header_wait_cannot_exceed_total_request_budget(self):
        import threading
        release = threading.Event()
        class Opener:
            def open(self,*args,**kwargs):
                release.wait(1)
                raise OSError('blocked headers')
        try:
            with patch.object(collector,'build_opener',return_value=Opener()), patch.object(collector,'DEADLINE_SECONDS',0.02):
                with self.assertRaisesRegex(collector.CollectionError,'^source_deadline$'):
                    collector.fetch_public(self.config['sources'][0]['url'])
        finally: release.set()

    def test_f1_invalid_urls_and_config_preserve_previous_publication(self):
        invalid_urls = ['https://example.com/mcp?', 'https://example.com/mcp#', 'https://@example.com/mcp', 'https://example.com/\\mcp', 'https://example.com/\x7f', 'https://example.com/\ud800']
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            output.write_bytes(b'previous')
            for url in invalid_urls:
                with self.subTest(url=repr(url)):
                    output.write_bytes(b'previous')
                    captures = {**self.raws, 'github-mcp':json.dumps({'github':{'type':'http','url':url}}).encode()}
                    with self.assertRaises(collector.CollectionError): collector.publish(self.config,captures,STAMP,str(Path(tmp)/'lake'),output)
                    self.assertEqual(output.read_bytes(),b'previous')
            for change in ['name', 'app_id', 'transports', 'private_field']:
                config = copy.deepcopy(self.config)
                if change == 'name': config['platforms'][0]['name'] = ''
                if change == 'app_id': config['sources'][0]['aliases']['github'] = ['g'*81]
                if change == 'transports': config['platforms'][0]['mcp_transports'] = ['stdio','stdio']
                if change == 'private_field': config['sources'][0]['credential'] = 'denied'
                with self.subTest(change=change):
                    with self.assertRaises(collector.CollectionError): collector.publish(config,self.raws,STAMP,str(Path(tmp)/'lake'),output)
                    self.assertEqual(output.read_bytes(),b'previous')

    def test_f1_oversized_serialized_snapshot_is_not_published(self):
        config = copy.deepcopy(self.config)
        config['sources'] = config['sources'][:1]
        captures = {'codex-plugins': self.raws['codex-plugins']}
        row = collector.parse_capture(config['sources'][0], captures['codex-plugins'], STAMP)[0]
        rows = [{**row,'entry_id':str(i),'evidence_url':'https://example.com/'+('x'*2000)} for i in range(2200)]
        with tempfile.TemporaryDirectory() as tmp, patch.object(collector,'parse_capture',return_value=rows):
            output = Path(tmp)/'snapshot.json'
            output.write_bytes(b'previous')
            with self.assertRaises(collector.CollectionError): collector.publish(config,captures,STAMP,str(Path(tmp)/'lake'),output)
            self.assertEqual(output.read_bytes(),b'previous')
            self.assertFalse((Path(tmp)/'lake').exists())

    def test_f2_mapping_change_has_new_batch_and_reconstructable_bronze(self):
        import pyarrow.parquet as pq
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            lake = str(Path(tmp)/'lake')
            first = collector.publish(self.config,self.raws,STAMP,lake,output)
            changed = copy.deepcopy(self.config)
            changed['sources'][0]['aliases']['github'] = ['notion']
            second = collector.publish(changed,self.raws,STAMP,lake,output)
            old_source = first['lineage']['source_manifests'][0]
            source = second['lineage']['source_manifests'][0]
            self.assertNotEqual(old_source['manifest_key'],source['manifest_key'])
            store = ObjectStore(lake)
            manifest = json.loads(store.get_bytes(source['manifest_key']))
            records = [json.loads(r['payload_json']) for r in pq.read_table(store.object_uri(manifest['bronze']['key'])).to_pylist()]
            self.assertEqual({r['app_id'] for r in records},{'notion'})
            normalized_manifest = json.loads(store.get_bytes(second['lineage']['manifest_key']))
            normalized = json.loads(store.get_bytes(normalized_manifest['raw']['key']))
            self.assertEqual(normalized['normalization']['config'],changed)
            self.assertIn('transform_sha256',manifest['metadata'])
            self.assertEqual(collector.publish(changed,self.raws,STAMP,lake,output),second)

    def test_f2_wrong_bronze_semantics_block_publication(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            lake = str(Path(tmp)/'lake')
            first = collector.publish(self.config,self.raws,STAMP,lake,output)
            before = output.read_bytes()
            changed = copy.deepcopy(self.config)
            changed['sources'][0]['aliases']['github'] = ['notion']
            original = collector.ingest_payload
            def wrong_source(*args,**kwargs):
                if kwargs['source'] == 'codex-plugins': return first['lineage']['source_manifests'][0]
                return original(*args,**kwargs)
            with patch.object(collector,'ingest_payload',side_effect=wrong_source):
                with self.assertRaises(collector.CollectionError) as error: collector.publish(changed,self.raws,STAMP,lake,output)
            self.assertEqual(str(error.exception.__cause__),'bronze_semantic_mismatch')
            self.assertEqual(output.read_bytes(),before)

    def test_manual_transport_must_be_advertised_by_host(self):
        config = copy.deepcopy(self.config)
        config['sources'] = [s for s in config['sources'] if s['id'] == 'github-mcp']
        captures = {'github-mcp':b'{"github":{"url":"https://example.com/mcp","type":"sse"}}'}
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = collector.publish(config,captures,STAMP,str(Path(tmp)/'lake'),Path(tmp)/'snapshot.json')
            rows = {r['platform_id']:r['availability'] for r in snapshot['capabilities']}
            self.assertEqual(rows['codex-cli'],'unknown')
            self.assertEqual(rows['claude-code'],'available')

    def test_full_serialized_public_contract_rejects_invalid_shapes_and_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = collector.publish(self.config,self.raws,STAMP,str(Path(tmp)/'lake'),Path(tmp)/'snapshot.json')
        self.assertEqual(collector.validate_snapshot_bytes(json.dumps(snapshot).encode()),snapshot)
        def invalid(change):
            value = copy.deepcopy(snapshot)
            change(value)
            with self.assertRaises(ValueError): collector.validate_snapshot_bytes(json.dumps(value).encode())
        changes = [
            lambda s: s.update(private='denied'),
            lambda s: s.update(platforms=s['platforms']*11),
            lambda s: s['platforms'][0].update(name='x'*81),
            lambda s: s['platforms'][0].update(id='x'*81),
            lambda s: s['platforms'][0].update(mcp_transports=['stdio','stdio']),
            lambda s: s.update(capabilities=[s['capabilities'][0]]*20001),
            lambda s: s['capabilities'][0].update(app_id='bad--id'),
            lambda s: s['capabilities'][0].update(entry_id='x'*129),
            lambda s: s['capabilities'][0].update(channel='owner'),
            lambda s: s['capabilities'][0].update(availability='connected'),
            lambda s: s['capabilities'][0].update(captured_at='2026-10-13T00:06:00Z'),
            lambda s: s['capabilities'][0].update(health='verified'),
            lambda s: s['lineage'].update(manifest_sha256='A'*64),
            lambda s: s['lineage'].update(manifest_key='control/manifests/../unsafe.json'),
            lambda s: s['lineage'].update(manifest_key='control/manifests/'+('x'*512)),
            lambda s: s['lineage'].update(source_manifests=s['lineage']['source_manifests']*21),
            lambda s: s['lineage']['source_manifests'][0].update(source_id='x'*81),
        ]
        for i,change in enumerate(changes):
            with self.subTest(case=i): invalid(change)

    def test_f2_normalization_revision_change_has_distinct_source_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp)/'snapshot.json'
            lake = str(Path(tmp)/'lake')
            first = collector.publish(self.config,self.raws,STAMP,lake,output)
            with patch.object(collector,'NORMALIZATION_REVISION','test.normalize.v3'):
                second = collector.publish(self.config,self.raws,STAMP,lake,output)
            self.assertNotEqual(first['lineage']['source_manifests'][0]['manifest_key'],second['lineage']['source_manifests'][0]['manifest_key'])
            self.assertEqual(first['capabilities'],second['capabilities'])


if __name__ == '__main__': unittest.main()
