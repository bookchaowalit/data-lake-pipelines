// Cloudflare Pages refuses files over 25 MiB, and the DuckDB-WASM engines the
// Evidence build copies into build/ are 33-38 MiB. This moves each oversized
// .wasm to the same file of the same @duckdb/duckdb-wasm version on jsDelivr,
// after checking that the CDN serves exactly the bytes that were built, and
// rewrites the build's references to it. Anything unexpected fails the build.
import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, statSync, unlinkSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

const MAX_BYTES = 25 * 1024 * 1024;
const BUILD = 'build';
const PKG_DIR = 'node_modules/@duckdb/duckdb-wasm';
const version = JSON.parse(readFileSync(join(PKG_DIR, 'package.json'), 'utf8')).version;
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');

function walk(dir) {
	return readdirSync(dir).flatMap((name) => {
		const path = join(dir, name);
		return statSync(path).isDirectory() ? walk(path) : [path];
	});
}

const files = walk(BUILD);
const packaged = new Map(
	readdirSync(join(PKG_DIR, 'dist'))
		.filter((name) => name.endsWith('.wasm'))
		.map((name) => [sha256(readFileSync(join(PKG_DIR, 'dist', name))), name])
);

let moved = 0;
for (const file of files.filter((path) => statSync(path).size > MAX_BYTES)) {
	const digest = sha256(readFileSync(file));
	const name = packaged.get(digest);
	if (!file.endsWith('.wasm') || !name) {
		throw new Error(`${file} is over 25 MiB and is not a packaged DuckDB-WASM engine`);
	}
	const url = `https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@${version}/dist/${name}`;
	const response = await fetch(url);
	if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
	if (sha256(Buffer.from(await response.arrayBuffer())) !== digest) {
		throw new Error(`${url} does not match the built ${name}`);
	}
	const reference = `"/${relative(BUILD, file).split('\\').join('/')}"`;
	let rewritten = 0;
	for (const js of files.filter((path) => path.endsWith('.js'))) {
		const text = readFileSync(js, 'utf8');
		if (text.includes(reference)) {
			writeFileSync(js, text.replaceAll(reference, JSON.stringify(url)));
			rewritten += 1;
		}
	}
	if (rewritten === 0) throw new Error(`no reference to ${reference} found in the build`);
	unlinkSync(file);
	moved += 1;
	console.log(`offloaded ${name} (${rewritten} reference) -> ${url}`);
}

const left = walk(BUILD).filter((path) => statSync(path).size > MAX_BYTES);
if (left.length) throw new Error(`still over 25 MiB: ${left.join(', ')}`);
console.log(`offload-wasm: ${moved} file(s) moved, every file now under 25 MiB`);
