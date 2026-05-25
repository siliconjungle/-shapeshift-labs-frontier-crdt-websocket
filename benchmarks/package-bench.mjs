import fs from 'node:fs';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { WebSocket } from 'ws';
import { createCrdtDocument } from '@shapeshift-labs/frontier-crdt';
import { createCrdtSyncEndpoint } from '@shapeshift-labs/frontier-crdt-sync';
import {
  createCrdtWebSocketProvider,
  createCrdtWebSocketSyncFrame,
  decodeCrdtWebSocketFrame,
  encodeCrdtWebSocketBinaryFrame,
  encodeCrdtWebSocketFrame
} from '../dist/index.js';
import { createCrdtWebSocketServer } from '../dist/server.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const args = parseArgs(process.argv.slice(2));
const rounds = readPositiveInt(args.rounds, 9);
const outPath = args.out ? path.resolve(rootDir, args.out) : null;
let sink = 0;

function measure(fn, inner) {
  for (let i = 0; i < inner; i++) fn();
  const samples = new Array(rounds);
  for (let roundIndex = 0; roundIndex < rounds; roundIndex++) {
    const start = performance.now();
    for (let i = 0; i < inner; i++) fn();
    samples[roundIndex] = ((performance.now() - start) * 1000) / inner;
  }
  samples.sort((left, right) => left - right);
  return { median: percentile(samples, 0.5), p95: percentile(samples, 0.95) };
}

async function measureAsync(fn, inner) {
  for (let i = 0; i < Math.min(inner, 3); i++) await fn();
  const samples = new Array(rounds);
  for (let roundIndex = 0; roundIndex < rounds; roundIndex++) {
    const start = performance.now();
    for (let i = 0; i < inner; i++) await fn();
    samples[roundIndex] = ((performance.now() - start) * 1000) / inner;
  }
  samples.sort((left, right) => left - right);
  return { median: percentile(samples, 0.5), p95: percentile(samples, 0.95) };
}

function runRow(name, inner, fn, extra = {}) {
  const timing = measure(fn, inner);
  return { fixture: name, medianUs: round(timing.median), p95Us: round(timing.p95), ...extra };
}

async function runAsyncRow(name, inner, fn, extra = {}) {
  const timing = await measureAsync(fn, inner);
  return { fixture: name, medianUs: round(timing.median), p95Us: round(timing.p95), ...extra };
}

function printReport(report) {
  console.log(report.package + ' package benchmark');
  console.log('Node ' + report.node + ' on ' + report.platform + ', rounds=' + rounds);
  console.log('These are Frontier-only package measurements, not competitor comparisons.');
  console.log('');
  console.log(padRight('Fixture', 44) + padLeft('Median', 12) + padLeft('p95', 11));
  for (const row of report.rows) {
    console.log(padRight(row.fixture, 44) + padLeft(formatUs(row.medianUs), 12) + padLeft(formatUs(row.p95Us), 11));
  }
  if (outPath) console.log('\nwrote ' + path.relative(rootDir, outPath));
}

function finish(packageName, rows) {
  const report = { package: packageName, version: readPackageVersion(), generatedAt: new Date().toISOString(), node: process.version, platform: process.platform + ' ' + process.arch, rounds, rows };
  if (outPath) { fs.mkdirSync(path.dirname(outPath), { recursive: true }); fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + '\n'); }
  printReport(report);
  if (sink === 42) console.log('sink=' + sink);
}

const frame = createCrdtWebSocketSyncFrame('bench-doc', 'alice', 'bob', {
  type: 'ack',
  senderId: 'alice',
  documentId: 'bench-doc',
  stateVector: { alice: 12 }
});

const rows = [
  runRow('WebSocket binary frame encode/decode', 5000, () => {
    sink += decodeCrdtWebSocketFrame(encodeCrdtWebSocketBinaryFrame(frame)).kind.length;
  }, { bytes: encodeCrdtWebSocketBinaryFrame(frame).byteLength }),
  runRow('WebSocket JSON frame encode/decode', 5000, () => {
    sink += decodeCrdtWebSocketFrame(encodeCrdtWebSocketFrame(frame)).kind.length;
  }, { bytes: encodeCrdtWebSocketFrame(frame).length }),
  await runAsyncRow('WebSocket client/server handshake', 20, async () => {
    sink += await connectOnce();
  }),
  await runAsyncRow('WebSocket two-peer CRDT sync', 20, async () => {
    sink += await syncOnce();
  })
];

finish('@shapeshift-labs/frontier-crdt-websocket', rows);

async function connectOnce() {
  const server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0 });
  await server.ready;
  const address = server.address();
  const url = `ws://127.0.0.1:${address.port}`;
  const doc = createCrdtDocument({ actorId: 'bench-a' });
  const provider = createCrdtWebSocketProvider(
    createCrdtSyncEndpoint(doc, { documentId: 'bench-doc', senderId: 'alice', actorRangeSync: true }),
    { url, documentId: 'bench-doc', peerId: 'alice', WebSocket }
  );
  await provider.connect();
  const count = server.getPeerIds('bench-doc').length;
  await provider.disconnect();
  await server.close();
  return count;
}

async function syncOnce() {
  const server = createCrdtWebSocketServer({ host: '127.0.0.1', port: 0 });
  await server.ready;
  const address = server.address();
  const url = `ws://127.0.0.1:${address.port}`;
  const alice = createCrdtDocument({ actorId: 'bench-alice' });
  const bob = createCrdtDocument({ actorId: 'bench-bob' });
  alice.set('/title', 'hello');
  const aliceProvider = createCrdtWebSocketProvider(
    createCrdtSyncEndpoint(alice, { documentId: 'bench-doc', senderId: 'alice', actorRangeSync: true }),
    { url, documentId: 'bench-doc', peerId: 'alice', WebSocket, syncOnConnect: true }
  );
  const bobProvider = createCrdtWebSocketProvider(
    createCrdtSyncEndpoint(bob, { documentId: 'bench-doc', senderId: 'bob', actorRangeSync: true }),
    { url, documentId: 'bench-doc', peerId: 'bob', WebSocket, syncOnConnect: true }
  );
  await aliceProvider.connect();
  await bobProvider.connect();
  await waitFor(() => bob.toJSON()?.title === 'hello');
  const value = bob.toJSON().title.length;
  await bobProvider.disconnect();
  await aliceProvider.disconnect();
  await server.close();
  return value;
}

async function waitFor(predicate, timeoutMs = 1000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error('timed out waiting for WebSocket sync benchmark');
}

function percentile(sorted, fraction) { return sorted[Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * fraction) - 1))]; }
function readPackageVersion() { return JSON.parse(fs.readFileSync(path.join(rootDir, 'package.json'), 'utf8')).version; }
function parseArgs(argv) { const out = {}; for (let i = 0; i < argv.length; i++) { const arg = argv[i]; if (arg === '--rounds') out.rounds = argv[++i]; else if (arg === '--out') out.out = argv[++i]; else if (arg === '--help' || arg === '-h') { console.log('Usage: npm run bench -- [--rounds 9] [--out benchmarks/results/package-bench.json]'); process.exit(0); } else throw new Error('unknown argument: ' + arg); } return out; }
function readPositiveInt(value, fallback) { if (value === undefined) return fallback; const number = Number(value); if (!Number.isInteger(number) || number <= 0) throw new Error('expected positive integer, got ' + value); return number; }
function round(value) { return Math.round(value * 100) / 100; }
function formatUs(value) { return value >= 1000 ? (value / 1000).toFixed(2) + ' ms' : value.toFixed(2) + ' us'; }
function padRight(value, width) { return String(value).padEnd(width); }
function padLeft(value, width) { return String(value).padStart(width); }
