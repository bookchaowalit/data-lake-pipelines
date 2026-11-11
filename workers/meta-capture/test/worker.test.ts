// meta-capture Worker (spec docs/specs/meta-intake.md M1, M2, M8 and the reply rules).
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import worker, { describe as guess, ictDate, linksOf, type Env } from '../src/worker';

class FakeBucket {
  objects = new Map<string, { body: string | ArrayBuffer; type?: string }>();
  async delete(key: string) {
    this.objects.delete(key);
  }
  async put(key: string, body: string | ArrayBuffer, opts?: { onlyIf?: { etagDoesNotMatch?: string }; httpMetadata?: { contentType?: string } }) {
    if (opts?.onlyIf?.etagDoesNotMatch === '*' && this.objects.has(key)) return null;
    this.objects.set(key, { body, type: opts?.httpMetadata?.contentType });
    return { key };
  }
  json(key: string) {
    return JSON.parse(String(this.objects.get(key)!.body));
  }
}

let bucket: FakeBucket;
let env: Env;
let sent: { method: string; body: any }[];
let logs: string[];
const SECRET = 'hook-secret-0123456789abcdef';
const CHAT = 4242;
const DATE = Date.UTC(2026, 10, 12, 2, 0) / 1000; // 09:00 ICT 2026-11-12

beforeEach(() => {
  bucket = new FakeBucket();
  env = { LAKE: bucket as unknown as R2Bucket, META_CAPTURE_BOT_TOKEN: 'bot-token', META_CAPTURE_WEBHOOK_SECRET: SECRET,
    META_CAPTURE_CHAT_IDS: `${CHAT}`, LAKE_PREFIX: 'lake/' };
  sent = [];
  logs = [];
  vi.spyOn(console, 'log').mockImplementation((line: string) => { logs.push(line); });
  vi.stubGlobal('fetch', vi.fn(async (input: string | URL, init: RequestInit = {}) => {
    const url = String(input);
    if (url.includes('/file/bot')) return new Response(new Uint8Array([1, 2, 3]));
    const method = url.split('/').pop()!;
    const body = init.body ? JSON.parse(String(init.body)) : {};
    sent.push({ method, body });
    if (method === 'getFile') return Response.json({ ok: true, result: { file_path: 'photos/a.jpg', file_size: 3 } });
    return Response.json({ ok: true, result: { message_id: 999 } });
  }));
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

function post(message: Record<string, unknown> | undefined, secret = SECRET, updateId = 1) {
  return worker.fetch(new Request('https://w.test/telegram', {
    method: 'POST', headers: { 'x-telegram-bot-api-secret-token': secret, 'content-type': 'application/json' },
    body: JSON.stringify({ update_id: updateId, message }),
  }), env);
}

const msg = (over: Record<string, unknown> = {}) => ({ message_id: 10, date: DATE, chat: { id: CHAT }, from: { id: CHAT }, ...over });
const keys = () => [...bucket.objects.keys()].sort();
const replies = () => sent.filter((s) => s.method === 'sendMessage').map((s) => s.body.text as string);

describe('helpers', () => {
  it('reads url and text_link entities with UTF-16 offsets', () => {
    const text = 'ดู https://fb.com/x และ ลิงก์';
    const at = text.indexOf('https');
    expect(linksOf(text, [{ type: 'url', offset: at, length: 16 }, { type: 'text_link', offset: 0, length: 2, url: 'https://threads.net/@a/post/b' }]))
      .toEqual(['https://fb.com/x', 'https://threads.net/@a/post/b']);
  });
  it('guesses platform and surface for the reply', () => {
    expect(guess('#g ตลาด #author นาย ก', ['https://m.facebook.com/groups/1/posts/2'])).toEqual({ platform: 'facebook', surface: 'group', authorIgnored: true });
    expect(guess('', ['https://www.threads.net/@a/post/b'])).toEqual({ platform: 'threads', surface: 'unknown', authorIgnored: false });
    expect(guess('#ig #feed', [])).toEqual({ platform: 'instagram', surface: 'feed', authorIgnored: false });
  });
  it('dates are ICT', () => {
    expect(ictDate(Date.UTC(2026, 10, 11, 18, 0) / 1000)).toBe('2026-11-12');
  });
});

describe('webhook', () => {
  it('M1 stores a group capture with its screenshot and replies once', async () => {
    const caption = '#g ตลาดผลไม้ https://m.facebook.com/groups/5/posts/6/?mibextid=z';
    const link = caption.slice(caption.indexOf('https'));
    const r = await post(msg({ caption, caption_entities: [{ type: 'url', offset: caption.indexOf('https'), length: link.length }],
      photo: [{ file_id: 's' }, { file_id: 'big' }] }));
    expect(r.status).toBe(200);
    const k = 'lake/meta/landing/meta_capture/date=2026-11-12/';
    const media = keys().find((x) => x.includes('_media/'))!;
    const json = keys().find((x) => x.endsWith('.json'))!;
    expect(json.startsWith(k)).toBe(true);
    expect(media).toMatch(/^lake\/meta\/landing\/meta_capture_media\/date=2026-11-12\/tg-[0-9a-f]{8}-10\/10-0\.jpg$/);
    const rec = bucket.json(json);
    expect(rec).toMatchObject({ v: 1, kind: 'capture', message_id: 10, media_group_id: null, image_keys: [media] });
    expect(rec.urls).toEqual(['https://m.facebook.com/groups/5/posts/6/?mibextid=z']);
    expect(sent.find((s) => s.method === 'getFile')!.body.file_id).toBe('big');
    expect(replies()).toEqual([expect.stringMatching(/^เก็บแล้ว · facebook\/group · tg-[0-9a-f]{8}-10$/)]);
  });

  it('an album is one capture and gets one reply', async () => {
    await post(msg({ message_id: 20, media_group_id: '77', caption: 'โพสต์ในกลุ่ม #g A', photo: [{ file_id: 'a' }] }));
    await post(msg({ message_id: 21, media_group_id: '77', photo: [{ file_id: 'b' }] }), SECRET, 2);
    const ids = new Set(keys().filter((k) => k.endsWith('.json')).map((k) => bucket.json(k).capture_id));
    expect(ids.size).toBe(1);
    expect([...ids][0]).toMatch(/-g77$/);
    expect(replies()).toHaveLength(1);
  });

  it('an album without a caption still gets exactly one reply', async () => {
    await post(msg({ message_id: 50, media_group_id: '99', photo: [{ file_id: 'a' }] }));
    await post(msg({ message_id: 51, media_group_id: '99', photo: [{ file_id: 'b' }] }), SECRET, 2);
    await post(msg({ message_id: 51, media_group_id: '99', photo: [{ file_id: 'b' }] }), SECRET, 2); // resent
    expect(replies()).toEqual([expect.stringMatching(/^เก็บแล้ว · unknown\/unknown · tg-[0-9a-f]{8}-g99$/)]);
  });

  it('an unset or short secret refuses everything, even a request without the header', async () => {
    for (const configured of ['', 'short']) {
      env.META_CAPTURE_WEBHOOK_SECRET = configured;
      const bare = await worker.fetch(new Request('https://w.test/telegram', { method: 'POST',
        body: JSON.stringify({ update_id: 1, message: msg({ text: 'x' }) }) }), env);
      expect(bare.status).toBe(200);
      expect((await post(msg({ text: 'x' }), configured)).status).toBe(200);
    }
    env.META_CAPTURE_WEBHOOK_SECRET = SECRET;
    await worker.fetch(new Request('https://w.test/telegram', { method: 'POST',
      body: JSON.stringify({ update_id: 1, message: msg({ text: 'x' }) }) }), env); // no header
    expect(keys()).toEqual([]);
    expect(sent).toEqual([]);
  });

  it('M2 a wrong secret or another chat writes nothing and gets no reply', async () => {
    expect((await post(msg({ text: 'x' }), 'wrong')).status).toBe(200);
    expect((await post(msg({ text: 'x', chat: { id: 1 } }))).status).toBe(200);
    expect(keys()).toEqual([]);
    expect(sent).toEqual([]);
  });

  it('a resent update is written once and not replied to again', async () => {
    await post(msg({ text: 'hello' }));
    await post(msg({ text: 'hello' }));
    expect(keys().filter((k) => k.endsWith('.json'))).toHaveLength(1);
    expect(replies()).toHaveLength(1);
  });

  it('a retry after a crash between the landing write and the ack still replies once', async () => {
    const real = bucket.put.bind(bucket);
    let failAck = true;
    bucket.put = async (key: string, body: string | ArrayBuffer, opts?: any) => {
      if (key.endsWith('/ack') && failAck) { failAck = false; throw new Error('crash'); }
      return real(key, body, opts);
    };
    expect((await post(msg({ text: 'x' }))).status).toBe(500);
    expect(replies()).toEqual([]);
    expect((await post(msg({ text: 'x' }))).status).toBe(200);
    expect(replies()).toHaveLength(1);
  });

  it('a rejected confirmation is retried on the resend', async () => {
    const realFetch = globalThis.fetch;
    let reject = true;
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL, init: RequestInit = {}) => {
      if (String(input).endsWith('/sendMessage') && reject) { reject = false; return new Response('', { status: 429 }); }
      return (realFetch as typeof fetch)(input, init);
    }));
    expect((await post(msg({ text: 'x' }))).status).toBe(500);
    expect((await post(msg({ text: 'x' }))).status).toBe(200);
    expect(replies()).toHaveLength(1);
    expect(keys().filter((k) => k.endsWith('.json'))).toHaveLength(1);
    expect(logs.join('\n')).toContain('"error":"ReplyFailed"');
  });

  it('a permanent refusal is not retried (no poison loop)', async () => {
    const realFetch = globalThis.fetch;
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL, init: RequestInit = {}) => {
      if (String(input).endsWith('/sendMessage')) return new Response('', { status: 400 });
      return (realFetch as typeof fetch)(input, init);
    }));
    expect((await post(msg({ text: 'x' }))).status).toBe(200);
    expect((await post(msg({ text: 'x' }))).status).toBe(200);
    expect(keys().some((k) => k.endsWith('/ack'))).toBe(true);
  });

  it('replies quote the message but still send if it was deleted', async () => {
    await post(msg({ text: 'x' }));
    const body = sent.find((s) => s.method === 'sendMessage')!.body;
    expect(body.reply_parameters).toEqual({ message_id: 10, allow_sending_without_reply: true });
  });

  it('refuses an empty message', async () => {
    await post(msg({ text: '   ' }));
    expect(keys()).toEqual([]);
    expect(replies()[0]).toContain('ไม่ได้เก็บ');
  });

  it('notes that group authors are not kept', async () => {
    await post(msg({ text: '#g A #author นาย ก ข่าว' }));
    expect(replies()[0]).toContain('ไม่เก็บชื่อผู้โพสต์ในกลุ่ม');
  });

  it('#del as a reply to the bot confirmation writes a delete request', async () => {
    await post(msg({ message_id: 30, text: '#del', reply_to_message: { message_id: 999, date: DATE, chat: { id: CHAT }, from: { id: 1, is_bot: true }, text: 'เก็บแล้ว · facebook/group · tg-0a1b2c3d-10' } }));
    // the id belongs to another chat hash: refused
    expect(keys()).toEqual([]);
    sent = [];
    await post(msg({ message_id: 10, text: 'keep me' }), SECRET, 3);
    const id = bucket.json(keys()[0]).capture_id;
    await post(msg({ message_id: 31, text: '#del', reply_to_message: { message_id: 999, date: DATE, chat: { id: CHAT }, from: { id: 1, is_bot: true }, text: `เก็บแล้ว · unknown/unknown · ${id}` } }), SECRET, 4);
    const del = keys().find((k) => k.includes('meta_capture_del'))!;
    expect(del).toBe(`lake/meta/landing/meta_capture_del/date=2026-11-12/${id}/31.json`);
    expect(bucket.json(del)).toMatchObject({ kind: 'delete', capture_id: id });
    expect(replies().at(-1)).toContain(`จะลบ ${id}`);
  });

  it('#del as a reply to the owner own message targets that capture', async () => {
    await post(msg({ message_id: 40, text: '#del', reply_to_message: { message_id: 12, date: DATE, chat: { id: CHAT }, from: { id: CHAT }, media_group_id: '55' } }));
    const del = keys()[0];
    expect(bucket.json(del).capture_id).toMatch(/^tg-[0-9a-f]{8}-g55$/);
  });

  it('M8 logs carry no text, links or tokens', async () => {
    await post(msg({ text: 'ราคาทุเรียน https://facebook.com/groups/1/posts/2', entities: [{ type: 'url', offset: 12, length: 37 }] }));
    const blob = logs.join('\n');
    for (const needle of ['ราคาทุเรียน', 'facebook.com', 'bot-token', SECRET, String(CHAT)]) expect(blob).not.toContain(needle);
    expect(blob).toContain('"event":"stored"');
  });

  it('an R2 failure returns 500 so Telegram retries', async () => {
    bucket.put = async () => { throw new Error('boom'); };
    const r = await post(msg({ text: 'x' }));
    expect(r.status).toBe(500);
    expect(logs.join('\n')).not.toContain('boom');
  });

  it('health', async () => {
    const r = await worker.fetch(new Request('https://w.test/health'), env);
    expect(await r.json()).toEqual({ ok: true });
  });
});
