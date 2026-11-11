// meta-capture: the owner's private Telegram bot for Meta posts (spec docs/specs/meta-intake.md).
// POST /telegram stores one landing JSON per message (plus screenshots) in R2 under
// <LAKE_PREFIX>/meta/landing/; the x-lakehouse cron parses, merges, OCRs and labels them.
// Logs carry the event, update id and counts only: never text, links or images.

export interface Env {
  LAKE: R2Bucket;
  META_CAPTURE_BOT_TOKEN: string;
  META_CAPTURE_WEBHOOK_SECRET: string;
  META_CAPTURE_CHAT_IDS: string;
  LAKE_PREFIX?: string;
  CF_VERSION_METADATA?: { id: string };
}

interface Entity { type: string; offset: number; length: number; url?: string }
interface PhotoSize { file_id: string; file_size?: number }
interface TelegramMessage {
  message_id: number;
  date: number;
  chat: { id: number };
  from?: { id: number; is_bot?: boolean };
  text?: string;
  caption?: string;
  entities?: Entity[];
  caption_entities?: Entity[];
  photo?: PhotoSize[];
  document?: { file_id: string; mime_type?: string; file_size?: number };
  media_group_id?: string;
  reply_to_message?: TelegramMessage;
}
interface TelegramUpdate { update_id: number; message?: TelegramMessage }

const MAX_IMAGE_BYTES = 10_000_000;
const TELEGRAM_TIMEOUT_MS = 8_000;
const CAPTURE_ID = /tg-[0-9a-f]{8}-g?\d+/;
const ICT_OFFSET_MS = 7 * 3_600_000;

function log(env: Env, event: string, fields: Record<string, unknown> = {}): void {
  console.log(JSON.stringify({ event, ...fields, version: env.CF_VERSION_METADATA?.id ?? 'unknown' }));
}

const errorName = (error: unknown) =>
  error instanceof Error && /^[A-Za-z][A-Za-z0-9_]{0,39}$/.test(error.name) ? error.name : 'unknown';

function safeEqual(a: string, b: string): boolean {
  const x = new TextEncoder().encode(a);
  const y = new TextEncoder().encode(b);
  let diff = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) diff |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return diff === 0;
}

async function chatHash(chatId: number): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(`meta-capture:${chatId}`));
  return [...new Uint8Array(digest).slice(0, 4)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export function ictDate(unixSeconds: number): string {
  return new Date(unixSeconds * 1000 + ICT_OFFSET_MS).toISOString().slice(0, 10);
}

/** Links in the message: Telegram url / text_link entities (UTF-16 offsets, as JS strings use). */
export function linksOf(text: string, entities: Entity[] = []): string[] {
  const out: string[] = [];
  for (const e of entities) {
    if (e.type === 'text_link' && e.url) out.push(e.url);
    else if (e.type === 'url') out.push(text.slice(e.offset, e.offset + e.length));
  }
  return [...new Set(out)];
}

const FOLD: Record<string, string> = { 'fb.com': 'facebook.com', 'instagr.am': 'instagram.com', 'threads.net': 'threads.com' };
const PLATFORM: Record<string, string> = { 'facebook.com': 'facebook', 'fb.watch': 'facebook', 'instagram.com': 'instagram', 'threads.com': 'threads' };

function hostOf(url: string): string {
  try {
    let host = new URL(url).hostname.toLowerCase();
    for (const p of ['www.', 'm.', 'web.', 'mbasic.', 'touch.', 'business.']) if (host.startsWith(p)) host = host.slice(p.length);
    return FOLD[host] ?? host;
  } catch {
    return '';
  }
}

/** A light guess for the reply only; the cron's parser (meta.py) is the one that is stored. */
export function describe(text: string, links: string[]): { platform: string; surface: string; authorIgnored: boolean } {
  const meta = links.find((u) => PLATFORM[hostOf(u)]);
  const tags = new Set([...text.matchAll(/(?<![\w#])#([^\s#]+)/g)].map((m) => m[1].toLowerCase()));
  let platform = meta ? PLATFORM[hostOf(meta)] : tags.has('ig') ? 'instagram' : tags.has('threads') ? 'threads' : 'unknown';
  let surface = tags.has('g') ? 'group' : tags.has('feed') ? 'feed' : tags.has('page') ? 'page' : tags.has('profile') ? 'profile' : 'unknown';
  if (surface === 'unknown' && meta) {
    try {
      if (new URL(meta).pathname.startsWith('/groups/')) surface = 'group';
    } catch { /* not a URL */ }
  }
  return { platform, surface, authorIgnored: surface === 'group' && tags.has('author') };
}

async function telegram(env: Env, method: string, body: Record<string, unknown>): Promise<Response> {
  return fetch(`https://api.telegram.org/bot${env.META_CAPTURE_BOT_TOKEN}/${method}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS),
  });
}

/** 'ok', 'retry' (429, 5xx, network or timeout) or 'failed' (any other refusal: retrying cannot help). */
async function reply(env: Env, msg: TelegramMessage, text: string): Promise<'ok' | 'retry' | 'failed'> {
  try {
    const r = await telegram(env, 'sendMessage', { chat_id: msg.chat.id, text, disable_web_page_preview: true,
      reply_parameters: { message_id: msg.message_id, allow_sending_without_reply: true } });
    if (r.ok) return 'ok';
    log(env, 'reply_failed', { status: r.status });
    return r.status === 429 || r.status >= 500 ? 'retry' : 'failed';
  } catch (error) {
    log(env, 'reply_failed', { error: errorName(error) });
    return 'retry';
  }
}

class ReplyFailed extends Error {
  override name = 'ReplyFailed';
}

/** Download one Telegram file; null when it is too big or Telegram refuses. */
async function download(env: Env, fileId: string): Promise<ArrayBuffer | null> {
  const r = await telegram(env, 'getFile', { file_id: fileId });
  if (!r.ok) return null;
  const info = (await r.json()) as { ok?: boolean; result?: { file_path?: string; file_size?: number } };
  const path = info.result?.file_path;
  if (!info.ok || !path || (info.result?.file_size ?? 0) > MAX_IMAGE_BYTES) return null;
  const f = await fetch(`https://api.telegram.org/file/bot${env.META_CAPTURE_BOT_TOKEN}/${path}`, { signal: AbortSignal.timeout(TELEGRAM_TIMEOUT_MS) });
  if (!f.ok) return null;
  const data = await f.arrayBuffer();
  return data.byteLength > MAX_IMAGE_BYTES ? null : data;
}

function landingKey(env: Env, rel: string): string {
  const prefix = (env.LAKE_PREFIX ?? '').replace(/^\/+|\/+$/g, '');
  return [prefix, 'meta', rel].filter(Boolean).join('/');
}

/** Write once: false when the key already exists (Telegram resent the update). */
async function putNew(env: Env, key: string, body: ArrayBuffer | string, contentType: string): Promise<boolean> {
  const r = await env.LAKE.put(key, body, { onlyIf: { etagDoesNotMatch: '*' }, httpMetadata: { contentType } });
  return r !== null;
}

function captureIdOf(hash: string, msg: TelegramMessage): string {
  return msg.media_group_id ? `tg-${hash}-g${msg.media_group_id}` : `tg-${hash}-${msg.message_id}`;
}

async function handleDelete(env: Env, msg: TelegramMessage, hash: string): Promise<string> {
  const target = msg.reply_to_message;
  let id: string | undefined;
  if (target?.from?.is_bot) id = (target.text ?? '').match(CAPTURE_ID)?.[0];
  else if (target) id = captureIdOf(hash, target);
  if (!id || !id.startsWith(`tg-${hash}-`)) {
    await reply(env, msg, 'ลบไม่ได้: ตอบกลับ (reply) ข้อความยืนยันของบอต หรือโพสต์ที่ส่งไว้ แล้วพิมพ์ #del');
    return 'delete_refused';
  }
  const rec = { v: 1, kind: 'delete', capture_id: id, message_id: msg.message_id, chat_hash: hash, date: msg.date };
  const fresh = await putNew(env, landingKey(env, `landing/meta_capture_del/date=${ictDate(msg.date)}/${id}/${msg.message_id}.json`), JSON.stringify(rec), 'application/json');
  if (fresh) await reply(env, msg, `จะลบ ${id} ในรอบประมวลผลถัดไป`);
  return 'delete_requested';
}

async function handleCapture(env: Env, msg: TelegramMessage, hash: string): Promise<{ status: string; images: number }> {
  const text = msg.text ?? msg.caption ?? '';
  const links = linksOf(text, msg.entities ?? msg.caption_entities ?? []);
  const fileId = msg.photo?.length ? msg.photo[msg.photo.length - 1].file_id
    : msg.document?.mime_type?.startsWith('image/') ? msg.document.file_id : undefined;
  if (!text.trim() && !links.length && !fileId) {
    await reply(env, msg, 'ไม่ได้เก็บ: ไม่มีลิงก์ ข้อความ หรือรูป');
    return { status: 'empty', images: 0 };
  }
  const id = captureIdOf(hash, msg);
  const day = ictDate(msg.date);
  const imageKeys: string[] = [];
  if (fileId) {
    const data = await download(env, fileId);
    if (data) {
      const k = landingKey(env, `landing/meta_capture_media/date=${day}/${id}/${msg.message_id}-0.jpg`);
      await putNew(env, k, data, 'image/jpeg');
      imageKeys.push(k);
    }
  }
  const rec = { v: 1, kind: 'capture', capture_id: id, message_id: msg.message_id, chat_hash: hash, date: msg.date,
    media_group_id: msg.media_group_id ?? null, text, urls: links, image_keys: imageKeys };
  const fresh = await putNew(env, landingKey(env, `landing/meta_capture/date=${day}/${id}/${msg.message_id}.json`), JSON.stringify(rec), 'application/json');
  // One reply per capture: an album arrives as one update per photo, captioned or not, so the first
  // update to claim the capture's ack marker replies. The marker is claimed after the landing write and
  // whether or not that write was fresh, so a retry after a crash between the two still replies once.
  const ackKey = landingKey(env, `landing/meta_capture/date=${day}/${id}/ack`);
  const ack = await putNew(env, ackKey, '', 'text/plain');
  if (ack) {
    const d = describe(text, links);
    const lost = fileId && !imageKeys.length ? '\n(โหลดรูปไม่ได้ ส่งใหม่ได้)' : '';
    const author = d.authorIgnored ? '\n(ไม่เก็บชื่อผู้โพสต์ในกลุ่ม)' : '';
    if ((await reply(env, msg, `เก็บแล้ว · ${d.platform}/${d.surface} · ${id}${author}${lost}`)) === 'retry') {
      // Transient refusal: release the marker and answer 500, so Telegram resends the update and the
      // confirmation is tried again (landing files are write-once: the resend stores nothing twice).
      // A permanent refusal keeps the marker and answers 200, so a bad update never loops.
      await env.LAKE.delete(ackKey);
      throw new ReplyFailed();
    }
  }
  return { status: fresh ? 'stored' : 'duplicate', images: imageKeys.length };
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (request.method === 'GET' && url.pathname === '/health') return Response.json({ ok: true });
    if (request.method !== 'POST' || url.pathname !== '/telegram') return new Response('not found', { status: 404 });
    // Wrong secret or chat: 200 with nothing written and no reply, so the bot gives nothing away.
    // An unset or short secret refuses everything (never fail open on a missing setting).
    const secret = env.META_CAPTURE_WEBHOOK_SECRET ?? '';
    const header = request.headers.get('x-telegram-bot-api-secret-token');
    if (secret.length < 24 || header === null) {
      log(env, 'refused', { reason: secret.length < 24 ? 'unconfigured' : 'secret' });
      return new Response('', { status: 200 });
    }
    if (!safeEqual(request.headers.get('x-telegram-bot-api-secret-token') ?? '', env.META_CAPTURE_WEBHOOK_SECRET)) {
      log(env, 'refused', { reason: 'secret' });
      return new Response('', { status: 200 });
    }
    let update: TelegramUpdate;
    try {
      update = (await request.json()) as TelegramUpdate;
    } catch {
      return new Response('', { status: 200 });
    }
    const msg = update.message;
    const allowed = new Set((env.META_CAPTURE_CHAT_IDS ?? '').split(',').map((s) => s.trim()).filter(Boolean));
    if (!msg || !allowed.has(String(msg.chat?.id))) {
      log(env, 'refused', { reason: msg ? 'chat' : 'no_message', update_id: update.update_id });
      return new Response('', { status: 200 });
    }
    try {
      const hash = await chatHash(msg.chat.id);
      const text = (msg.text ?? msg.caption ?? '').trim();
      if (/^#del\b/i.test(text)) {
        log(env, await handleDelete(env, msg, hash), { update_id: update.update_id });
      } else if (text === '/start') {
        await reply(env, msg, 'พร้อมรับโพสต์ · ส่งลิงก์ ข้อความ หรือ screenshot · #g ชื่อกลุ่ม · #note ข้อความ · ตอบกลับด้วย #del เพื่อลบ');
        log(env, 'start', { update_id: update.update_id });
      } else {
        const r = await handleCapture(env, msg, hash);
        log(env, r.status, { update_id: update.update_id, images: r.images });
      }
      return new Response('', { status: 200 });
    } catch (error) {
      // 500: Telegram retries the update; every write is write-once, so a retry never duplicates.
      log(env, 'error', { update_id: update.update_id, error: errorName(error) });
      return new Response('', { status: 500 });
    }
  },
};
