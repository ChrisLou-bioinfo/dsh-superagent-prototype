/**
 * Matrix source kind: normalizes Synapse room messages into domain Triggers, replies in the room, reads thread
 * context, and runs the long-polling loop. Talks to the homeserver's client-server API directly, so a deployment
 * needs no external CLI; the access token is part of the Source configuration.
 */
import type { Context } from '@deepseek-ai/cordis'
import { setTimeout as delay } from 'node:timers/promises'
import { z } from 'zod'
import { isoTime, key, type ChannelRef, type OutboundMessage, type Trigger } from '../core/index.ts'
import { KV } from '../kv.ts'
import { health, sourceContext, type SourceContext } from './host.ts'
import type { ChannelLink, ChannelLinks, ContextMessage, ContextPart, ContextPartName, Localized, SourceKind, ThreadContext } from './kind.ts'

export const SOURCE = 'matrix'
const Health = ['matrix'] as const
const EVENT = /\$[A-Za-z0-9_-]+/
/** Room IDs are `!opaque:server`; allow any server name so a self-hosted deployment can use its own. */
const ROOM = /^![A-Za-z0-9_-]+:[A-Za-z0-9.-]+(:[0-9]+)?$/
/** Matrix user IDs are `@localpart:server`; the localpart may contain dots, dashes and underscores. */
const USER = /^@[A-Za-z0-9._=/-]+:[A-Za-z0-9.-]+(:[0-9]+)?$/

const ConfigSchema = z.object({
  homeserver: z.string().trim().regex(/^(https?:\/\/\S+)?$/, 'http(s) URL').default(''),
  accessToken: z.string().trim().default(''),
  userId: z.string().trim().regex(/^(@[A-Za-z0-9._=/-]+:[A-Za-z0-9.-]+(:[0-9]+)?)?$/, '@user:server').default(''),
  rooms: z.array(z.object({ roomId: z.string().trim().regex(ROOM, '!room:server'), name: z.string().trim().max(80).optional(), listen: z.enum(['mentions', 'all']) }).strict())
    .refine(rooms => new Set(rooms.map(room => room.roomId)).size === rooms.length, 'Duplicate room IDs')
    .default([]),
  otherRooms: z.enum(['none', 'mentions']).default('none'),
  pollSeconds: z.number().int().min(5).max(3600).default(20),
  syncTimeoutSeconds: z.number().int().min(0).max(300).default(25),
}).strict()
export type MatrixConfig = z.infer<typeof ConfigSchema>

/** Stable trigger id for one Matrix event; every read path for this event produces the same id. */
export const matrixTriggerId = (eventId: string): string => key('matrix', eventId)
/** Thread id: one room message and its thread replies. */
export const matrixThreadId = (roomId: string, rootId: string | undefined, eventId: string): string => `${roomId}:${rootId ?? eventId}`
/** Channel address for a room reply; `rootId` keeps a reply inside its thread. */
export const matrixChannel = (roomId: string, eventId: string, rootId?: string): ChannelRef =>
  ({ source: SOURCE, thread: `matrix:${roomId}`, address: { roomId, eventId, ...rootId ? { rootId } : {} } })

/** A Matrix client-server event as `/sync` prints it. */
const EventSchema = z.object({
  type: z.string(),
  event_id: z.string().regex(EVENT),
  sender: z.string(),
  origin_server_ts: z.number().int().nonnegative(),
  content: z.record(z.string(), z.unknown()).default({}),
  room_id: z.string().optional(),
  unsigned: z.record(z.string(), z.unknown()).optional(),
})
export type MatrixEvent = z.infer<typeof EventSchema>

const matrixToLink = (roomId: string, eventId: string): string => `https://matrix.to/#/${encodeURIComponent(roomId)}/${encodeURIComponent(eventId)}`

/** Mention handling: MSC3952 user mentions, plus a plain `@localpart` match for clients that send no mention metadata. */
function mentioned(event: MatrixEvent, userId: string): boolean {
  const mentions = event.content['m.mentions']
  if (mentions && typeof mentions === 'object' && Array.isArray((mentions as { user_ids?: unknown }).user_ids)) {
    const ids = (mentions as { user_ids: unknown[] }).user_ids
    if (ids.includes(userId)) return true
  }
  const body = typeof event.content.body === 'string' ? event.content.body : ''
  const localpart = userId.replace(/^@/, '').split(':')[0] ?? ''
  return localpart.length > 0 && (body.includes(userId) || new RegExp(`(^|\\s)@${localpart.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`).test(body))
}

/** Thread root of a threaded reply (`m.relates_to` with `m.thread`), if any. */
function threadRoot(event: MatrixEvent): string | undefined {
  const relates = event.content['m.relates_to']
  if (!relates || typeof relates !== 'object') return undefined
  const { rel_type: relType, event_id: eventId } = relates as { rel_type?: unknown; event_id?: unknown }
  return relType === 'm.thread' && typeof eventId === 'string' ? eventId : undefined
}

/** The event a message replies to, if any (a plain `m.in_reply_to` relation). */
function repliesToEvent(event: MatrixEvent): string | undefined {
  const relates = event.content['m.relates_to']
  if (!relates || typeof relates !== 'object') return undefined
  const inner = (relates as { 'm.in_reply_to'?: unknown })['m.in_reply_to']
  if (!inner || typeof inner !== 'object') return undefined
  const eventId = (inner as { event_id?: unknown }).event_id
  return typeof eventId === 'string' ? eventId : undefined
}

/** Converts one Matrix message event into a Trigger, returning undefined for non-messages and the bot's own events. */
export function matrixTrigger(input: unknown, roomId: string, config: MatrixConfig): Trigger | undefined {
  const event = EventSchema.parse(input)
  if (event.type !== 'm.room.message') return
  if (event.sender === config.userId) return
  const msgtype = event.content.msgtype
  if (msgtype !== 'm.text' && msgtype !== 'm.notice' && msgtype !== 'm.emote') return
  const raw = event.content.body
  const body = typeof raw === 'string' ? raw : ''
  if (!body.trim()) return
  const rootId = threadRoot(event)
  return {
    id: matrixTriggerId(event.event_id),
    source: SOURCE,
    threads: [{ source: SOURCE, id: matrixThreadId(roomId, rootId, event.event_id) }],
    actor: { source: SOURCE, id: event.sender },
    human: true,
    ...mentioned(event, config.userId) ? { addressed: 'mention' as const } : {},
    at: isoTime(new Date(event.origin_server_ts).toISOString()),
    text: body,
    channel: matrixChannel(roomId, event.event_id, rootId),
    url: matrixToLink(roomId, event.event_id),
    meta: { roomId, eventId: event.event_id, ...rootId ? { rootId } : {}, ...repliesToEvent(event) ? { parentId: repliesToEvent(event)! } : {} },
  }
}

// ── homeserver access ───────────────────────────────────────────────────────────────────────────────────────────

/** Thrown when the homeserver answers with a Matrix error; `errcode` is the Matrix error code. */
export class MatrixError extends Error {
  constructor(message: string, readonly status: number, readonly errcode?: string) { super(message) }
}

const encode = (value: string): string => encodeURIComponent(value)

/** What a homeserver call needs: the address and the bot's access token. */
type Connection = { homeserver: string; accessToken: string }

/** Calls the client-server API with the Source's own access token. */
async function request(config: Connection, method: string, path: string, body: unknown, signal: AbortSignal): Promise<unknown> {
  const url = `${config.homeserver.replace(/\/+$/, '')}/_matrix/client/v3${path}`
  const response = await fetch(url, {
    method,
    signal,
    headers: { Authorization: `Bearer ${config.accessToken}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  })
  const text = await response.text()
  let value: unknown
  try { value = text ? JSON.parse(text) : {} } catch { value = {} }
  if (!response.ok) {
    const errcode = value && typeof value === 'object' && typeof (value as { errcode?: unknown }).errcode === 'string' ? (value as { errcode: string }).errcode : undefined
    const error = value && typeof value === 'object' && typeof (value as { error?: unknown }).error === 'string' ? (value as { error: string }).error : `HTTP ${response.status}`
    throw new MatrixError(error, response.status, errcode)
  }
  return value
}

/** A reply is permanently undeliverable when the room is gone or the bot may no longer post in it. */
const gone = (error: unknown): boolean => error instanceof MatrixError
  && (error.status === 403 || error.status === 404 || error.errcode === 'M_FORBIDDEN' || error.errcode === 'M_NOT_FOUND' || error.errcode === 'M_UNKNOWN_TOKEN')

/** Posts text into a room, in the Trigger's thread when it has one; the message id is the transaction id, so a retry sends once. */
export async function sendMatrix(config: Connection, message: OutboundMessage): Promise<'sent' | 'gone'> {
  const { roomId, rootId } = message.channel.address
  if (!roomId) throw new Error('Missing Matrix room for a reply')
  const content: Record<string, unknown> = { msgtype: 'm.text', body: message.text }
  if (rootId) content['m.relates_to'] = { rel_type: 'm.thread', event_id: rootId }
  try {
    await request(config, 'PUT', `/rooms/${encode(roomId)}/send/m.room.message/${encode(message.id)}`, content, AbortSignal.timeout(30_000))
  } catch (error) {
    if (gone(error)) return 'gone'
    throw error
  }
  return 'sent'
}

/** Deep link to the room, or to the message when its Trigger is known. */
export const matrixLink = (address: Record<string, string>, _links: ChannelLinks): ChannelLink => ({
  url: address.roomId ? `https://matrix.to/#/${encodeURIComponent(address.roomId)}` : '', label: { zh: 'Matrix 消息', en: 'Matrix message' },
  ...address.eventId ? { triggerId: matrixTriggerId(address.eventId) } : {},
})

/** Reads one event as a context message; events the bot cannot read are reported as deleted. */
function contextMessage(raw: unknown): ContextMessage | undefined {
  const parsed = EventSchema.safeParse(raw)
  if (!parsed.success) return undefined
  const event = parsed.data
  const body = typeof event.content.body === 'string' ? event.content.body : ''
  const redacted = event.type === 'm.room.message' ? false : event.type === 'm.room.redaction'
  return { id: event.event_id, from: event.sender, at: isoTime(new Date(event.origin_server_ts).toISOString()),
    text: redacted ? '' : body, ...redacted ? { deleted: true } : {}, ...repliesToEvent(event) ? { reply_to: repliesToEvent(event)! } : {},
    url: matrixToLink('', event.event_id).replace('/#//', '/#/') }
}

/** Thread context reader: a thread's replies, the thread root, and the replied-to event. */
export const matrixThreadContext: ThreadContext<MatrixConfig> = {
  messageId: trigger => String(trigger.meta.eventId ?? ''),
  async read(_commands, trigger, part: ContextPartName, cursor, config) {
    const roomId = String(trigger.meta.roomId ?? '')
    const eventId = String(trigger.meta.eventId ?? '')
    const rootId = typeof trigger.meta.rootId === 'string' ? trigger.meta.rootId : undefined
    const signal = AbortSignal.timeout(30_000)
    const empty: ContextPart = { messages: [], body: {} }
    if (!roomId) return empty
    if (part === 'parent') {
      const target = typeof trigger.meta.parentId === 'string' ? trigger.meta.parentId : undefined
      if (!target) return empty
      const event = contextMessage(await request(config, 'GET', `/rooms/${encode(roomId)}/event/${encode(target)}`, undefined, signal))
      return { messages: event ? [event] : [], body: {} }
    }
    if (part === 'root') {
      const target = rootId ?? eventId
      if (!target) return empty
      const event = contextMessage(await request(config, 'GET', `/rooms/${encode(roomId)}/event/${encode(target)}`, undefined, signal))
      return { messages: event ? [event] : [], body: {} }
    }
    // The room's recent messages, oldest first; the thread root narrows it to that thread when one exists.
    const path = rootId
      ? `/rooms/${encode(roomId)}/relations/${encode(rootId)}/m.thread?dir=b&limit=50${cursor ? `&from=${encode(cursor)}` : ''}`
      : `/rooms/${encode(roomId)}/messages?dir=b&limit=50${cursor ? `&from=${encode(cursor)}` : ''}`
    const page = await request(config, 'GET', path, undefined, signal) as { chunk?: unknown[]; next_batch?: string; start?: string }
    const messages = (page.chunk ?? []).map(contextMessage).filter((item): item is ContextMessage => item !== undefined).reverse()
    return { messages, ...(page.next_batch ?? page.start) ? { next: page.next_batch ?? page.start } : {}, body: {} }
  },
}

// ── the Source kind ─────────────────────────────────────────────────────────────────────────────────────────────

export const matrixKind: SourceKind<MatrixConfig> = {
  kind: SOURCE,
  title: { zh: 'Matrix', en: 'Matrix' },
  description: { zh: '接收下列房间里发给机器人的消息，直接连接自建的 Matrix homeserver。', en: 'Receives messages to the bot in these rooms, connecting to your own Matrix homeserver.' },
  schema: ConfigSchema,
  fields: [
    { key: 'homeserver', type: 'text', label: { zh: 'Homeserver 地址', en: 'Homeserver URL' },
      hint: { zh: '自建 Synapse 的客户端地址，例如 http://192.168.1.10:8008 或 https://matrix.example.com。', en: 'Client address of your Synapse, such as http://192.168.1.10:8008 or https://matrix.example.com.' } },
    { key: 'accessToken', type: 'text', label: { zh: '机器人 access token', en: 'Bot access token' },
      hint: { zh: '机器人账号的 access token。用 Element 登录机器人账号，设置里可以找到；或用 /login 接口取得。只保存在本机状态目录里。', en: 'Access token of the bot account. Find it in Element under the bot account\'s settings, or obtain it from /login. It is stored only in the local state directory.' } },
    { key: 'userId', type: 'text', pattern: '@[A-Za-z0-9._=/-]+:[A-Za-z0-9.-]+', label: { zh: '机器人用户 ID', en: 'Bot user ID' },
      hint: { zh: '完整 ID，例如 @mydsh:example.com。用来过滤机器人自己的消息，并识别 @ 提及。', en: 'Full ID such as @mydsh:example.com. Filters the bot\'s own messages and detects @ mentions.' } },
    { key: 'rooms', type: 'rows', label: { zh: '接收的房间', en: 'Rooms' },
      hint: { zh: '“仅 @”只接收 @ 机器人的消息和已有工单的线程；“全部消息”也接收其余消息，评估可以按规则从中推断工作。房间必须未加密，加密房间的消息无法读取。', en: '"@ only" receives messages that mention the bot and Threads that have Tickets; "All" also receives the rest, from which Eval may infer work. Rooms must be unencrypted; encrypted rooms cannot be read.' },
      columns: [
        { key: 'roomId', type: 'text', label: { zh: '房间 ID', en: 'Room ID' }, pattern: '![A-Za-z0-9_-]+:[A-Za-z0-9.-]+' },
        { key: 'listen', type: 'select', label: { zh: '接收', en: 'Receive' }, options: [{ value: 'mentions', label: { zh: '仅 @', en: '@ only' } }, { value: 'all', label: { zh: '全部消息', en: 'All' } }] },
      ] },
    { key: 'otherRooms', type: 'select', label: { zh: '其他房间', en: 'Other rooms' }, options: [{ value: 'none', label: { zh: '不接收', en: 'Not received' } }, { value: 'mentions', label: { zh: '仅 @', en: '@ only' } }],
      hint: { zh: '机器人已加入、但没列在上面的房间。“仅 @”接收那里 @ 机器人的消息；谁的消息能发起工作看策略里的准入。', en: 'Rooms the bot has joined that are not listed above. "@ only" receives messages there that mention the bot; whose messages may originate work is the Policy\'s admission.' } },
    { key: 'pollSeconds', type: 'number', advanced: true, min: 5, max: 3600, label: { zh: '同步间隔 / 秒', en: 'Sync interval / s' } },
    { key: 'syncTimeoutSeconds', type: 'number', advanced: true, min: 0, max: 300, label: { zh: '长连接等待 / 秒', en: 'Long-poll wait / s' } },
  ],
  health: [...Health],
  scope({ meta }, config) {
    const roomId = typeof meta.roomId === 'string' ? meta.roomId : ''
    const room = config.rooms.find(item => item.roomId === roomId)
    if (room) return { listened: true, observed: room.listen === 'all', row: room.roomId }
    return config.otherRooms === 'mentions' ? { listened: true, observed: false, row: 'other' } : { listened: false, reason: 'room_not_allowed' }
  },
  channel: {
    // A reply goes only to a room the Source still receives.
    allowed: (address, config) => matrixKind.scope!({ meta: { roomId: address.roomId ?? '' } }, config).listened,
    send: async (host, message) => {
      // The connection comes from the resolved deployment, as the Feishu adapter takes its bot from there.
      const connection = host.commands.deployment.matrix
      if (!connection) throw new Error('The Matrix Source is not configured')
      return sendMatrix(connection, message)
    },
    link: matrixLink,
  },
  threadContext: matrixThreadContext,
  find: query => ({ threads: [], terms: [...new Set(query.match(/\$[A-Za-z0-9_-]+/g) ?? [])] }),
  repliesTo: trigger => typeof trigger.meta.parentId === 'string' ? matrixTriggerId(trigger.meta.parentId) : undefined,
  policy: {
    unnamed: { person: { zh: 'Matrix 用户', en: 'Matrix user' }, bot: { zh: 'Matrix 机器人', en: 'Matrix bot' } },
    schema: z.object({
      allowed: z.array(z.object({ id: z.string().regex(USER, '@user:server'), name: z.string().trim().max(100).optional() }).strict())
        .refine(users => new Set(users.map(user => user.id)).size === users.length, 'Duplicate user IDs').default([]),
      bots: z.array(z.object({ id: z.string().regex(USER, '@user:server'), name: z.string().trim().max(100).optional() }).strict())
        .refine(bots => new Set(bots.map(bot => bot.id)).size === bots.length, 'Duplicate bot IDs').optional(),
    }).strict(),
    fields: [
      { key: 'allowed', type: 'rows', label: { zh: '能发起工作的 Matrix 用户', en: 'Matrix users who may originate work' },
        columns: [{ key: 'id', type: 'text', label: { zh: '用户 ID', en: 'User ID' }, pattern: '@[A-Za-z0-9._=/-]+:[A-Za-z0-9.-]+' }, { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] },
      { key: 'bots', type: 'rows', label: { zh: '消息可以成为候选的机器人', en: 'Bots whose messages may become candidates' },
        hint: { zh: '名单里的机器人在接收的房间里发的消息成为推断的候选，交给评估；它们不能发起明确的请求，也不能停止工单。', en: 'Messages of a bot on this list, in rooms the Source receives, become inferred candidates for an assessment; they never make an explicit request or stop a ticket.' },
        columns: [{ key: 'id', type: 'text', label: { zh: '用户 ID', en: 'User ID' }, pattern: '@[A-Za-z0-9._=/-]+:[A-Za-z0-9.-]+' },
          { key: 'name', type: 'text', label: { zh: '名字', en: 'Name' }, optional: true }] },
    ],
    explicit: { zh: '名单内的人 @ 机器人，总是明确的请求，不需要配置。', en: 'Someone on the list who mentions the bot always makes an explicit request; that needs no setting.' },
    rows(stored) {
      const config = ConfigSchema.parse(stored)
      const mentions: Localized = { zh: '仅 @', en: '@ only' }
      return [
        ...config.rooms.map(room => ({ id: room.roomId, label: room.name || room.roomId, note: room.listen === 'all' ? { zh: '全部消息', en: 'All' } : mentions,
          cells: { listedUnaddressed: room.listen === 'all', unlistedAddressed: true, unlistedUnaddressed: room.listen === 'all' } })),
        { id: 'other', label: { zh: '其他房间', en: 'Other rooms' }, note: config.otherRooms === 'mentions' ? mentions : { zh: '不接收', en: 'Not received' },
          cells: { listedUnaddressed: false, unlistedAddressed: config.otherRooms === 'mentions', unlistedUnaddressed: false } },
      ]
    },
  },
  environment: {
    variables: [
      { key: 'DSH_SUPERAGENT_MATRIX_ROOM', description: { zh: '接下工单的 Matrix 房间 ID；工单不是从 Matrix 来的时为空', en: 'Room ID of the Matrix message that started the ticket; empty when the ticket did not start on Matrix' } },
      { key: 'DSH_SUPERAGENT_MATRIX_USER', description: { zh: '写下这条 Matrix 消息的人的用户 ID；不是从 Matrix 来的时为空', en: 'User ID of the person whose Matrix message started the ticket; empty when it did not start on Matrix' } },
    ],
    values: ({ trigger }) => ({
      DSH_SUPERAGENT_MATRIX_ROOM: trigger?.source === SOURCE ? String(trigger.meta.roomId ?? '') : '',
      DSH_SUPERAGENT_MATRIX_USER: trigger?.source === SOURCE ? trigger.actor.id : '',
    }),
  },
  facts: deployment => deployment ? {} : {},
}

// ── the plugin entry and its loop ───────────────────────────────────────────────────────────────────────────────

interface SyncResponse { next_batch?: string; rooms?: { join?: Record<string, { timeline?: { events?: unknown[] } }> } }

/** One `/sync` round: stores every new message of a listened room as a Trigger, then advances the read position. */
export async function pollMatrix(ctx: SourceContext, signal: AbortSignal): Promise<{ read: number; skipped: number }> {
  const config = ctx.config<MatrixConfig>(SOURCE)
  if (!config) throw new Error('The Matrix Source is off')
  const since = ctx.state.get(KV.matrix.cursor) ?? ''
  const timeout = since ? config.syncTimeoutSeconds * 1000 : 0
  const filter = { room: { timeline: { limit: 50, types: ['m.room.message'] }, state: { types: [] }, ephemeral: { types: [] } } }
  const path = `/sync?timeout=${timeout}${since ? `&since=${encode(since)}` : ''}&filter=${encode(JSON.stringify(filter))}`
  const page = await request(config, 'GET', path, undefined, AbortSignal.any([signal, AbortSignal.timeout(timeout + 60_000)])) as SyncResponse
  let read = 0
  let skipped = 0
  for (const [roomId, room] of Object.entries(page.rooms?.join ?? {})) {
    const listened = config.rooms.some(item => item.roomId === roomId) || config.otherRooms !== 'none'
    for (const raw of room.timeline?.events ?? []) {
      if (signal.aborted) return { read, skipped }
      if (!listened) { skipped++; continue }
      const trigger = matrixTrigger(raw, roomId, config)
      if (!trigger) { skipped++; continue }
      await ctx.receive(trigger)
      ctx.threads.describe(`matrix:${roomId}:${String(trigger.meta.rootId ?? trigger.meta.eventId)}`, { label: `${roomId}`, url: trigger.url })
      read++
    }
  }
  // Advance the position only after every Trigger of this round was stored; stable ids make a replay harmless.
  if (page.next_batch && page.next_batch !== since) ctx.state.set(KV.matrix.cursor, page.next_batch)
  return { read, skipped }
}

export const name = 'dsh-superagent-matrix'
export const inject = ['orchestrator']

export function apply(ctx: Context): void {
  const orchestrator = ctx.orchestrator as unknown as import('./host.ts').SourceHost
  ctx.effect(() => orchestrator.sources.register(matrixKind), 'matrix: source kind')
  ctx.effect(() => {
    const source = sourceContext(orchestrator, matrixKind)
    const controller = new AbortController()
    // DSH ends its process on an unhandled rejection, so a failure that escapes the loop becomes the Source's health.
    const running = watch(source, controller.signal)
      .catch((error: unknown) => { source.report(SOURCE, { state: 'error', detail: String(error) }) })
    return async () => { controller.abort(); await running }
  }, 'matrix: sync loop')
}

async function watch(ctx: SourceContext, signal: AbortSignal): Promise<void> {
  await ctx.ready
  // Setup restarts dsh-superagent once it resolves the accounts; until then the Source reads nothing.
  if (!ctx.configured()) { ctx.report(SOURCE, health('paused', 'setupPending')); return }
  let failures = 0
  while (!signal.aborted) {
    const config = ctx.config<MatrixConfig>(SOURCE)
    if (!config || !config.homeserver || !config.accessToken || !config.userId) ctx.report(SOURCE, health('paused', 'sourceOff'))
    else {
      const at = new Date().toISOString()
      try {
        const { read, skipped } = await pollMatrix(ctx, signal)
        failures = 0
        ctx.report(SOURCE, { state: 'ready', detail: `${read} messages, ${skipped} skipped`, at })
      } catch (error) {
        failures += 1
        // One network failure is retried on the next round; three in a row need a person.
        if (!signal.aborted && failures >= 3) ctx.report(SOURCE, health('error', 'pollFailures', { count: String(failures), error: String(error) }))
      }
    }
    await delay((ctx.config<MatrixConfig>(SOURCE)?.pollSeconds ?? 20) * 1000, undefined, { signal }).catch(() => {})
  }
}