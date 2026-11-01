/** The Matrix Source: trigger normalization, self-filtering, thread mapping, receiving scope, and reply addressing. */
import { describe, expect, it } from 'vitest'
import { matrixChannel, matrixKind, matrixThreadId, matrixTrigger, matrixTriggerId, type MatrixConfig } from '../src/sources/matrix.ts'

const BOT = '@agent:example.org'
const config: MatrixConfig = {
  homeserver: 'https://matrix.example.org', accessToken: 'syt_token', userId: BOT,
  rooms: [{ roomId: '!room:example.org', listen: 'mentions' }, { roomId: '!all:example.org', listen: 'all' }],
  otherRooms: 'none', pollSeconds: 20, syncTimeoutSeconds: 25,
}
const event = (overrides: Record<string, unknown> = {}) => ({
  type: 'm.room.message', event_id: '$event1', sender: '@alice:example.org', origin_server_ts: 1_700_000_000_000,
  content: { msgtype: 'm.text', body: 'hello', 'm.mentions': { user_ids: [BOT] } }, ...overrides,
})

describe('matrix trigger', () => {
  it('turns a mention into an explicit trigger with a stable id and a room channel', () => {
    const trigger = matrixTrigger(event(), '!room:example.org', config)
    expect(trigger).toBeDefined()
    expect(trigger!.id).toBe(matrixTriggerId('$event1'))
    expect(trigger!.source).toBe('matrix')
    expect(trigger!.addressed).toBe('mention')
    expect(trigger!.actor).toEqual({ source: 'matrix', id: '@alice:example.org' })
    expect(trigger!.human).toBe(true)
    expect(trigger!.at).toBe(new Date(1_700_000_000_000).toISOString())
    expect(trigger!.channel).toEqual({ source: 'matrix', thread: 'matrix:!room:example.org',
      address: { roomId: '!room:example.org', eventId: '$event1' } })
    expect(trigger!.meta).toEqual({ roomId: '!room:example.org', eventId: '$event1' })
  })

  it('keeps a threaded reply in the thread of its root, and points replies at the root', () => {
    const trigger = matrixTrigger(event({ content: { msgtype: 'm.text', body: 'later', 'm.relates_to': { rel_type: 'm.thread', event_id: '$root' } } }),
      '!room:example.org', config)
    expect(trigger!.threads).toEqual([{ source: 'matrix', id: matrixThreadId('!room:example.org', '$root', '$event1') }])
    expect(trigger!.meta.rootId).toBe('$root')
    expect(trigger!.channel!.address.rootId).toBe('$root')
  })

  it('never takes in the bot\'s own messages, notices to nobody, edits, or non-text events', () => {
    expect(matrixTrigger(event({ sender: BOT }), '!room:example.org', config)).toBeUndefined()
    expect(matrixTrigger(event({ content: { msgtype: 'm.audio', body: 'voice' } }), '!room:example.org', config)).toBeUndefined()
    expect(matrixTrigger(event({ content: { msgtype: 'm.text', body: '   ' } }), '!room:example.org', config)).toBeUndefined()
    expect(matrixTrigger(event({ type: 'm.room.member' }), '!room:example.org', config)).toBeUndefined()
  })

  it('recognizes a plain @localpart mention when the client sends no mention metadata, and leaves unaddressed text unaddressed', () => {
    expect(matrixTrigger(event({ content: { msgtype: 'm.text', body: '@agent 改一下' } }), '!room:example.org', config)!.addressed).toBe('mention')
    expect(matrixTrigger(event({ content: { msgtype: 'm.text', body: '只是聊天' } }), '!room:example.org', config)!.addressed).toBeUndefined()
  })

  it('accepts a room message only according to the Source scope: listened rooms, a mentioned-only room observed only for tickets', () => {
    const scope = matrixKind.scope!
    expect(scope({ meta: { roomId: '!room:example.org' } }, config)).toEqual({ listened: true, observed: false, row: '!room:example.org' })
    expect(scope({ meta: { roomId: '!all:example.org' } }, config)).toEqual({ listened: true, observed: true, row: '!all:example.org' })
    expect(scope({ meta: { roomId: '!elsewhere:example.org' } }, config)).toEqual({ listened: false, reason: 'room_not_allowed' })
    expect(scope({ meta: { roomId: '!elsewhere:example.org' } }, { ...config, otherRooms: 'mentions' })).toEqual({ listened: true, observed: false, row: 'other' })
  })

  it('replies only into rooms the Source still receives, and links to the room', () => {
    const channel = matrixKind.channel!
    expect(channel.allowed!({ roomId: '!room:example.org' }, config)).toBe(true)
    expect(channel.allowed!({ roomId: '!elsewhere:example.org' }, config)).toBe(false)
    expect(channel.link({ roomId: '!room:example.org', eventId: '$event1' }, {})).toEqual({
      url: 'https://matrix.to/#/!room%3Aexample.org', label: { zh: 'Matrix 消息', en: 'Matrix message' }, triggerId: matrixTriggerId('$event1') })
    expect(matrixChannel('!room:example.org', '$event1', '$root').address).toEqual({ roomId: '!room:example.org', eventId: '$event1', rootId: '$root' })
  })

  it('reports a reply to another message, so a follow-up resumes the ticket its thread belongs to', () => {
    const trigger = matrixTrigger(event({ content: { msgtype: 'm.text', body: 're: that', 'm.relates_to': { 'm.in_reply_to': { event_id: '$other' } } } }),
      '!room:example.org', config)
    expect(matrixKind.repliesTo!(trigger!)).toBe(matrixTriggerId('$other'))
    expect(trigger!.meta.parentId).toBe('$other')
  })
})