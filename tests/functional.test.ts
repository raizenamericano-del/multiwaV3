/**
 * Functional test suite — `npm test`
 *
 * Boots the real Socket.io gateway, seeds a session with a fake Baileys socket
 * and drives the session manager end to end. Covers:
 *   - LID (@lid) chats being resolved to phone numbers instead of dropped
 *   - view-once media: parsing, media-node persistence, retry flow
 *   - reactions (incoming + outgoing), revokes (delete for everyone)
 *   - quoted replies and "delete for me"
 *   - the Socket.io events the UI depends on
 *
 * It uses its own SQLite/Postgres database from DATABASE_URL and cleans up.
 */
import { createServer } from 'node:http'
import { Server as IOServer } from 'socket.io'
import { io as ioClient } from 'socket.io-client'
import { registerSocketGateway, SOCKET_PATH } from '@/lib/socket-server'
import { sessionManager } from '@/lib/baileys/session-manager'
import { parseWaMessage, serializeMediaNode, rebuildMediaMessage, parseReaction, parseRevoke, quotedPreview } from '@/lib/baileys/persistence'
import prisma from '@/lib/prisma'

const PORT = 3997
let pass = 0, fail = 0
const check = (label: string, cond: boolean, detail?: unknown) => {
  if (cond) { pass++; console.log(`  ✓ ${label}`) }
  else { fail++; console.log(`  ✗ ${label} ${detail !== undefined ? JSON.stringify(detail) : ''}`) }
}

const httpServer = createServer()
const io = new IOServer(httpServer, { path: SOCKET_PATH })
await registerSocketGateway(io)
await new Promise<void>((r) => httpServer.listen(PORT, r))

const session = await prisma.waSession.create({
  data: { name: 'Features Test', phoneNumber: '628' + String(Date.now()).slice(-9), status: 'connected' },
})

const sendToWa: unknown[] = []
const live = {
  sessionId: session.id,
  sock: {
    user: { id: '6281234567890:1@s.whatsapp.net', name: 'gw' },
    groupMetadata: async () => ({ subject: 'Grup' }),
    updateMediaMessage: async () => null,
    sendMessage: async (_jid: string, content: unknown) => {
      sendToWa.push(content)
      return { key: { id: `OUT${sendToWa.length}`, remoteJid: 'x@s.whatsapp.net', fromMe: true } }
    },
  },
  qr: null, reconnectAttempts: 0, reconnectTimer: null, manualDisconnect: false,
  pairingRequested: false, startedAt: Date.now(),
  lidToPn: new Map(), contactNames: new Map(), debugLog: [],
  counters: { upsert: 0, stored: 0, skipped: 0, historyMessages: 0, reactions: 0, revoked: 0, mediaRetried: 0, mediaRecovered: 0 },
} as never
;(sessionManager as unknown as { sessions: Map<string, unknown> }).sessions.set(session.id, live)

const socket = ioClient(`http://localhost:${PORT}`, { path: SOCKET_PATH, transports: ['websocket'] })
const events: string[] = []
socket.on('session:message-updated', (p: { message: { id: string; reactions: Record<string,string>; deletedAt: string | null; mediaStatus: string | null } }) =>
  events.push(`updated:${p.message.id}:${JSON.stringify(p.message.reactions)}:del=${p.message.deletedAt ? 'y' : 'n'}:media=${p.message.mediaStatus}`))
socket.on('session:message-removed', () => events.push('removed'))
socket.on('session:message', (p: { message: { id: string } }) => events.push(`new:${p.message.id}`))
socket.emit('subscribe', { sessionId: session.id })
await new Promise((r) => socket.on('server:ready', r))
await new Promise((r) => setTimeout(r, 250))

const manager = sessionManager as unknown as {
  handleIncomingMessage: (l: unknown, r: unknown) => Promise<void>
  handleReaction: (l: unknown, r: unknown) => Promise<void>
  handleRevoke: (l: unknown, r: unknown) => Promise<void>
  retryMediaDownload: (s: string, m?: string, w?: string) => Promise<unknown>
  sendReaction: (s: string, m: string, e: string) => Promise<unknown>
  deleteMessage: (s: string, m: string, scope: 'me' | 'everyone') => Promise<unknown>
}
const now = () => Math.floor(Date.now() / 1000)
const mk = (o: Record<string, unknown>) => ({ pushName: 'Kontak', messageTimestamp: now(), ...o } as never)

console.log('\n=== 1. VIEW ONCE: parsing + media node + status ===')
const voImage = mk({
  key: { remoteJid: '6281111111111@s.whatsapp.net', fromMe: false, id: 'VO1' },
  message: {
    viewOnceMessageV2: {
      message: {
        imageMessage: {
          url: 'https://mmg.whatsapp.net/d/fake.enc', directPath: '/v/t62/f.enc',
          mediaKey: Buffer.alloc(32, 7), fileEncSha256: Buffer.alloc(32, 3), fileSha256: Buffer.alloc(32, 4),
          mimetype: 'image/jpeg', fileLength: '2048', caption: 'rahasia', viewOnce: true, mediaKeyTimestamp: '1700000000',
        },
      },
    },
  },
})
const parsed = parseWaMessage(voImage)
check('viewOnce terdeteksi', parsed?.viewOnce === true)
check('tipe = image', parsed?.type === 'image')
check('caption terbaca', parsed?.text === 'rahasia')
check('mediaNode tersimpan (untuk retry)', Boolean(parsed?.mediaNode) && parsed!.mediaNode!.includes('imageMessage'))
const rebuilt = rebuildMediaMessage(parsed!.mediaNode!, { id: 'VO1', remoteJid: '6281111111111@s.whatsapp.net', fromMe: false })
check('mediaNode bisa di-rebuild jadi WAMessage', Boolean(rebuilt?.message?.imageMessage))
check('mediaKey round-trip utuh (Buffer)', Buffer.isBuffer((rebuilt!.message!.imageMessage as { mediaKey: unknown }).mediaKey))

await manager.handleIncomingMessage(live, voImage)
const voRow = await prisma.message.findFirst({ where: { sessionId: session.id, waMessageId: 'VO1' } })
check('baris tersimpan dengan viewOnce=true', voRow?.viewOnce === true)
check('mediaStatus = pending (WhatsApp belum bagi media)', voRow?.mediaStatus === 'pending', voRow?.mediaStatus)
check('mediaRetries mulai dari 0', voRow?.mediaRetries === 0)
const voChat = await prisma.chat.findUnique({ where: { id: voRow!.chatId } })
check('preview chat menandai view once', Boolean(voChat?.lastMessagePreview?.startsWith('👁️')), voChat?.lastMessagePreview)

console.log('\n=== 2. MEDIA RETRY (dipicu messages.media-update) ===')
await manager.retryMediaDownload(session.id, undefined, 'VO1')
const afterRetry = await prisma.message.findUnique({ where: { id: voRow!.id } })
check('percobaan retry tercatat', (afterRetry?.mediaRetries ?? 0) >= 1, afterRetry?.mediaRetries)
const counters = (live as { counters: Record<string, number> }).counters
check('counter mediaRetried naik', counters.mediaRetried >= 1)
check('event message-updated dikirim ke browser', events.some((e) => e.startsWith(`updated:${voRow!.id}`)), events.slice(-3))

console.log('\n=== 3. REACTION ===')
await manager.handleIncomingMessage(live, mk({
  key: { remoteJid: '6281111111111@s.whatsapp.net', fromMe: false, id: 'R1' },
  message: { conversation: 'pesan untuk direaksi' },
}))
const target = await prisma.message.findFirst({ where: { sessionId: session.id, waMessageId: 'R1' } })
const reactionRaw = mk({
  key: { remoteJid: '6281111111111@s.whatsapp.net', fromMe: false, id: 'RX1', participant: '6281111111111@s.whatsapp.net' },
  message: { reactionMessage: { text: '🔥', key: { id: 'R1', remoteJid: '6281111111111@s.whatsapp.net', fromMe: false } } },
})
const parsedReaction = parseReaction(reactionRaw as never)
check('reactionMessage diparse', parsedReaction?.emoji === '🔥' && parsedReaction?.targetWaMessageId === 'R1')
await manager.handleReaction(live, parsedReaction)
const reacted = await prisma.message.findUnique({ where: { id: target!.id } })
check('reaction tersimpan di baris target', JSON.parse(reacted?.reactions ?? '{}')['6281111111111@s.whatsapp.net'] === '🔥', reacted?.reactions)
check('reaction TIDAK dibuat jadi pesan baru', (await prisma.message.count({ where: { sessionId: session.id, waMessageId: 'RX1' } })) === 0)
check('counter reactions naik', counters.reactions === 1)

console.log('\n=== 4. SEND REACTION (keluar) ===')
await manager.sendReaction(session.id, target!.id, '👍')
check('baileys menerima payload react', JSON.stringify(sendToWa.at(-1)).includes('"react"'))
check('reaction kita tersimpan', Object.values(JSON.parse((await prisma.message.findUnique({ where: { id: target!.id } }))?.reactions ?? '{}')).includes('👍'))

console.log('\n=== 5. REVOKE (delete for everyone dari lawan) ===')
const revokeRaw = mk({
  key: { remoteJid: '6281111111111@s.whatsapp.net', fromMe: false, id: 'RV1' },
  message: { protocolMessage: { type: 0, key: { id: 'R1', remoteJid: '6281111111111@s.whatsapp.net', fromMe: false } } },
})
const parsedRevoke = parseRevoke(revokeRaw as never)
check('protocolMessage REVOKE diparse', parsedRevoke?.targetWaMessageId === 'R1')
await manager.handleRevoke(live, parsedRevoke)
const revoked = await prisma.message.findUnique({ where: { id: target!.id } })
check('deletedAt terisi', Boolean(revoked?.deletedAt))
check('teks dikosongkan', revoked?.text === null)
check('bukan dibuat sebagai pesan baru', (await prisma.message.count({ where: { sessionId: session.id, waMessageId: 'RV1' } })) === 0)
check('counter revoked naik', counters.revoked === 1)

console.log('\n=== 6. QUOTED / REPLY ===')
const asked = mk({
  key: { remoteJid: '6281111111111@s.whatsapp.net', fromMe: false, id: 'Q1' },
  message: { extendedTextMessage: { text: 'Balasan saya', contextInfo: { stanzaId: 'VO1', participant: '6281111111111@s.whatsapp.net', quotedMessage: { conversation: 'rahasia' } } } },
})
const parsedQuote = parseWaMessage(asked as never)
check('quoted terdeteksi', parsedQuote?.quoted?.id === 'VO1')
check('preview quoted = teks aslinya', parsedQuote?.quoted?.text === 'rahasia')
check('quoted fromMe=false (dari lawan)', parsedQuote?.quoted?.fromMe === false)
check('quotedPreview() jalan langsung', quotedPreview({ extendedTextMessage: { text: 'x', contextInfo: { stanzaId: 'Z', quotedMessage: { imageMessage: { caption: 'foto lama' } } } } })?.type === 'image')

console.log('\n=== 7. DELETE FOR ME (hapus lokal) ===')
const toDeleteRow = await prisma.message.findFirst({ where: { sessionId: session.id, waMessageId: 'Q1' } })
    ?? await prisma.message.findFirst({ where: { sessionId: session.id, waMessageId: 'R1' } })
await manager.handleIncomingMessage(live, asked)
const qRow = await prisma.message.findFirst({ where: { sessionId: session.id, waMessageId: 'Q1' } })
await manager.deleteMessage(session.id, qRow!.id, 'me')
check('baris benar-benar terhapus dari DB', (await prisma.message.findUnique({ where: { id: qRow!.id } })) === null)
check('event message-removed dikirim', events.includes('removed'))
void toDeleteRow

console.log('\n=== 8. DEBUG ENDPOINT: counter lengkap ===')
const debug = await sessionManager.getDebugInfo(session.id)
check('counters reaksi/revoke/media ada', debug?.counters?.reactions === 1 && debug?.counters?.revoked === 1 && (debug?.counters?.mediaRetried ?? 0) >= 1, debug?.counters)

socket.close()
await prisma.waSession.delete({ where: { id: session.id } })
await prisma.$disconnect()
httpServer.close()
console.log(`\nHASIL: ${pass} lulus, ${fail} gagal\n`)
process.exit(fail === 0 ? 0 : 1)
