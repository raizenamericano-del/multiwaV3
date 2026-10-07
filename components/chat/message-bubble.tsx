'use client'

import * as React from 'react'
import {
  AlertCircle,
  Check,
  CheckCheck,
  Clock,
  Copy,
  CornerUpLeft,
  Download,
  Eye,
  FileText,
  Loader2,
  MapPin,
  MoreVertical,
  RefreshCw,
  SmilePlus,
  Trash2,
  X,
} from 'lucide-react'
import { toast } from 'sonner'
import { AudioPlayer } from '@/components/chat/audio-player'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { api } from '@/lib/fetcher'
import type { MessageDTO, MessageStatus } from '@/lib/types'
import { cn, formatBytes, formatTime } from '@/lib/utils'

const QUICK_REACTIONS = ['❤️', '😂', '👍', '🙏', '🔥', '😮', '😢']

function StatusTicks({ status }: { status: MessageStatus }) {
  if (status === 'failed') return <AlertCircle className="h-3.5 w-3.5 text-red-400" />
  if (status === 'pending') return <Clock className="h-3 w-3 opacity-80" />
  if (status === 'sent') return <Check className="h-3.5 w-3.5 opacity-80" />
  if (status === 'read') return <CheckCheck className="h-3.5 w-3.5 text-[#53bdeb]" />
  return <CheckCheck className="h-3.5 w-3.5 opacity-80" />
}

function typeLabel(message: MessageDTO) {
  switch (message.type) {
    case 'image':
      return '📷 Photo'
    case 'video':
      return '🎥 Video'
    case 'audio':
      return '🎙️ Voice note'
    case 'document':
      return message.mediaName ?? '📄 Document'
    case 'sticker':
      return '🩹 Sticker'
    default:
      return '💬 Message'
  }
}

interface Props {
  message: MessageDTO
  showSender: boolean
  isGroup: boolean
  onOpenMedia: (message: MessageDTO) => void
  onReply: (message: MessageDTO) => void
  onReact: (message: MessageDTO, emoji: string) => void
  onDeleteLocal: (message: MessageDTO) => void
  onUpdated: () => void
}

export function MessageBubble({
  message,
  showSender,
  isGroup,
  onOpenMedia,
  onReply,
  onReact,
  onDeleteLocal,
  onUpdated,
}: Props) {
  const out = message.fromMe
  const mediaUrl = message.mediaPath ? `/api/media?id=${message.id}` : null
  const downloadUrl = mediaUrl ? `${mediaUrl}&download=1` : null

  const [revealed, setRevealed] = React.useState(!message.viewOnce)
  const [busy, setBusy] = React.useState(false)
  const [reactOpen, setReactOpen] = React.useState(false)

  const reactions = Object.values(message.reactions ?? {})
  const isDeleted = Boolean(message.deletedAt)
  const mediaMissing = !mediaUrl && message.type !== 'text' && message.type !== 'other'
  const mediaPending = mediaMissing && !isDeleted

  async function retryDownload() {
    setBusy(true)
    try {
      await api.post(`/api/messages/${message.id}/download`)
      toast.success('Media downloaded')
      onUpdated()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Media is not available yet')
    } finally {
      setBusy(false)
    }
  }

  async function deleteForEveryone() {
    setBusy(true)
    try {
      await api.delete(`/api/messages/${message.id}?scope=everyone`)
      toast.success('Deleted for everyone')
      onUpdated()
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Delete failed')
    } finally {
      setBusy(false)
    }
  }

  async function deleteForMe() {
    setBusy(true)
    try {
      await api.delete(`/api/messages/${message.id}?scope=me`)
      toast.success('Removed from this panel')
      onDeleteLocal(message)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Delete failed')
    } finally {
      setBusy(false)
    }
  }

  /* --------------------------------- deleted -------------------------------- */
  if (isDeleted) {
    return (
      <div className={cn('flex w-full', out ? 'justify-end' : 'justify-start')}>
        <div
          className={cn(
            'flex max-w-[85%] items-center gap-2 px-4 py-2.5 text-sm italic opacity-70 sm:max-w-[70%]',
            out ? 'bubble-out' : 'bubble-in',
          )}
        >
          <AlertCircle className="h-3.5 w-3.5" />
          {out ? 'You deleted this message' : 'This message was deleted'}
          <span className="text-[10px] not-italic opacity-80">{formatTime(message.timestamp)}</span>
        </div>
      </div>
    )
  }

  const hasMediaBlock =
    (message.type === 'image' || message.type === 'sticker' || message.type === 'video') && mediaUrl

  return (
    <div className={cn('group flex w-full', out ? 'justify-end' : 'justify-start')}>
      <div className="flex max-w-[88%] items-end gap-1 sm:max-w-[70%] md:max-w-[65%]">
        {/* hover actions (left side for outgoing) */}
        {out && (
          <BubbleActions
            message={message}
            isGroup={isGroup}
            busy={busy}
            hasText={Boolean(message.text)}
            hasMedia={Boolean(downloadUrl)}
            onReply={onReply}
            onReact={onReact}
            onReactOpen={() => setReactOpen((v) => !v)}
            onDeleteEveryone={deleteForEveryone}
            onDeleteMe={deleteForMe}
          />
        )}

        <div
          className={cn(
            'relative px-3 py-2 text-sm shadow-sm',
            out ? 'bubble-out' : 'bubble-in',
            reactOpen && 'ring-1 ring-[#25D366]/40',
          )}
        >
          {showSender && !out && message.senderName && (
            <div className="mb-1 text-[11px] font-semibold text-[#4ade80]">{message.senderName}</div>
          )}

          {/* quoted / reply context */}
          {message.quoted && (
            <div
              className={cn(
                'mb-1.5 flex flex-col gap-0.5 rounded-lg border-l-2 px-2.5 py-1.5 text-xs',
                out ? 'border-[#25D366] bg-black/20' : 'border-[#4ade80] bg-black/25',
              )}
            >
              <span className="font-medium text-[#4ade80]">
                {message.quoted.fromMe ? 'You' : (message.quoted.type === 'text' ? 'Reply' : typeLabel(message))}
              </span>
              <span className="line-clamp-2 opacity-85">{message.quoted.text}</span>
            </div>
          )}

          {/* view-once badge */}
          {message.viewOnce && (
            <div className="mb-1 inline-flex items-center gap-1 rounded-full bg-black/25 px-2 py-0.5 text-[10px] font-medium text-[#4ade80]">
              <Eye className="h-3 w-3" /> View once
            </div>
          )}

          {/* ------------------------------ MEDIA ------------------------------ */}
          {hasMediaBlock && (
            <div className="relative mb-1">
              {message.type === 'video' ? (
                <video
                  src={mediaUrl as string}
                  controls={revealed}
                  preload="metadata"
                  className={cn('max-h-80 w-full rounded-xl bg-black/40', !revealed && 'blur-2xl')}
                />
              ) : message.type === 'sticker' ? (
                // eslint-disable-next-line @next/next/no-img-element
                <img src={mediaUrl as string} alt="sticker" className="h-32 w-32 object-contain" loading="lazy" />
              ) : (
                <button type="button" onClick={() => revealed && onOpenMedia(message)} className="block">
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img
                    src={mediaUrl as string}
                    alt={message.text ?? 'image'}
                    className={cn(
                      'max-h-80 w-full rounded-xl object-cover transition-all duration-300',
                      !revealed && 'blur-2xl brightness-[0.6]',
                    )}
                    loading="lazy"
                  />
                </button>
              )}

              {!revealed && (
                <button
                  type="button"
                  onClick={() => setRevealed(true)}
                  className="absolute inset-0 flex flex-col items-center justify-center gap-1.5 rounded-xl bg-black/25 text-white backdrop-blur-[1px] transition hover:bg-black/35"
                >
                  <span className="flex h-11 w-11 items-center justify-center rounded-full bg-black/50">
                    <Eye className="h-5 w-5" />
                  </span>
                  <span className="text-xs font-medium">Tap to view once</span>
                </button>
              )}
            </div>
          )}

          {message.type === 'audio' && mediaUrl && (
            <AudioPlayer src={mediaUrl} duration={message.mediaDuration} out={out} />
          )}

          {message.type === 'document' && (
            <a
              href={downloadUrl ?? '#'}
              target="_blank"
              rel="noreferrer"
              className={cn(
                'mb-1 flex items-center gap-3 rounded-xl p-2 transition',
                out ? 'bg-white/10 hover:bg-white/15' : 'bg-black/25 hover:bg-black/35',
              )}
            >
              <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-white/10">
                <FileText className="h-4 w-4" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-xs font-medium">
                  {message.mediaName ?? 'Document'}
                </span>
                <span className="block text-[10px] opacity-70">
                  {formatBytes(message.mediaSize)} · tap to download
                </span>
              </span>
              <Download className="h-4 w-4 opacity-70" />
            </a>
          )}

          {/* media still not available from WhatsApp (typical for view-once) */}
          {mediaPending && (
            <div
              className={cn(
                'mb-1 flex items-center gap-2 rounded-xl px-3 py-2 text-xs',
                out ? 'bg-white/10' : 'bg-black/25',
              )}
            >
              {busy ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <RefreshCw className="h-3.5 w-3.5" />
              )}
              <span className="flex-1">
                {message.mediaStatus === 'failed'
                  ? 'Media unavailable'
                  : 'Waiting for WhatsApp to share the media…'}
              </span>
              {message.mediaStatus !== 'failed' && (
                <button
                  type="button"
                  onClick={() => void retryDownload()}
                  disabled={busy}
                  className="rounded-lg bg-black/30 px-2 py-1 text-[10px] font-medium transition hover:bg-black/45 disabled:opacity-50"
                >
                  Retry
                </button>
              )}
            </div>
          )}

          {message.type === 'other' && message.text?.startsWith('📍') && (
            <div className="mb-1 flex items-center gap-2">
              <MapPin className="h-4 w-4 text-[#4ade80]" />
            </div>
          )}

          {message.text && (
            <p className="whitespace-pre-wrap break-words leading-relaxed">{message.text}</p>
          )}

          {!message.text && !hasMediaBlock && !mediaPending && message.type !== 'audio' && (
            <p className="text-xs italic opacity-70">[{message.type} message]</p>
          )}

          {/* reactions */}
          {reactions.length > 0 && (
            <div className={cn('mt-1.5 flex flex-wrap gap-1', out ? 'justify-end' : 'justify-start')}>
              {reactions.map((emoji, index) => (
                <span
                  key={`${emoji}-${index}`}
                  className="rounded-full border border-white/15 bg-black/35 px-1.5 py-0.5 text-[11px] leading-none"
                >
                  {emoji}
                </span>
              ))}
            </div>
          )}

          {/* quick reaction row */}
          {reactOpen && (
            <div className="absolute -top-11 left-0 z-20 flex items-center gap-1 rounded-full border border-white/[0.08] bg-[hsl(200_24%_7%)] px-2 py-1 shadow-xl">
              {QUICK_REACTIONS.map((emoji) => (
                <button
                  key={emoji}
                  type="button"
                  className="rounded-full px-1 text-base transition hover:scale-125"
                  onClick={() => {
                    onReact(message, emoji)
                    setReactOpen(false)
                  }}
                >
                  {emoji}
                </button>
              ))}
              <button
                type="button"
                className="rounded-full p-1 text-muted-foreground hover:text-foreground"
                onClick={() => setReactOpen(false)}
              >
                <X className="h-3 w-3" />
              </button>
            </div>
          )}

          <div
            className={cn(
              'mt-1 flex items-center justify-end gap-1 text-[10px]',
              out ? 'text-white/70' : 'text-white/50',
            )}
          >
            <span>{formatTime(message.timestamp)}</span>
            {out && <StatusTicks status={message.status} />}
          </div>
        </div>

        {/* hover actions (right side for incoming) */}
        {!out && (
          <BubbleActions
            message={message}
            isGroup={isGroup}
            busy={busy}
            hasText={Boolean(message.text)}
            hasMedia={Boolean(downloadUrl)}
            onReply={onReply}
            onReact={onReact}
            onReactOpen={() => setReactOpen((v) => !v)}
            onDeleteEveryone={deleteForEveryone}
            onDeleteMe={deleteForMe}
          />
        )}
      </div>
    </div>
  )
}

function BubbleActions({
  message,
  isGroup,
  busy,
  hasText,
  hasMedia,
  onReply,
  onReact,
  onReactOpen,
  onDeleteEveryone,
  onDeleteMe,
}: {
  message: MessageDTO
  isGroup: boolean
  busy: boolean
  hasText: boolean
  hasMedia: boolean
  onReply: (message: MessageDTO) => void
  onReact: (message: MessageDTO, emoji: string) => void
  onReactOpen: () => void
  onDeleteEveryone: () => void
  onDeleteMe: () => void
}) {
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button
          variant="ghost"
          size="iconSm"
          aria-label="Message actions"
          className="h-7 w-7 shrink-0 opacity-0 transition-opacity group-hover:opacity-100 focus-visible:opacity-100 data-[state=open]:opacity-100 max-md:opacity-60"
        >
          <MoreVertical className="h-3.5 w-3.5" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align={message.fromMe ? 'end' : 'start'} side="top">
        <DropdownMenuItem onSelect={() => onReply(message)}>
          <CornerUpLeft className="h-4 w-4" /> Reply
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => onReactOpen()}>
          <SmilePlus className="h-4 w-4" /> React
        </DropdownMenuItem>
        {hasText && (
          <DropdownMenuItem
            onSelect={async () => {
              await navigator.clipboard.writeText(message.text ?? '')
              toast.success('Text copied')
            }}
          >
            <Copy className="h-4 w-4" /> Copy text
          </DropdownMenuItem>
        )}
        {hasMedia && (
          <DropdownMenuItem asChild>
            <a href={`/api/media?id=${message.id}&download=1`} target="_blank" rel="noreferrer">
              <Download className="h-4 w-4" /> Download media
            </a>
          </DropdownMenuItem>
        )}
        <DropdownMenuSeparator />
        {message.fromMe && (
          <DropdownMenuItem
            className="text-red-300 focus:text-red-200"
            onSelect={() => void onDeleteEveryone()}
            disabled={busy}
          >
            <Trash2 className="h-4 w-4" /> Delete for everyone
          </DropdownMenuItem>
        )}
        <DropdownMenuItem
          className="text-red-300 focus:text-red-200"
          onSelect={() => void onDeleteMe()}
          disabled={busy}
        >
          <Trash2 className="h-4 w-4" /> Delete for me
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}
