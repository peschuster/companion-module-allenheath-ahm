import { createLogger } from '../utility/log.js'

const log = createLogger('FrameResponses')

const SYSEX_START = 0xf0
const SYSEX_END = 0xf7
const MIDI_STATUS_BIT = 0x80

const MUTE_STATUS = new Set([0x90, 0x91, 0x92])
const LEVEL_STATUS = new Set([0xb0, 0xb1, 0xb2])

const NRPN_PARAM_MSB = 0x63
const NRPN_PARAM_LSB = 0x62
const BANK_SELECT_MSB = 0x00
const PROGRAM_CHANGE = 0xc0

// messageLength() results that are not a byte count
const INCOMPLETE = -1
const UNKNOWN = -2

// Guards against unbounded growth if the stream desyncs inside a SysEx message
const MAX_BUFFER_BYTES = 4096

const EMPTY = Buffer.alloc(0)

/**
 * Reports whether a byte can start one of the AHM responses we know how to frame.
 * @param {number} byte
 * @returns {boolean}
 */
function isMessageStart(byte) {
	return byte === SYSEX_START || MUTE_STATUS.has(byte) || LEVEL_STATUS.has(byte)
}

/**
 * Measures the response starting at the given offset.
 *
 * The AHM answers with these shapes. It sends mutes and levels with MIDI running status, so they
 * are shorter than the forms in support/AHM-TCP-Protocol-V1.5.pdf:
 *
 *   Send level / send mute   F0 .. F7                     variable, terminated by F7
 *   Channel mute             9n CH VV CH 00               5, note-on plus running-status note-off
 *   Channel level (NRPN)     Bn 63 CH 62 17 06 LV         7, three CCs under running status
 *   Preset recall            B0 00 BK C0 PR               5, bank select plus program change
 *
 * The documented forms repeat the status byte (6 and 9 bytes) and are accepted too.
 *
 * @param {Buffer} data
 * @param {number} start - Offset of the status byte
 * @returns {number} Byte count, or INCOMPLETE when more bytes are needed, or UNKNOWN
 */
function messageLength(data, start) {
	const status = data[start]
	const available = data.length - start

	if (status === SYSEX_START) {
		const end = data.indexOf(SYSEX_END, start + 1)
		return end === -1 ? INCOMPLETE : end - start + 1
	}

	// Byte 3 tells the forms apart. A data byte means running status, the same status byte again
	// means the documented form, and any other status byte means a note-on without its note-off.
	if (MUTE_STATUS.has(status)) {
		if (available < 4) return INCOMPLETE
		if (data[start + 3] === status) return 6
		if (data[start + 3] < MIDI_STATUS_BIT) return 5
		return 3
	}

	if (LEVEL_STATUS.has(status)) {
		if (available < 4) return INCOMPLETE

		if (data[start + 1] === NRPN_PARAM_MSB) {
			if (data[start + 3] === NRPN_PARAM_LSB) return 7
			if (data[start + 3] === status) return 9
			return UNKNOWN
		}

		if (data[start + 1] === BANK_SELECT_MSB && data[start + 3] === PROGRAM_CHANGE) return 5

		return UNKNOWN
	}

	return UNKNOWN
}

/**
 * Finds the next byte that could start a response, so a desynced stream can resync.
 * @param {Buffer} data
 * @param {number} from
 * @returns {number} Offset of the next possible status byte, or the buffer length
 */
function nextMessageStart(data, from) {
	for (let offset = from; offset < data.length; offset++) {
		if (isMessageStart(data[offset])) return offset
	}

	return data.length
}

/**
 * Response framer factory function. TCP is a byte stream, so a read can hold part of a response
 * or several of them. The framer cuts the stream into whole responses and keeps a trailing
 * fragment for the next read.
 * @returns {{push: Function, reset: Function}}
 */
export function responseFramer() {
	let buffer = EMPTY

	/**
	 * Drops any buffered fragment.
	 * @returns {void}
	 */
	function reset() {
		if (buffer.length > 0) {
			log.debug('DiscardedFragment', { bytes: buffer.length })
		}
		buffer = EMPTY
	}

	/**
	 * Adds a chunk of received bytes and returns every response completed by it.
	 * @param {Buffer} chunk
	 * @returns {Buffer[]} Complete responses, in arrival order
	 */
	function push(chunk) {
		buffer = buffer.length > 0 ? Buffer.concat([buffer, chunk]) : chunk

		const messages = []
		let offset = 0

		while (offset < buffer.length) {
			const length = messageLength(buffer, offset)

			if (length === INCOMPLETE) break

			if (length === UNKNOWN) {
				const resyncAt = nextMessageStart(buffer, offset + 1)
				log.warn('UnframedBytes', { reason: 'UnknownStatusByte' }, buffer.subarray(offset, resyncAt))
				offset = resyncAt
				continue
			}

			// The shape is known but the tail has not arrived yet
			if (offset + length > buffer.length) break

			messages.push(buffer.subarray(offset, offset + length))
			offset += length
		}

		buffer = offset >= buffer.length ? EMPTY : buffer.subarray(offset)

		if (buffer.length > MAX_BUFFER_BYTES) {
			log.warn('UnframedBytes', { reason: 'BufferOverflow', bytes: buffer.length })
			buffer = EMPTY
		}

		return messages
	}

	return { push, reset }
}
