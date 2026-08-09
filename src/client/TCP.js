import { TCPHelper, InstanceStatus } from '@companion-module/base'
import { parseResponse } from './parseResponse.js'
import { Priority } from '../utility/constants.js'
import { getContext } from '../context.js'
import { createLogger, formatHex } from '../utility/log.js'

const log = createLogger('TCP')
const ENABLE_TCP_LOGGING = false
const TIME_BETW_MULTIPLE_REQ_MS = 150
// TCPHelper turns keepalive on but leaves the idle time at the OS default of 2 h, so an idle
// connection to a dead peer only fails after that long
const KEEPALIVE_IDLE_MS = 15000

/**
 * TCP Client factory function. Creates TCP client and handles request queueing, sending, and receiving.
 * @returns {{destroy: Function, init: Function, queue: Function, clearQueue: Function, waitUntilIdle: Function, isConnected: Function, onConnected: Function, onDisconnect: Function}} Returns helper functions
 */
export function TCPClient() {
	let midiSocket
	let txQueue = []
	let queueRunning = false
	let isConnected = false
	let onConnectedCallback
	let onDisconnectCallback
	let cancelSleep = null
	let idleWaiters = []

	/**
	 * Resolves all callers waiting for the transmit queue to become idle.
	 * @returns {void}
	 */
	function notifyIdle() {
		if (queueRunning || txQueue.length > 0) return

		for (const resolve of idleWaiters) {
			resolve()
		}
		idleWaiters = []
	}

	/**
	 * Removes all pending transmit requests from the queue.
	 * @returns {void}
	 */
	function clearQueue() {
		const count = txQueue.length
		txQueue = []
		if (count > 0) {
			log.debug('Cleared queued requests', { count })
		}
		notifyIdle()
	}

	/**
	 * Destroys the TCP socket and clears pending transmit requests.
	 * @returns {void}
	 */
	function destroy() {
		clearQueue()

		if (!midiSocket) return

		midiSocket.destroy()
		midiSocket = undefined
		isConnected = false
	}

	/**
	 * Marks the connection as lost, clears pending requests, and notifies the disconnect callback.
	 * Only the first call after a connect does anything, because TCPHelper keeps emitting 'error'
	 * while its reconnect attempts fail.
	 * @param {string} host
	 * @param {number} port
	 * @returns {void}
	 */
	function handleDisconnect(host, port) {
		if (!isConnected) return

		isConnected = false
		clearQueue()
		log.info('Disconnected', { host, port })
		if (onDisconnectCallback) {
			onDisconnectCallback()
		}
	}

	/**
	 * Creates a new TCP socket connection.
	 * @param {string} host
	 * @param {number} port
	 * @returns {void}
	 */
	function init(host, port) {
		destroy()

		if (!host || !port) return

		const { companion } = getContext()

		midiSocket = new TCPHelper(host, port)

		midiSocket._socket.setKeepAlive(true, KEEPALIVE_IDLE_MS)

		midiSocket.on('status_change', (status, message) => {
			log.debug('StatusChanged', { status, message: message ?? '' })
			companion.updateStatus(status)
		})

		// TCPHelper has no 'close' event. A lost connection shows up as 'end' or 'error'.
		midiSocket.on('end', () => handleDisconnect(host, port))

		midiSocket.on('error', (err) => {
			log.error('SocketError', { message: err.message })
			companion.updateStatus(InstanceStatus.ConnectionFailure)
			handleDisconnect(host, port)
		})

		midiSocket.on('data', (data) => {
			if (ENABLE_TCP_LOGGING) console.log(`[AHM][TCP][Raw] Received Hex=${formatHex(data)}`)
			parseResponse(data)
		})

		midiSocket.on('connect', () => {
			log.info('Connected', { host, port })
			companion.updateStatus(InstanceStatus.Ok)
			isConnected = true

			if (onConnectedCallback) {
				onConnectedCallback()
			}
		})
	}

	/**
	 * Registers a callback for successful TCP connection.
	 * @param {Function} cb
	 * @returns {void}
	 */
	function onConnected(cb) {
		onConnectedCallback = cb
	}

	/**
	 * Registers a callback for TCP disconnect.
	 * @param {Function} cb
	 * @returns {void}
	 */
	function onDisconnect(cb) {
		onDisconnectCallback = cb
	}

	/**
	 * Queues MIDI packet for sending
	 * @param {Buffer[]} buffers
	 * @param {Priority} priority - defaults to high unless specified
	 * @returns {void}
	 */
	function queue(buffers, priority = Priority.HIGH) {
		// Drop requests while disconnected, otherwise they are sent late, after the reconnect
		if (!isConnected) return

		if (priority === Priority.HIGH) {
			txQueue.unshift({ buffers, priority, timestamp: Date.now() })
			if (cancelSleep) {
				cancelSleep()
				cancelSleep = null
			}
		} else {
			txQueue.push({ buffers, priority, timestamp: Date.now() })
		}
		startQueue()
	}

	/**
	 * Resolves once the transmit queue has finished sending all pending requests.
	 * @returns {Promise<void>}
	 */
	function waitUntilIdle() {
		if (!queueRunning && txQueue.length === 0) {
			return Promise.resolve()
		}

		return new Promise((resolve) => {
			idleWaiters.push(resolve)
		})
	}

	/**
	 * Sends queued packets one at a time with a fixed delay between requests.
	 * @returns {Promise<void>}
	 */
	async function startQueue() {
		if (!isConnected) return

		// if queue is already running, let it be
		if (queueRunning) return
		queueRunning = true

		while (txQueue.length > 0) {
			const txBuffer = txQueue.shift()
			if (!txBuffer) continue

			try {
				send(txBuffer.buffers)
			} catch (e) {
				log.error('SendFailed', { message: e.message ?? e })
			}
			if (ENABLE_TCP_LOGGING) console.log(`[AHM][TCP][Queue] Progress remaining=${txQueue.length}`)

			await new Promise((resolve) => {
				cancelSleep = resolve
				setTimeout(resolve, TIME_BETW_MULTIPLE_REQ_MS)
			})
			cancelSleep = null
		}

		queueRunning = false
		notifyIdle()
	}

	/**
	 * Sends all buffers belonging to one queued MIDI request.
	 * @param {Buffer[]} buffers
	 * @returns {void}
	 */
	function send(buffers) {
		if (!isConnected) return

		if (buffers.length !== 0) {
			for (let i = 0; i < buffers.length; i++) {
				if (!midiSocket) return
				if (ENABLE_TCP_LOGGING) console.log(`[AHM][TCP][Raw] Sent Hex=${formatHex(buffers[i])}`)
				midiSocket.send(buffers[i])
			}
		}
	}

	return {
		destroy,
		init,
		queue,
		clearQueue,
		waitUntilIdle,
		isConnected: () => isConnected,
		onConnected,
		onDisconnect,
	}
}
