/*
 * Copyright 2002-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.tcp;

import reactor.tcp.codec.Codec;
import reactor.tcp.data.Buffers;
import reactor.util.Assert;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLEngineResult.Status;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Implementation of {@link TcpConnection} supporting SSL/TLS over NIO. Unlike TcpNetConnection, which uses Sockets, the
 * JVM does not directly support SSL for SocketChannels, used by NIO. Instead, the SSLEngine is provided whereby the SSL
 * encryption is performed by passing in a plain text buffer, and receiving an encrypted buffer to transmit over the
 * network. Similarly, encrypted data read from the network is decrypted.<p> However, before this can be done, certain
 * handshaking operations are required, involving the creation of data buffers which must be exchanged by the peers. A
 * number of such transfers are required; once the handshake is finished, it is relatively simple to encrypt/decrypt the
 * data.<p> Also, it may be deemed necessary to re-perform handshaking.<p> This class supports the management of
 * handshaking as necessary, both from the initiating and receiving peers.
 *
 * @author Gary Russell
 */
public class TcpNioSSLConnection<T> extends TcpNioConnection<T> {

	private final SSLEngine sslEngine;

	private volatile ByteBuffer decoded;

	private volatile ByteBuffer encoded;

	private boolean needMoreNetworkData;

	private final Buffers encodedBuffers = new Buffers();

	private final BlockingQueue<Buffers> encodedBuffersToWrite = new LinkedBlockingQueue<Buffers>();

	private volatile boolean writeBlocked;

	private volatile SSLEngineResult currentResult;

	private volatile boolean pendingSend;

	private volatile boolean needHandshake = true;

	public TcpNioSSLConnection(SocketChannel socketChannel, boolean server, boolean lookupHost, ConnectionFactorySupport<T> connectionFactory,
														 SSLEngine sslEngine, Codec<T> codec) throws Exception {
		super(socketChannel, server, lookupHost, connectionFactory, codec);
		this.sslEngine = sslEngine;
	}

	/**
	 * Overrides super class method to perform decryption and/or participate in handshaking. Decrypted data is sent to the
	 * super class to be assembled into a Message. Data received from the network may constitute multiple SSL packets, and
	 * may end with a partial packet. In that case, the buffer is compacted, ready to receive the remainder of the packet.
	 */
	@Override
	protected void fireNewDataEvent(final ByteBuffer networkBuffer) throws IOException {
		Assert.notNull(networkBuffer, "rawBuffer cannot be null");
		if (logger.isDebugEnabled()) {
			logger.debug("New Data " + sslEngine.getHandshakeStatus() + ", remaining:" + networkBuffer.remaining());
		}
		SSLEngineResult result = null;
		this.needMoreNetworkData = false;
		while (!this.needMoreNetworkData) {
			result = decode(networkBuffer);
			if (logger.isDebugEnabled()) {
				logger.debug("result " + resultToString(result) + ", remaining:" + networkBuffer.remaining());
			}
		}
		if (result.getStatus() == Status.BUFFER_UNDERFLOW) {
			networkBuffer.compact();
		} else {
			networkBuffer.clear();
		}
		if (logger.isDebugEnabled()) {
			logger.debug("New data.xit " + resultToString(result) + ", remaining:" + networkBuffer.remaining());
		}
	}

	/**
	 * Performs the actual decryption of a received packet - which may be real data, or handshaking data. Appropriate
	 * action is taken with the data. If this side did not initiate the handshake, any handshaking data sent out is handled
	 * by the thread running in the {@link SSLChannelOutputStream#doWrite(ByteBuffer)} method, which is awoken here, as a
	 * result of reaching that stage in the handshaking.
	 */
	@SuppressWarnings("fallthrough")
	private SSLEngineResult decode(ByteBuffer networkBuffer) throws IOException {
		HandshakeStatus handshakeStatus = this.sslEngine.getHandshakeStatus();
		SSLEngineResult result = new SSLEngineResult(Status.OK, handshakeStatus, 0, 0);
		if (handshakeStatus == HandshakeStatus.NEED_UNWRAP && this.needMoreNetworkData) {
			handshakeStatus = this.encodeIfNeeded(networkBuffer, handshakeStatus);
			result = new SSLEngineResult(Status.OK, handshakeStatus, 0, 0);
			if (this.doClientSideHandshake(networkBuffer, result)) {
				return this.currentResult;
			}
		}

		switch (handshakeStatus) {
			case NEED_TASK:
				runTasks();
				break;
			case NEED_UNWRAP:
			case FINISHED:
			case NOT_HANDSHAKING:
				this.decoded.clear();
				result = this.sslEngine.unwrap(networkBuffer, this.decoded);
				if (logger.isDebugEnabled()) {
					logger.debug("After unwrap:" + resultToString(result));
				}
				Status status = result.getStatus();
				if (status == Status.BUFFER_OVERFLOW) {
					this.decoded = this.allocateEncryptionBuffer(this.sslEngine.getSession().getApplicationBufferSize());
				}
				if (result.bytesProduced() > 0) {
					this.decoded.flip();
					byte[] buf = new byte[this.decoded.remaining()];
					ByteBuffer copy = ByteBuffer.wrap(buf);
					this.decoded.get(buf);
					super.fireNewDataEvent(copy);
				}
				break;
			case NEED_WRAP:
				this.encoded.clear();
				result = this.sslEngine.wrap(networkBuffer, this.encoded);
				if (logger.isDebugEnabled()) {
					logger.debug("After wrap:" + resultToString(result));
				}
				if (result.getStatus() == Status.BUFFER_OVERFLOW) {
					this.encoded = this.allocateEncryptionBuffer(this.sslEngine.getSession().getPacketBufferSize());
				} else {
					this.writeEncodedIfAny();
				}
				break;
			default:
		}
		switch (result.getHandshakeStatus()) {
			case FINISHED:
			case NOT_HANDSHAKING:
			case NEED_UNWRAP:
				this.needMoreNetworkData = result.getStatus() == Status.BUFFER_UNDERFLOW || networkBuffer.remaining() == 0;
				break;
			default:
		}
		if (result.getHandshakeStatus() == HandshakeStatus.FINISHED) {
			this.needHandshake = false;
			if (this.pendingSend) {
				super.retrySend();
			}
		}
		return result;
	}

	/**
	 * Part of the SSLEngine handshaking protocol required at various stages. Tasks are run on the current thread.
	 */
	private void runTasks() {
		Runnable task;
		while ((task = this.sslEngine.getDelegatedTask()) != null) {
			task.run();
		}
	}

	/**
	 * Determines whether {@link #runTasks()} is needed and invokes if so.
	 */
	private HandshakeStatus runTasksIfNeeded(SSLEngineResult result) throws IOException {
		if (result != null) {
			if (logger.isDebugEnabled()) {
				logger.debug("Running tasks if needed " + resultToString(result));
			}
			if (result.getHandshakeStatus() == HandshakeStatus.NEED_TASK) {
				runTasks();
			}
		}
		HandshakeStatus handshakeStatus = sslEngine.getHandshakeStatus();
		if (logger.isDebugEnabled()) {
			logger.debug("New handshake status " + handshakeStatus);
		}
		return handshakeStatus;
	}

	/**
	 * Initializes the SSLEngine and sets up the encryption/decryption buffers.
	 */
	public void init() throws IOException {
		if (this.decoded == null) {
			this.decoded = allocateEncryptionBuffer(2048);
			this.encoded = allocateEncryptionBuffer(2048);
			this.initilizeEngine();
		}
	}

	private ByteBuffer allocateEncryptionBuffer(int size) {
		return ByteBuffer.allocate(size);
	}

	private void initilizeEngine() throws IOException {
		boolean client = !this.isServer();
		this.sslEngine.setUseClientMode(client);
	}

	@Override
	protected synchronized void doWrite(BlockingQueue<Buffers> plainTextBuffers) throws IOException {
		boolean workToDo = true;
		while (workToDo) {
			if (this.writeBlocked) {
				this.writeEncodedIfAny();
				if (this.encoded.remaining() > 0) {
					if (logger.isDebugEnabled()) {
						logger.debug("Still blocked");
					}
					break;
				} else {
					if (this.sslEngine.getHandshakeStatus() != HandshakeStatus.FINISHED) {
						if (logger.isDebugEnabled()) {
							logger.debug("Still handshaking");
						}
						break;
					}
				}
			}
			if (this.needHandshake) {
				doWrite(ByteBuffer.wrap(new byte[0]));
				workToDo = false;
				pendingSend = true;
			} else {
				Buffers buffers = plainTextBuffers.poll();
				if (buffers == null) {
					workToDo = false;
				} else {
					int consumedBuffers = 0;
					int bytesWritten = 0;
					for (ByteBuffer buffer : buffers) {
						bytesWritten += buffer.remaining();
						doWrite(buffer);
						if (buffer.remaining() > 0) {
							bytesWritten -= buffer.remaining();
							workToDo = false;
							break;
						} else {
							consumedBuffers++;
						}
						if (this.writeBlocked) {
							workToDo = false;
						}
					}
					if (logger.isTraceEnabled()) {
						logger.trace(this.getConnectionId() + " Written " + bytesWritten);
					}
					buffers.discardBuffers(consumedBuffers);
				}
			}
		}
	}

	/**
	 * Encrypts the plaintText buffer and writes it to the SocketChannel. Will participate in SSL handshaking as necessary.
	 * For very large data, the SSL packets will be limited by the engine's buffer sizes and multiple writes will be
	 * necessary.
	 */
	protected synchronized void doWrite(ByteBuffer plainText)
			throws IOException {
		int remaining = plainText.remaining();
		do {
			SSLEngineResult result = encode(plainText);
			if (logger.isDebugEnabled()) {
				logger.debug("doWrite: " + resultToString(result));
			}
			if (result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
				writeEncodedIfAny();
				if (plainText.remaining() >= remaining) {
					throw new RuntimeException(
							"Unexpected condition - SSL wrap did not consume any data; remaining = "
									+ remaining);
				}
				remaining = plainText.remaining();
			} else {
				if (doClientSideHandshake(plainText, result)) {
					return;
				}
				writeEncodedIfAny();
			}
		} while (remaining > 0);
	}

	private String resultToString(SSLEngineResult result) {
		return result.toString().replace('\n', ' ');
	}

	/**
	 * Handles SSL handshaking; when network data is needed from the peer, suspends until that data is received.
	 */
	private boolean doClientSideHandshake(ByteBuffer plainText,
																				SSLEngineResult result) throws IOException {
		HandshakeStatus status = TcpNioSSLConnection.this.sslEngine.getHandshakeStatus();
		this.needMoreNetworkData = false;
		while (status != HandshakeStatus.FINISHED) {
			writeEncodedIfAny();
			status = runTasksIfNeeded(result);
			if (status == HandshakeStatus.NEED_UNWRAP) {
				if (logger.isDebugEnabled()) {
					logger.debug("Need more network data");
				}
				this.needMoreNetworkData = true;
				return true;
			}
			status = this.encodeIfNeeded(plainText, status);
		}
		return false;
	}

	private HandshakeStatus encodeIfNeeded(ByteBuffer plainText, final HandshakeStatus status) throws IOException {
		if (status == HandshakeStatus.NEED_WRAP ||
				status == HandshakeStatus.NOT_HANDSHAKING ||
				status == HandshakeStatus.FINISHED) {
			SSLEngineResult result = encode(plainText);
			this.currentResult = result;
			HandshakeStatus newStatus = result.getHandshakeStatus();
			if (newStatus == HandshakeStatus.NOT_HANDSHAKING ||
					newStatus == HandshakeStatus.FINISHED) {
				return newStatus;
			}
		} else {
			if (logger.isDebugEnabled()) {
				logger.debug(status.toString());
			}
		}
		return status;
	}

	private void writeEncodedIfAny() throws IOException {
		this.encoded.flip();
		int remaining = this.encoded.remaining();
		if (remaining > 0) {
			this.encodedBuffers.add(encoded);
			this.encodedBuffersToWrite.add(this.encodedBuffers);
			super.doWrite(this.encodedBuffersToWrite);
			if (this.encoded.remaining() > 0) {
				this.writeBlocked = true;
				this.encodedBuffers.discardBytes(remaining - this.encoded.remaining());
			} else {
				this.encoded.clear();
				if (this.encodedBuffers.getBufferCount() > 0) {
					this.encodedBuffers.discardBuffers(1);
				}
			}
		}
	}

	/**
	 * Encrypts plain text data. The result may indicate handshaking is needed.
	 */
	private SSLEngineResult encode(ByteBuffer plainText)
			throws SSLException, IOException {
		TcpNioSSLConnection.this.encoded.clear();
		SSLEngineResult result = TcpNioSSLConnection.this.sslEngine.wrap(plainText, TcpNioSSLConnection.this.encoded);
		if (logger.isDebugEnabled()) {
			logger.debug("After wrap:" + resultToString(result) + " Plaintext buffer @" + plainText.position() + "/" + plainText.limit());
		}
		if (result.getStatus() == SSLEngineResult.Status.BUFFER_OVERFLOW) {
			TcpNioSSLConnection.this.encoded = allocateEncryptionBuffer(sslEngine.getSession().getPacketBufferSize());
			result = TcpNioSSLConnection.this.sslEngine.wrap(plainText, TcpNioSSLConnection.this.encoded);
		}
		return result;
	}

}
