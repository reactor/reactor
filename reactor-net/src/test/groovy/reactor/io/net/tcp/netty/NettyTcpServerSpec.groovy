/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.net.tcp.netty

import reactor.Environment
import reactor.io.buffer.Buffer
import reactor.io.codec.PassThroughCodec
import reactor.io.codec.json.JsonCodec
import reactor.io.net.NetStreams
import reactor.rx.Streams
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class NettyTcpServerSpec extends Specification {

	static final int port = 26874
	Environment env

	def setup() {
		env = Environment.initializeIfEmpty()
	}

	def "NettyTcpServer responds to requests from clients"() {
		given: "a simple TcpServer"
			def stopLatch = new CountDownLatch(1)
			def dataLatch = new CountDownLatch(1)
			def server = NetStreams.<Buffer, Buffer> tcpServer {
				it.
						env(env).
						listen(port).
						codec(new PassThroughCodec<Buffer>())
			}

		when: "the server is started"
			server.consume { conn ->
				conn.sinkBuffers(Streams.just(Buffer.wrap("Hello World!")))
			}

		then: "the server was started"
			server.start().awaitSuccess()

		when: "data is sent"
			def client = new SimpleClient(port, dataLatch, Buffer.wrap("Hello World!"))
			client.start()
			dataLatch.await(5, TimeUnit.SECONDS)

		then: "data was recieved"
			client.data?.remaining() == 12
			new Buffer(client.data).asString() == "Hello World!"
			dataLatch.count == 0

		cleanup: "the server is stopped"
			server.shutdown().onSuccess{
				stopLatch.countDown()
			}
			stopLatch.await(5, TimeUnit.SECONDS)

	}

	def "NettyTcpServer can encode and decode JSON"() {
		given: "a TcpServer with JSON defaultCodec"
			def stopLatch = new CountDownLatch(1)
			def dataLatch = new CountDownLatch(1)
			def server = NetStreams.<Pojo, Pojo> tcpServer {
				it.env(env).
						listen(port).
						codec(new JsonCodec<Pojo, Pojo>(Pojo))
			}

		when: "the server is started"
			server.pipeline { conn ->
				conn.map { pojo ->
					assert pojo.name == "John Doe"
					new Pojo(name: "Jane Doe")
				}
			}

		then: "the server was started"
			server.start().awaitSuccess(50, TimeUnit.SECONDS)

		when: "a pojo is written"
			def client = new SimpleClient(port, dataLatch, Buffer.wrap("{\"name\":\"John Doe\"}"))
			client.start()
			dataLatch.await(5, TimeUnit.SECONDS)

		then: "data was recieved"
			client.data?.remaining() == 19
			new Buffer(client.data).asString() == "{\"name\":\"Jane Doe\"}"
			dataLatch.count == 0

		cleanup: "the server is stopped"
			server.shutdown().onSuccess{
				stopLatch.countDown()
			}
			stopLatch.await(5, TimeUnit.SECONDS)
	}

	def "flush every 5 elems"() {
		given: "a TcpServer with JSON defaultCodec"

			def latch = new CountDownLatch(10)
			def server = NetStreams.<Pojo, Pojo> tcpServer {
				it.env(env).
						listen(port).
						codec(new JsonCodec<Pojo, Pojo>(Pojo))
			}

			def client = NetStreams.<Pojo, Pojo> tcpClient {
				it.env(env).
						connect("localhost", port).
						codec(new JsonCodec<Pojo, Pojo>(Pojo))
			}

		when: "the client/server is prepared"
			server.pipeline { input ->
				input
						.log('serve')
						.capacity(5l)
			}

			client.pipeline { input ->
				input
						.log('receive')
						.consume { latch.countDown() }

				Streams.range(1, 10)
						.map { new Pojo(name: 'test' + it) }
						.log('send')
			}

		then: "the server was started"
			server.start().flatMap { client.open() }.awaitSuccess(5, TimeUnit.SECONDS)
			latch.await(10, TimeUnit.SECONDS)


		cleanup: "the server is stopped"
			client.close().flatMap { server.shutdown() }.awaitSuccess(5, TimeUnit.SECONDS)
	}

	static class SimpleClient extends Thread {
		final int port
		final CountDownLatch latch
		final Buffer output
		ByteBuffer data

		SimpleClient(int port, CountDownLatch latch, Buffer output) {
			this.port = port
			this.latch = latch
			this.output = output
		}

		@Override
		void run() {
			def ch = SocketChannel.open(new InetSocketAddress(port))
			def len = ch.write(output.byteBuffer())
			assert ch.connected
			data = ByteBuffer.allocate(len)
			int read = ch.read(data)
			assert read > 0
			data.flip()
			latch.countDown()
		}
	}

	static class Pojo {
		String name

		@Override
		String toString() {
			name
		}
	}

}
