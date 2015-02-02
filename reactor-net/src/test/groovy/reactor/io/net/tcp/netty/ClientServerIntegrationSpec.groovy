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
import reactor.fn.Consumer
import reactor.io.codec.LengthFieldCodec
import reactor.io.codec.json.JsonCodec
import reactor.io.net.Channel
import reactor.io.net.Spec
import reactor.io.net.netty.tcp.NettyTcpClient
import reactor.io.net.netty.tcp.NettyTcpServer
import reactor.io.net.tcp.support.SocketUtils
import spock.lang.Ignore
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertNotNull

@Ignore
class ClientServerIntegrationSpec extends Specification {

	Environment env1
	Environment env2

	def setup() {
		env1 = new Environment()
		env2 = new Environment()
	}

	@Unroll
	def "Client should be able to send data to server"(List<Pojo> data) {
		given: "a TcpServer and TcpClient with JSON defaultCodec"
			def dataLatch = new CountDownLatch(data.size())

			final int port = SocketUtils.findAvailableTcpPort()

			def consumerMock = Mock(Consumer) { data.size() * accept(_) }
			def codec = new LengthFieldCodec(new JsonCodec(Pojo))

			def server = new Spec.TcpServer<Pojo, Pojo>(NettyTcpServer).
					env(env1).dispatcher("sync").
					listen(port).
					codec(codec).
					consume({ conn ->
						conn.consume({ pojo ->
							dataLatch.countDown()
							consumerMock.accept(pojo)
						} as Consumer<Pojo>)
					} as Consumer<Channel<Pojo, Pojo>>).
					get()

			def client = new Spec.TcpClient<Pojo, Pojo>(NettyTcpClient).
					env(env2).dispatcher("sync").
					codec(codec).
					connect("localhost", port).
					get()

		when: 'the server is started'
			server.start().await(5, TimeUnit.SECONDS)

		and: "connection is established"
			def connection = client.open().await(5, TimeUnit.SECONDS)
			assertNotNull("Connection made successfully", connection)

		and: "pojo is written"
			data.each { Pojo item -> connection.sendAndForget(item) }
			dataLatch.await(30, TimeUnit.SECONDS)

		then: "everything went fine"
			dataLatch.count == 0
			client.close().await(5, TimeUnit.SECONDS)
			server.shutdown().await(5, TimeUnit.SECONDS)

		where:
			data << [
					[new Pojo('John')],
					[new Pojo('John'), new Pojo("Jane")],
					[new Pojo('John'), new Pojo("Jane"), new Pojo("Blah")],
					(1..10).collect { new Pojo("Value_$it") }.toList(),
			]
	}

	@Unroll
	def "Server should be able to send POJO to client"(List<Pojo> data) {
		given: "a TcpServer and TcpClient with JSON defaultCodec"
			def dataLatch = new CountDownLatch(data.size())

			final int port = SocketUtils.findAvailableTcpPort()

			def consumerMock = Mock(Consumer) { data.size() * accept(_) }
			def codec = new LengthFieldCodec(new JsonCodec(Pojo))

			def server = new Spec.TcpServer<Pojo, Pojo>(NettyTcpServer).
					env(env1).dispatcher("sync").
					listen(port).
					codec(codec).
					consume({ conn -> data.each { pojo -> conn.sendAndForget(pojo) } } as Consumer).
					get()

			def client = new Spec.TcpClient<Pojo, Pojo>(NettyTcpClient).
					env(env2).dispatcher("sync").
					codec(codec).
					connect("localhost", port).
					get()

		when: 'the server is started'
			server.start().await(5, TimeUnit.SECONDS)

		and: "connection is established"
			client.open().
					consume({ Channel conn ->
						conn.consume({ Pojo pojo ->
							dataLatch.countDown()
							consumerMock.accept(pojo)
						} as Consumer)
					} as Consumer).
					await(1, TimeUnit.SECONDS)

		and: "data is being sent"
			dataLatch.await(30, TimeUnit.SECONDS)

		then: "everything went fine"
			dataLatch.count == 0
			client.close().await(5, TimeUnit.SECONDS)
			server.shutdown().await(5, TimeUnit.SECONDS)

		where:
			data << [
					[new Pojo('John')],
					[new Pojo('John'), new Pojo("Jane")],
					[new Pojo('John'), new Pojo("Jane"), new Pojo("Blah")],
					(1..10).collect { new Pojo("Value_$it") }.toList(),
			]
	}

	static class Pojo {
		public Pojo() {}

		public Pojo(String name) { this.name = name }
		String name
	}

}
