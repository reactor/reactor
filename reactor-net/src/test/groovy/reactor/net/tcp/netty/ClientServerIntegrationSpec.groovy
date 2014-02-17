/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.net.tcp.netty

import reactor.core.Environment
import reactor.function.Consumer
import reactor.io.encoding.LengthFieldCodec
import reactor.io.encoding.json.JsonCodec
import reactor.net.netty.tcp.NettyTcpClient
import reactor.net.netty.tcp.NettyTcpServer
import reactor.net.NetChannel
import reactor.net.tcp.spec.TcpClientSpec
import reactor.net.tcp.spec.TcpServerSpec
import reactor.net.tcp.support.SocketUtils
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class ClientServerIntegrationSpec extends Specification {

    Environment env

    def setup() {
        env = new Environment()
    }

    @Unroll
    def "Client should be able to send data to server"(List<Pojo> data) {
        given: "a TcpServer and TcpClient with JSON codec"
        def startLatch = new CountDownLatch(2)
        def stopLatch = new CountDownLatch(2)
        def dataLatch = new CountDownLatch(data.size())

        final int port = SocketUtils.findAvailableTcpPort()

        def consumerMock = Mock(Consumer) { data.size() * accept(_) }

        def codec = new LengthFieldCodec(new JsonCodec(Pojo))

        def server = new TcpServerSpec<Pojo, Pojo>(NettyTcpServer)
                .env(env)
                .listen(port)
                .codec(codec)
                .consume({ conn ->
                    conn.in().consume({ pojo ->
                        dataLatch.countDown()
                        consumerMock.accept(pojo)
                    } as Consumer<Pojo>)
                } as Consumer<NetChannel<Pojo, Pojo>>)
                .get()

        def client = new TcpClientSpec<Pojo, Pojo>(NettyTcpClient)
                .env(env)
                .codec(codec)
                .connect("localhost", port)
                .get()

        when: 'the server is started'
        server.start({startLatch.countDown()})
        startLatch.await(5, TimeUnit.SECONDS)

        and: "connection is established"
        def connectionPromise = client.open()
        startLatch.countDown()
        def connection = connectionPromise.await()

        and: "pojo is written"
        data.each { Pojo item -> connection.sendAndForget(item) }
        [dataLatch, startLatch].each {it.await(50, TimeUnit.SECONDS)}

        then: "everything went fine"
        startLatch.count == 0
        dataLatch.count == 0

        when: "server and client are stopped"
        client.close().onSuccess({stopLatch.countDown()} as Consumer<Void>)
        server.shutdown().onSuccess({stopLatch.countDown() } as Consumer<Void>)
        stopLatch.await(5, TimeUnit.SECONDS)

        then: "everything is really stopped"
        stopLatch.count == 0

        where:
            data << [
                    [new Pojo('John')],
                    [new Pojo('John'), new Pojo("Jane")],
                    [new Pojo('John'), new Pojo("Jane"), new Pojo("Blah")],
                    (1..10).collect {new Pojo("Value_$it")}.toList(),
            ]
    }

    @Unroll
    def "Server should be able to send POJO to client"(List<Pojo> data) {
        given: "a TcpServer and TcpClient with JSON codec"
        def startLatch = new CountDownLatch(1)
        def stopLatch = new CountDownLatch(2)
        def dataLatch = new CountDownLatch(data.size())

        final int port = SocketUtils.findAvailableTcpPort()

        def consumerMock = Mock(Consumer) { data.size() * accept(_) }

        def codec = new LengthFieldCodec(new JsonCodec(Pojo))

        def server = new TcpServerSpec<Pojo, Pojo>(NettyTcpServer)
                .env(env)
                .listen(port)
                .codec(codec)
                .consume({ conn -> data.each{pojo -> conn.out().accept(pojo)}} as Consumer)
                .get()

        def client = new TcpClientSpec<Pojo, Pojo>(NettyTcpClient)
                .env(env)
                .codec(codec)
                .connect("localhost", port)
                .get()

        when: 'the server is started'
        server.start({ startLatch.countDown() })
        startLatch.await(5, TimeUnit.SECONDS)

        and: "connection is established"
        client.open().consume({ NetChannel conn ->
            conn.consume({ Pojo pojo ->
                dataLatch.countDown()
                consumerMock.accept(pojo)
            } as Consumer)
            startLatch.countDown()
        } as Consumer).await()

        and: "data is being sent"
        [dataLatch, startLatch].each { it.await(5, TimeUnit.SECONDS) }

        then: "everything went fine"
        startLatch.count == 0
        dataLatch.count == 0

        when: "server and client are stopped"
        client.close().onSuccess({ stopLatch.countDown() })
        server.shutdown().onSuccess({ stopLatch.countDown() })
        stopLatch.await(5, TimeUnit.SECONDS)

        then: "everything is really stopped"
        stopLatch.count == 0

        where:
        data << [
                [new Pojo('John')],
                [new Pojo('John'), new Pojo("Jane")],
                [new Pojo('John'), new Pojo("Jane"), new Pojo("Blah")],
                (1..10).collect {new Pojo("Value_$it")}.toList(),
        ]
    }

    static class Pojo {
        public Pojo(){}
        public Pojo(String name) {this.name = name}
        String name
    }

}
