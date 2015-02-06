/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
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
package reactor.io.net.http

import reactor.Environment
import reactor.io.codec.StandardCodecs
import reactor.io.net.NetStreams
import reactor.rx.Streams
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 * @author Stephane Maldini
 */
class HttpSpec extends Specification {

	static final int port = 8080
	Environment env

	def setup() {
		env = Environment.initializeIfEmpty()
	}

	def "http responds to requests from clients"() {
		given: "a simple HttpServer"
			def server = NetStreams.httpServer{
				it.codec(StandardCodecs.STRING_CODEC).listen(port)
			}
			def client = NetStreams.httpClient{
				it.codec(StandardCodecs.STRING_CODEC)
			}

		when: "the server is prepared"
			server.post('/test/{param}') { req ->
				req
						.log('server-received')
						.map{it+' ' + req.param('param') + '!'}
						.log('server-reply')
			}

		then: "the server was started"
			server?.start()?.awaitSuccess(5, TimeUnit.SECONDS)

		when: "data is sent with Reactor HTTP support"
			def content = client.post('http://localhost:8080/test/World') { req ->
				req
						.header('Content-Type', 'text/plain')
						.log('client-received')

					Streams
							.just("Hello")
							.log('client-send')
			}.
					next().
					flatMap { rep ->
						rep.next()
					}



		then: "data was recieved"
			client.open().awaitSuccess(500, TimeUnit.SECONDS)
			content.await(300, TimeUnit.SECONDS) == "Hello World!"

		cleanup: "the client/server where stopped"
			client?.close()?.flatMap { server.shutdown() }?.awaitSuccess(5, TimeUnit.SECONDS)
	}

}