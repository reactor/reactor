/*
 * Copyright (c) 2011-2013 the original author or authors.
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

package reactor.spring.integration

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.channels.SocketChannel
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
@ContextConfiguration(locations = "classpath*:reactor/spring/integration/syslog.xml")
class SyslogIngestionSpec extends Specification {

	@Autowired
	CountDownLatch latch

	def "can ingest syslog messages into a MessageChannel"() {
		given: "a dumb client"
		def socket = SocketChannel.open(new InetSocketAddress(5140))

		when: "writing syslog data"
		socket.write(ByteBuffer.wrap("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n".bytes))
		latch.await(1, TimeUnit.SECONDS)

		then: "latch was counted down"
		latch.count == 0
	}

}
