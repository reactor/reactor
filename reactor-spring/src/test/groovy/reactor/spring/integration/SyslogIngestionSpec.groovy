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
