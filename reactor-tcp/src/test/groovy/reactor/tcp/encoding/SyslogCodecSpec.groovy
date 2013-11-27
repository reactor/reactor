package reactor.tcp.encoding

import reactor.io.Buffer
import reactor.tcp.encoding.syslog.SyslogCodec
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class SyslogCodecSpec extends Specification{

	def "SyslogCodec can decode syslog messages"() {
		given: "syslog data"
			def codec = new SyslogCodec()
			def data = Buffer.wrap("<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8\n")
			def host = ""

		when: "data is decoded"
			def msg = codec.decoder(null).apply(data)
			host = msg.host

		then: "data was decoded"
			host == "mymachine"
	}

}
