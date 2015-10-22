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
package reactor.io.codec.syslog

import reactor.io.buffer.Buffer
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
