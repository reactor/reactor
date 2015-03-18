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

package reactor.io.queue

import net.openhft.chronicle.ChronicleQueueBuilder
import net.openhft.chronicle.tools.ChronicleTools
import reactor.io.codec.StandardCodecs
import reactor.io.codec.json.JsonCodec
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class PersistentQueueSpec extends Specification {

	static QueuePersistor<String> persistor() {
		def config = ChronicleQueueBuilder.indexed('persistent-queue')

		new IndexedChronicleQueuePersistor<String>(
				"persistent-queue",
				StandardCodecs.STRING_CODEC,
				true,
				false,
				config
		)
	}

	static <T> QueuePersistor<T> jsonPersistor(Class<T> type) {
		def config = ChronicleQueueBuilder.indexed('persistent-queue')

		new IndexedChronicleQueuePersistor<T>(
				"persistent-queue",
				new JsonCodec<T, T>(type),
				true,
				false,
				config
		)
	}

	def cleanup() {
		ChronicleTools.deleteOnExit("persistent-queue")
	}

	def "File-based PersistentQueue is sharable"() {

		given:
			def wq = new PersistentQueue<String>(persistor())
			def strings1 = []
			def rq = new PersistentQueue<String>(persistor())
			def strings2 = []

		when:
			"data is written to a write Queue"
			(1..100).each {
				def s = "test $it".toString()
				wq.offer(s)
				strings1 << s
			}

		then:
			"all data was written"
			wq.size() == 100

		when:
			"data is read from a shared read Queue"
			rq.each {
				strings2 << it
			}

		then:
			"all data was read"
			strings1 == strings2

        cleanup:
            wq.close()
            rq.close()
	}

	def "Java Chronicle-based PersistentQueue is performant"() {

		given:
			def wq = new PersistentQueue(jsonPersistor(Map))
			def msgs = 10000

		when:
			"data is written to the Queue"
			def start = System.currentTimeMillis()
			def count = 0
			for (int i in 1..msgs) {
				wq.offer(["test": i])
			}
			for (def m in wq) {
				count++
			}
			def end = System.currentTimeMillis()
			double elapsed = end - start
			int throughput = msgs / (elapsed / 1000)
			println "throughput: ${throughput}/sec in ${(int) elapsed}ms"

		then:
			"throughput is sufficient"
			count == msgs
			throughput > 1000

        cleanup:
            wq.close();
	}

}
