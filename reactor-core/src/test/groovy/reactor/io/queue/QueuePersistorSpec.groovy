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
import reactor.io.codec.StandardCodecs
import spock.lang.Specification

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class QueuePersistorSpec extends Specification {

	def "InMemoryQueuePersistor persists objects"() {

		given:
			"an InMemoryQueuePersistor"
			def persistor = new InMemoryQueuePersistor()
			def obj = new Object()

		when:
			"an Object is persisted"
			persistor.offer(obj)

		then:
			"the Object was persisted"
			persistor.get(persistor.lastId()) == obj

		when:
			"an Object is removed"
			persistor.remove()

		then:
			"the Object was removed"
			null == persistor.get(persistor.lastId())
			persistor.size() == 0

		cleanup:
			persistor?.close()

	}

	def "IndexedChronicleQueuePersistor persists objects"() {

		given:
			"an IndexedChronicleQueuePersistor"
			def persistor = new IndexedChronicleQueuePersistor<String>(
					"queue-persistor",
					StandardCodecs.STRING_CODEC,
					true,
					true,
					null,
					ChronicleQueueBuilder.indexed('queue-persistor').test()
			)
			def obj = "Hello World!"

		when:
			"an object is persisted"
			persistor.offer(obj)

		then:
			"the object was persisted"
			persistor.get
			persistor.get(persistor.lastId()) == obj
			persistor.hasNext()

		when:
			"the object is removed"
			persistor.remove()

		then:
			"the object was removed"
			persistor.size() == 0

		cleanup:
			persistor?.close()

	}



}
