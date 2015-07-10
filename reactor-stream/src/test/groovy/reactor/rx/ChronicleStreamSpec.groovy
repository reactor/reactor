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

package reactor.rx

import reactor.io.codec.StandardCodecs
import reactor.io.persistent.spec.PersistentQueueSpec
import reactor.rx.broadcast.Broadcaster
import spock.lang.Specification

/**
 * @author Stephane Maldini
 */
class ChronicleStreamSpec extends Specification {

	def "ChronicleStream persists objects and notifies subscribers"() {

		given:
			"2 slaves and 1 master"
			def putPromise = IOStreams.<Integer, String> persistentMapReader('journal')
					.onPut()
					.log('put')
					.next()

			def deletePromise = IOStreams.<Integer, String> persistentMapReader('journal')
					.onRemove()
					.log('remove')
					.next()

			def persistor = IOStreams.<Integer, String> persistentMap('journal', true)

			def allPromise = persistor
					.map { it.key() }
					.log('all')
					.toList(2)

			def obj = 'test1'

		when:
			"an Object is persisted"
			persistor[1] = obj

		then:
			"the Object was persisted"
			persistor[1] == obj

		when:
			"an Object is removed"
			persistor.remove(1)

		then:
			"the Object was removed"
			putPromise.await().t2 == 'test1'
			deletePromise.await() == 1
			allPromise.await() == [1, 1]
			persistor.size() == 0
	}

	def "Stream can use persistent queues"() {

		given:
			"a persistent queue specification and a Stream"
			def persistentQueue = new PersistentQueueSpec()
					.codec(StandardCodecs.STRING_CODEC)
					.deleteOnExit(true)
					.get()

			def stream = Broadcaster.<String> create()
			def result = null

			def bufferedStream = stream.onOverflowBuffer{persistentQueue}.observe{
				println it
				result = it
			}.consumeLater()

		when:
			"an object is persisted"
			stream.onNext("test!")
			println bufferedStream.debug()

		then:
			"the object was persisted"
			persistentQueue.size() == 1

		when:
			"the object is read"
			bufferedStream.requestMore(1)
			println stream.debug()

		then:
			"the object was removed"
			result
			persistentQueue.size() == 0

		cleanup:
			persistentQueue?.close()

	}

}
