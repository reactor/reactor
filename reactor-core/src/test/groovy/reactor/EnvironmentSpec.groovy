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
package reactor

import reactor.core.Dispatcher
import reactor.core.config.ConfigurationReader
import reactor.core.config.ReactorConfiguration
import spock.lang.Specification

class EnvironmentSpec extends Specification {

	def "An environment cleans up its Dispatchers when it's shut down"() {

		given:
			"An Environment"

			ReactorConfiguration configuration = new ReactorConfiguration([], 'default', [:] as Properties)
			Dispatcher dispatcher = Mock(Dispatcher)
			Dispatcher dispatcher2 = Mock(Dispatcher)
			Environment environment = new Environment(['alpha': dispatcher, 'bravo': dispatcher2], Mock(ConfigurationReader, {
				read() >> configuration
			}))

		when:
			"it is shut down"
			environment.shutdown()

		then:
			"its dispatchers are cleaned up"
			1 * dispatcher.shutdown()
			1 * dispatcher2.shutdown()
	}

	def "An environment can create Dispatchers"() {

		given:
			"An Environment"
			Environment.initializeIfEmpty()

		when:
			"it creates a dispatcher"
			def dispatcher = Environment.newDispatcher()

		then:
			"its dispatcher exists"
			dispatcher
			dispatcher.backlogSize() == 2048

		when:
			"it creates a dispatcher like the default shared"
			dispatcher = Environment.newDispatcherLike(Environment.SHARED, "newDispatcher")

		then:
			"its dispatchers exists in environment"
			dispatcher
			Environment.dispatcher("newDispatcher") == dispatcher
			Environment.sharedDispatcher().getClass() == dispatcher.getClass()
			Environment.sharedDispatcher().backlogSize() == dispatcher.backlogSize()

		cleanup:
			if(Environment.alive())
				Environment.terminate()
	}

}
