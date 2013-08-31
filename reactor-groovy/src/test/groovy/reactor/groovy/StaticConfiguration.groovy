/* * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.groovy

import groovy.transform.CompileStatic
import reactor.core.configuration.DispatcherType
import reactor.event.dispatch.SynchronousDispatcher
import reactor.groovy.config.GroovyEnvironment

@CompileStatic
class StaticConfiguration {

	static GroovyEnvironment test() {
		GroovyEnvironment.create {
			environment {
				defaultDispatcher = "test"

				dispatcher('test') {
					type = DispatcherType.SYNCHRONOUS
				}
			}
		}
	}

	static GroovyEnvironment test2() {
		GroovyEnvironment.create {
			reactor('test1') {
				on('test') {
					println it
				}

				reactor('child_test1') {
					ext 'a', 'rw'
				}
			}
			reactor('test2') {
				ext 'a', '2'
			}
		}
	}

	static GroovyEnvironment test3() {
		GroovyEnvironment.create {
			reactor('test1') {
				dispatcher = new SynchronousDispatcher()
				on('test') {
					reply it
				}

			}
		}
	}

	static GroovyEnvironment test4() {
		def parentEnvironment = GroovyEnvironment.create {
			environment {
				defaultDispatcher = 'testDispatcher'

				dispatcher 'testDispatcher', new SynchronousDispatcher()
			}

			reactor('test1') {
				dispatcher 'testDispatcher'
				on('test') {
					reply it
				}
			}
		}

		GroovyEnvironment.create {
			include parentEnvironment

			reactor('test1') {
				on('test2') {
					reply it
				}
			}
			reactor('test2') {
				dispatcher 'testDispatcher'
			}
		}
	}
}