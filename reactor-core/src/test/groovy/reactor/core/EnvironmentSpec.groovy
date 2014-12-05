/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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
package reactor.core

import reactor.core.configuration.ConfigurationReader
import reactor.core.configuration.ReactorConfiguration
import spock.lang.Specification

class EnvironmentSpec extends Specification {

  def "An environment cleans up its Dispatchers when it's shut down"() {

    given:
      "An Environment"

      ReactorConfiguration configuration = new ReactorConfiguration([], 'default', [:] as Properties)
      Dispatcher dispatcher = Mock(Dispatcher)
      Environment environment = new Environment(['alpha': [dispatcher, dispatcher], 'bravo': [dispatcher]], Mock(ConfigurationReader, {
        read() >> configuration
      }))

    when:
      "it is shut down"
      environment.shutdown()

    then:
      "its dispatchers are cleaned up"
      3 * dispatcher.shutdown()
  }

}
