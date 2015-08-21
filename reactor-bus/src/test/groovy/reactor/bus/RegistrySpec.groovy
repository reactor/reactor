/*
 * Copyright (c) 2011-2015 Pivotal Software, Inc.
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *         http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.bus

import reactor.bus.registry.CachingRegistry
import reactor.bus.registry.Registry
import reactor.bus.registry.SimpleCachingRegistry
import spock.lang.Specification

import static reactor.bus.selector.Selectors.$

/**
 * Created by jbrisbin on 1/29/15.
 */
class RegistrySpec extends Specification {

	def "Registry tracks Registrations"(Registry<?, String> regs) {

		given: "simple Registrations"
			regs.register $("Hello"), "World!"

		when: "Registration is selected"
			def r = regs.select("Hello")

		then: "a Registration was found"
			r.size() == 1
			r[0].object == "World!"

		when: "Registration is cancelled"
			r[0].cancel()

		then: "Registration is not found on subsequent search"
			!regs.select("Hello")

		where:
			regs << [new CachingRegistry<?, String>(true, true, null),
			         new SimpleCachingRegistry<?, String>(true, true, null)]

	}

}
