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



package reactor.groovy

import groovy.transform.CompileStatic
import reactor.core.Environment
import reactor.P
import reactor.fn.Supplier

/**
 * This class shouldnt fail compilation
 *
 * @author Stephane Maldini
 */
@CompileStatic
class CompileStaticTest {

	Environment env = new Environment()

	def run() {

		def testClosure = {
			'test'
		}

		def supplier = new Supplier<String>(){
			@Override
			String get() {
				'test'
			}
		}

		P.task(supplier).using(env)
	}

	static void main(String[] args) {
		new CompileStaticTest().run()

	}
}
