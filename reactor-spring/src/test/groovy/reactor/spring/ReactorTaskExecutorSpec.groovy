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

package reactor.spring

import reactor.R
import reactor.core.Environment
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 */
class ReactorTaskExecutorSpec extends Specification {

  Environment env

  def setup() {
    env = new Environment()
  }

  def "ReactorTaskExecutor executes tasks"() {
    given:
      "a Reactor-backed TaskExecutor"
      def reactor = R.reactor().using(env).dispatcher(Environment.EVENT_LOOP).get()
      def exec = new ReactorTaskExecutor(reactor)
      def latch = new CountDownLatch(1)

    when:
      "a task is submitted"
      exec.execute({
        latch.countDown()
      })
      latch.await(1, TimeUnit.SECONDS)

    then:
      "the latch was counted down"
      latch.getCount() == 0
  }

}
