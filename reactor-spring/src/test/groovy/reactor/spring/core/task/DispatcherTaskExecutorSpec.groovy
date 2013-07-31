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

package reactor.spring.core.task

import reactor.spring.core.task.DispatcherTaskExecutor

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import reactor.core.Environment
import reactor.event.dispatch.SynchronousDispatcher
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class DispatcherTaskExecutorSpec extends Specification {

  Environment env

  def setup() {
    env = new Environment()
  }

  def "DispatcherTaskExecutor executes tasks"() {
    given:
      "a Dispatcher-backed TaskExecutor"
      def exec = new DispatcherTaskExecutor(new SynchronousDispatcher())
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
