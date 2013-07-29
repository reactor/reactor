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

package reactor.queue

import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class PersistentQueueSpec extends Specification {

  def "File-based PersistentQueue is sharable"() {

    given:
      "a pair of IndexedChroniclePersistentQueues"
      def wq = new PersistentQueue<String>(new IndexedChronicleQueuePersistor("./persistent-queue"))
      def rq = new PersistentQueue<String>(new IndexedChronicleQueuePersistor("./persistent-queue"))

    when:
      "data is written to the queue using a write Queue"
      for (i in 1..100) {
        wq.offer("test $i")
      }
      def count = 0
      for (String s : rq) {
        ++count
      }

    then:
      "data was readable from the read Queue"
      count == 100

  }

}
