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
package reactor.bus.alloc;

import org.junit.Assert;
import org.junit.Test;
import reactor.bus.Event;

public class EventAllocatorTests {

  @Test
  public void eventAllocatorTest() {
    EventAllocator eventAllocator = EventAllocator.defaultEventAllocator();

    Event<String> eStr = eventAllocator.get(String.class).get();
    eStr.setData("string");
    Assert.assertTrue("String data is settable into the String event", eStr.getData() == "string");


    Event<Integer> eInt = eventAllocator.get(Integer.class).get();
    eInt.setData(1);
    Assert.assertTrue("Integer data is settable into the Integer event", eInt.getData() == 1);
  }
}
