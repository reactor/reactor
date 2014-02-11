package reactor.core.alloc;

import org.junit.Test;
import reactor.core.alloc.event.EventAllocator;
import reactor.event.Event;

import static junit.framework.Assert.assertTrue;

public class EventAllocatorTests {

  @Test
  public void eventAllocatorTest() {
    EventAllocator eventAllocator = EventAllocator.defaultEventAllocator();

    Event<String> eStr = eventAllocator.get(String.class).get();
    eStr.setData("string");
    assertTrue("String data is settable into the String event", eStr.getData() == "string");


    Event<Integer> eInt = eventAllocator.get(Integer.class).get();
    eInt.setData(1);
    assertTrue("Integer data is settable into the Integer event", eInt.getData() == 1);
  }
}
