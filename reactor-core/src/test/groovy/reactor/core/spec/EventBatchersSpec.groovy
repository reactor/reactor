package reactor.core.spec

import reactor.event.Event
import reactor.queue.IndexedChronicleQueuePersistor
import reactor.queue.PersistentQueue
import spock.lang.Specification

import static reactor.GroovyTestUtils.$
import static reactor.GroovyTestUtils.consumer

/**
 * @author Jon Brisbin
 */
class EventBatchersSpec extends Specification {

  def "A Reactor delays notification of events in a Sequencer"() {

    given:
      "a synchronous Reactor EventBatcher"
      def r = Reactors.reactor().synchronousDispatcher().get()
      def c = new EventBatcherSpec().
          observable(r).
          notifyKey("test").
          get()
      def events = [Event.wrap("1"), Event.wrap("2"), Event.wrap("3")]
      def processedEvents = []
      r.on($("test"), consumer { ev ->
        processedEvents << ev
      })

    when:
      "events are published to the EventBatcher"
      events.each {
        c.accept(it)
      }

    then:
      "events should not have been processed"
      processedEvents.size() == 0

    when:
      "the EventBatcher is flushed"
      c.flush()

    then:
      "events have been processed in order"
      processedEvents == events
      processedEvents[0].data == "1"
      processedEvents[1].data == "2"
      processedEvents[2].data == "3"

  }

  def "A Reactor persists Events in a Sequencer"() {

    given:
      "a EventBatcher backed by a PersistentQueue"
      def r = Reactors.reactor().synchronousDispatcher().get()
      def persistor = new IndexedChronicleQueuePersistor("./persistent-queue")
      def c = new EventBatcherSpec().
          observable(r).
          notifyKey("test").
          eventQueue(new PersistentQueue<Event<String>>(persistor)).
          get()
      def events = [Event.wrap("1"), Event.wrap("2"), Event.wrap("3")]
      def processedEvents = []
      r.on($("test"), consumer { ev ->
        processedEvents << ev
      })

    when:
      "events are published to the EventBatcher"
      events.each {
        c.accept(it)
      }

    then:
      "events should not have been processed"
      processedEvents.size() == 0

    when:
      "the EventBatcher is flushed"
      c.flush()

    then:
      "events have been processed in order"
      processedEvents[0].data == "1"
      processedEvents[1].data == "2"
      processedEvents[2].data == "3"

    cleanup:
      persistor.close()

  }

}
