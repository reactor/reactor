package reactor.pipe;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
public abstract class AbstractPipeTests extends AbstractRawBusTests {

  protected abstract <T, O> void subscribe(IPipe.IPipeEnd<T, O> pipe);

  protected abstract <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> value);

  protected abstract <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, T value);



//  @Test
//  public void testSmoke() throws InterruptedException { // Tests backpressure and in-thread dispatches
//    Firehose<Key> concurrentFirehose = new Firehose<>(new ConcurrentRegistry<>(),
//                                                      RingBufferWorkProcessor.create(Executors.newFixedThreadPool(4),
//                                                                                     256),
//                                                      4,
//                                                      new Consumer<Throwable>() {
//                                                        @Override
//                                                        public void accept(Throwable throwable) {
//                                                          System.out.printf("Exception caught while dispatching: %s\n",
//                                                                            throwable.getMessage());
//                                                          throwable.printStackTrace();
//                                                        }
//                                                      });
//
//    int iterations = 2000;
//    CountDownLatch latch = new CountDownLatch(iterations);
//
//    subscribe(
//      Pipe.<Integer>build()
//        .map((i) -> i + 1)
//        .map(i -> {
//          try {
//            Thread.sleep(10);
//          } catch (InterruptedException e) {
//            e.printStackTrace();
//          }
//          return i * 2;
//        })
//        .consume((i_) -> latch.countDown()));
//
//    for (int i = 0; i < iterations; i++) {
//      firehose.notify(Key.wrap("source", "first"), i);
//      if (i % 500 == 0) {
//        System.out.println("Processed " + i + " keys");
//      }
//    }
//
//    latch.await(5, TimeUnit.MINUTES);
//    assertThat(latch.getCount(), is(0L));
//
//    concurrentFirehose.shutdown();
//  }
}
