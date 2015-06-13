package reactor.core.dispatch;

import org.junit.Test;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BusySpinWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

/**
 * Created by anato_000 on 6/7/2015.
 */
public class RingBufferDispatcher3Test {

    @Test
    public void test() throws InterruptedException {
        RingBufferDispatcher3 dispatcher = new RingBufferDispatcher3("rdb3", 8, null, ProducerType.MULTI, new BusySpinWaitStrategy());

        dispatch(dispatcher);
        dispatch(dispatcher);

        Thread.sleep(60000);
    }

    private void dispatch(final RingBufferDispatcher3 dispatcher) {
        dispatcher.dispatch("Hello", new Consumer<String>() {
            @Override
            public void accept(String s) {
                System.out.println(s);

                for (int i = 0; i < 16; i++) {
                    final int j = i;
                    dispatcher.dispatch("world", new Consumer<String>() {
                        @Override
                        public void accept(String s) {
                            System.out.println(j);
                        }
                    }, null);
                }
            }
        }, new Consumer<Throwable>() {
            @Override
            public void accept(Throwable t) {
                t.printStackTrace();
            }
        });
    }

}