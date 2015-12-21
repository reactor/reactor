package reactor.pipe;

import org.junit.Ignore;
import org.junit.Test;
import org.pcollections.TreePVector;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class AnonymousPipeTests extends AbstractPipeTests {

    @Override
    protected <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> values) {
        pipe.subscribe(Key.wrap("source", "first"),
                firehose);

        for (T v : values) {
            firehose.notify(Key.wrap("source", "first"), v);
        }
    }

    //  @Test
    //  public void testUnregister() throws InterruptedException {
    //    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    //    CountDownLatch latch = new CountDownLatch(1);
    //
    //    AnonymousPipe<Integer> s = pipe.anonymous(Key.wrap("source"));
    //
    //    s.map((i) -> i + 1)
    //     .map(i -> i * 2)
    //     .consume(i -> latch.countDown());
    //
    //    pipe.notify(Key.wrap("source"), 1);
    //    latch.await(10, TimeUnit.SECONDS);
    //    s.unregister();
    //
    //    assertThat(pipe.firehose().getConsumerRegistry().stream().count(), is(0L));
    //  }
    //
    //  @Test
    //  public void testRedirect() throws InterruptedException {
    //    Key destination = Key.wrap("destination");
    //    NamedPipe<Integer> pipe = new NamedPipe<>(firehose);
    //    AVar<Integer> res = new AVar<>();
    //
    //    AnonymousPipe<Integer> s = pipe.anonymous(Key.wrap("source"));
    //
    //    s.map((i) -> i + 1)
    //     .map(i -> i * 2)
    //     .redirect((k, v) -> destination);
    //
    //    pipe.consume(destination, (Integer i) -> res.set(i));
    //
    //    pipe.notify(Key.wrap("source"), 1);
    //
    //    assertThat(res.get(1, TimeUnit.SECONDS), is(4));
    //  }
    //

}
