package reactor.pipe;

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
    protected <T, O> void subscribe(IPipe.IPipeEnd<T, O> pipe) {
        pipe.subscribe(Key.wrap("source", "first"),
                firehose);
    }

    @Override
    protected <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> values) {
        pipe.subscribe(Key.wrap("source", "first"),
                firehose);

        for (T v : values) {
            firehose.notify(Key.wrap("source", "first"), v);
        }
    }

    @Test
    public void testMap() throws InterruptedException {
        AVar<String> res = new AVar<>();

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .map(i -> {
                            return i + 1;
                        })
                        .map(i -> i * 2)
                        .map(Object::toString)
                        .consume(res::set),
                Arrays.asList(1));

        assertThat(res.get(1, TimeUnit.SECONDS), is("4"));
    }

    @Test
    public void testConsumeWithSupplier() throws InterruptedException {
        AVar<Integer> res = new AVar<>();

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .map(i -> i + 1)
                        .map(i -> i * 2)
                        .consume(() -> (k, v) -> res.set(v)),
                Arrays.asList(1));

        assertThat(res.get(1, TimeUnit.SECONDS), is(4));
    }

    @Test
    public void testStatefulMap() throws InterruptedException {
        AVar<Integer> res = new AVar<>(3);

        subscribeAndDispatch(
                Pipe.<Integer>build().map((i) -> i + 1)
                        .map((Atom<Integer> state, Integer i) -> {
                                    return state.update(old -> old + i);
                                },
                                0)
                        .consume(res::set),
                Arrays.asList(1, 2, 3));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(9));
    }

    @Test
    public void scanTest() throws InterruptedException {
        AVar<Integer> res = new AVar<>(3);

        subscribeAndDispatch(
                Pipe.<Integer>build().scan((acc, i) -> acc + i,
                        0)
                        .consume(res::set),
                Arrays.asList(1, 2, 3));

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(6));
    }

    @Test
    public void debounceTest() throws InterruptedException {
        AVar<Integer> res = new AVar<>(1);

        long start = System.currentTimeMillis();
        AtomicLong end = new AtomicLong();
        subscribeAndDispatch(
                Pipe.<Integer>build().debounce(1, TimeUnit.SECONDS)
                        .consume((v) -> {
                            res.set(v);
                            end.set(System.currentTimeMillis());
                        }),
                Arrays.asList(1, 2));

        Thread.sleep(500);
        firehose.notify(Key.wrap("source", "first"),
                3);

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(3));
        assertTrue(end.get() - start > 1000);
    }

    @Test
    public void throttleTest() throws InterruptedException {
        AVar<Integer> res = new AVar<>(1);

        AtomicLong end = new AtomicLong();
        subscribeAndDispatch(
                Pipe.<Integer>build().throttle(1, TimeUnit.SECONDS)
                        .consume((v) -> {
                            res.set(v);
                            end.set(System.currentTimeMillis());
                        }),
                Arrays.asList(1, 2));

        Thread.sleep(500);
        long start = System.currentTimeMillis();
        firehose.notify(Key.wrap("source", "first"),
                3);

        assertThat(res.get(LATCH_TIMEOUT, LATCH_TIME_UNIT), is(3));
        System.out.println(end.get() - start);
        assertTrue(end.get() - start >= 1000);
    }

    @Test
    public void testFilter() throws InterruptedException {
        AVar<Integer> res = new AVar<>();

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .map(i -> i + 1)
                        .filter(i -> i % 2 != 0)
                        .map(i -> i * 2)
                        .consume(res::set),
                Arrays.asList(1, 2));

        assertThat(res.get(1, TimeUnit.SECONDS), is(6));
    }

    @Test
    public void testPartition() throws InterruptedException {
        AVar<List<Integer>> res = new AVar<>();

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .partition((i) -> {
                            return i.size() == 5;
                        })
                        .consume(res::set),
                Arrays.asList(1, 2, 3, 4, 5, 6, 7));

        assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(1, 2, 3, 4, 5))));
    }

    @Test
    public void testSlide() throws InterruptedException {
        AVar<List<Integer>> res = new AVar<>(6);

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .slide(i -> i.subList(i.size() > 5 ? i.size() - 5 : 0,
                                i.size()))
                        .consume(res::set),
                Arrays.asList(1, 2, 3, 4, 5, 6));

        assertThat(res.get(1, TimeUnit.SECONDS), is(TreePVector.from(Arrays.asList(2, 3, 4, 5, 6))));
    }

    @Test
    public void testConsume() throws InterruptedException {
        AVar<Integer> resValue = new AVar<>();
        AVar<Key> resKey = new AVar<>();

        subscribeAndDispatch(
                Pipe.<Integer>build()
                        .map((i) -> i + 1)
                        .map(i -> i * 2)
                        .consume((k, v) -> {
                            resKey.set(k);
                            resValue.set(v);
                        }),
                Arrays.asList(1));

        assertThat(resKey.get(1, TimeUnit.SECONDS).getPart(0), is("source"));
        assertThat(resValue.get(1, TimeUnit.SECONDS), is(4));
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
