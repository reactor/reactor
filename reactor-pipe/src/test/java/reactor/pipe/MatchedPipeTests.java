package reactor.pipe;

import org.junit.Test;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MatchedPipeTests extends AbstractPipeTests {

  @Test
  public void testMatching() throws InterruptedException {
    AVar<Integer> res1 = new AVar<>(1);
    AVar<Integer> res2 = new AVar<>(1);

    for (AVar<Integer> avar : new AVar[]{res1, res2}) {
      Pipe.<Integer>build()
        .consume((i) -> {
          System.out.println(i);
          avar.set(i);
        })
        .subscribe(key -> key.getPart(0).equals("source"),
                   firehose);
    }

    firehose.notify(Key.wrap("source"), 100);

    assertThat(res1.get(1, TimeUnit.SECONDS), is(100));
    assertThat(res2.get(1, TimeUnit.SECONDS), is(100));
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
        1);

    System.out.println(res.get(1, TimeUnit.SECONDS));
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
        1);

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






  @Override
  protected <T, O> void subscribe(IPipe.IPipeEnd<T, O> pipe) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);
  }

  @Override
  protected <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> values) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);

    for(T value: values) {
      firehose.notify(Key.wrap("source", "first"), value);
    }
  }

  @Override
  protected <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, T value) {
    pipe.subscribe((k) -> k.getPart(0).equals("source"),
                   firehose);

    firehose.notify(Key.wrap("source", "first"), 1);
  }


}
