package reactor.pipe;

import org.junit.Test;
import reactor.fn.Predicate;
import reactor.pipe.concurrent.AVar;
import reactor.pipe.concurrent.Atom;
import reactor.pipe.key.Key;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@SuppressWarnings("unchecked")
public class MatchedPipeTests extends AbstractPipeTests {

    @Override
    protected <T, O> void subscribeAndDispatch(IPipe.IPipeEnd<T, O> pipe, List<T> values) {
        pipe.subscribe((k) -> k.getPart(0).equals("source"),
                       firehose);

        for (T value : values) {
            firehose.notify(Key.wrap("source", "first"), value);
        }
    }

    @Test
    public void testMatching() throws InterruptedException {
        AVar<Integer> res1 = new AVar<>(1);
        AVar<Integer> res2 = new AVar<>(1);

        for (AVar<Integer> avar : new AVar[]{res1, res2}) {
            integerPipe
                    .consume((i) -> {
                        System.out.println(i);
                        avar.set(i);
                    })
                    .subscribe(new Predicate<Key>() {
                        @Override
                        public boolean test(Key key) {
                            return key.getPart(0).equals("source");
                        }
                    },
                    firehose);
        }

        firehose.notify(Key.wrap("source"), 100);

        assertThat(res1.get(1, TimeUnit.SECONDS), is(100));
        assertThat(res2.get(1, TimeUnit.SECONDS), is(100));
    }

}
