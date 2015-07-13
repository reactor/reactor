package reactor.aeron.processor;

import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Anatoly Kadyshev
 */
public class RequestCounterTest {

    @Test
    public void testGetNextRequestLimit() {
        RequestCounter counter = new RequestCounter(5);

        counter.request(10);

        assertThat(counter.getNextRequestLimit(), is(5L));
        counter.release(5);

        assertThat(counter.getNextRequestLimit(), is(5L));
        counter.release(5);

        assertThat(counter.getNextRequestLimit(), is(0L));
    }

}