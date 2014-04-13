package reactor.core;

import org.junit.Test;
import reactor.AbstractReactorTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class EnvironmentTest extends AbstractReactorTest {

    @Test
    public void testGetDispatcherThrowsExceptionWhenNoDispatcherIsFound() throws Exception {
        try {
            env.getDispatcher("NonexistingDispatcher");
            fail("Should have thrown an exception");
        } catch ( IllegalArgumentException e ) {
            assertEquals("No Dispatcher found for name 'NonexistingDispatcher'", e.getMessage());
        }
    }

}
