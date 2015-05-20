package reactor.core.processor;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Sergey Shcherbakov
 */
public class ReactorProcessorGettersTests {

	@Test
	public void testRingBufferProcessorGetters() {
		
		final int TEST_BUFFER_SIZE = 16;
		ReactorProcessor<Object, Object> processor = RingBufferProcessor.create("testProcessor", TEST_BUFFER_SIZE);
		
		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());
		
	}

	@Test
	public void testRingBufferWorkProcessorGetters() {
		
		final int TEST_BUFFER_SIZE = 16;
		ReactorProcessor<Object, Object> processor = RingBufferWorkProcessor.create("testProcessor", TEST_BUFFER_SIZE);
		
		assertEquals(TEST_BUFFER_SIZE, processor.getAvailableCapacity());
		
		processor.accept(new Object());
		
		assertEquals(TEST_BUFFER_SIZE - 1 , processor.getAvailableCapacity());

	}
	
}
