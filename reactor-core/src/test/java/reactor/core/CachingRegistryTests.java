package reactor.core;

import static org.junit.Assert.assertEquals;
import static reactor.Fn.$;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import reactor.fn.Registration;
import reactor.fn.Selector;

public final class CachingRegistryTests {

	private final CachingRegistry<Object> cachingRegistry = new CachingRegistry<Object>();

	@Test
	public void registrationsWithTheSameSelectorAreOrderedByInsertionOrder() {
		Selector selector = $("selector");

		this.cachingRegistry.register(selector, "echo");
		this.cachingRegistry.register(selector, "bravo");
		this.cachingRegistry.register(selector, "alpha");
		this.cachingRegistry.register(selector, "charlie");
		this.cachingRegistry.register(selector, "delta");

		Iterable<Registration<? extends Object>> registrations = this.cachingRegistry.select(selector);
		List<Object> objects = new ArrayList<Object>();
		for (Registration<? extends Object> registration : registrations) {
			objects.add(registration.getObject());
		}

		assertEquals(Arrays.asList("echo", "bravo", "alpha", "charlie", "delta"), objects);
	}
}
