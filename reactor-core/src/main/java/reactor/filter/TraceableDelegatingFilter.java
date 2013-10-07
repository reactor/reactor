package reactor.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.Assert;

import java.util.Collections;
import java.util.List;

/**
 * @author Jon Brisbin
 */
public class TraceableDelegatingFilter implements Filter {

	private final Filter delegate;
	private final Logger log;

	public TraceableDelegatingFilter(Filter delegate) {
		Assert.notNull(delegate, "Delegate Filter cannot be null.");
		this.delegate = delegate;
		this.log = LoggerFactory.getLogger(delegate.getClass());
	}

	@Override
	public <T> List<T> filter(List<T> items, Object key) {
		if(log.isTraceEnabled()) {
			log.trace("filtering {} using key {}", items, key);
		}
		List<T> l = delegate.filter(items, key);
		if(log.isTraceEnabled()) {
			log.trace("items {} matched key {}", (null == items ? Collections.emptyList() : items), key);
		}
		return l;
	}

}
