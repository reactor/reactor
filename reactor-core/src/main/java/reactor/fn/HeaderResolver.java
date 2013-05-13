package reactor.fn;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Responsible for extracting any applicable headers from a key.
 *
 * @author Jon Brisbin
 */
public interface HeaderResolver {

	/**
	 * Resolve the headers that might be encoded in a key.
	 *
	 * @param key The key to match.
	 * @return Any applicable headers. Might be {@literal null}.
	 */
	@Nullable
	Map<String, String> resolve(Object key);

}
