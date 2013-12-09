package reactor.event.selector;

import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Jon Brisbin
 */
public class UriSelector extends ObjectSelector<URI> {

	private static final UriHeaderResolver URI_HEADER_RESOLVER = new UriHeaderResolver();

	private final String scheme;
	private final String authority;
	private final String userInfo;
	private final String host;
	private final int    port;
	private final String path;
	private final String fragment;

	public UriSelector(String uri) {
		this(URI.create(uri));
	}

	public UriSelector(URI uri) {
		super(uri);
		scheme = (null != uri.getScheme() ? uri.getScheme() : "*");
		authority = uri.getAuthority();
		userInfo = uri.getUserInfo();
		host = (null != uri.getHost() ? uri.getHost() : "*");
		if(authority.contains("*:")) {
			int i = authority.indexOf("@");
			int z = authority.lastIndexOf(":") + 1;
			if(i < 0) {
				port = Integer.parseInt(authority.substring(z));
			} else {
				port = Integer.parseInt(authority.substring(i, z));
			}
		} else {
			port = uri.getPort();
		}
		path = (null != uri.getPath() ? uri.getPath() : "/*");
		fragment = uri.getFragment();
	}

	@Override
	public HeaderResolver getHeaderResolver() {
		return URI_HEADER_RESOLVER;
	}

	@Override
	public boolean matches(Object key) {
		if(null == key) {
			return false;
		}

		URI uri = objectToURI(key);

		boolean schemeMatches = "*".equals(scheme) || scheme.equals(uri.getScheme());
		boolean userInfoMatches = null == userInfo || userInfo.equals(uri.getUserInfo());
		boolean hostMatches = "*".equals(host) || host.equals(uri.getHost());
		boolean portMatches = -1 == port || port == uri.getPort();
		boolean pathMatches = "/*".equals(path) || path.equals(uri.getPath());
		boolean fragmentMatches = null == fragment || fragment.equals(uri.getFragment());

		return schemeMatches
				&& userInfoMatches
				&& hostMatches
				&& portMatches
				&& pathMatches
				&& fragmentMatches;
	}

	private static URI objectToURI(Object key) {
		if(key instanceof URI) {
			return (URI)key;
		} else if(key instanceof String) {
			return URI.create(key.toString());
		} else {
			throw new IllegalArgumentException("Cannot convert " +
					                                   key +
					                                   " into a java.net.URI. Key should be a URI or a String.");
		}
	}

	private static class UriHeaderResolver implements HeaderResolver {
		@Nullable
		@Override
		public Map<String, String> resolve(Object key) {
			if(null == key) {
				return null;
			}

			URI uri = objectToURI(key);
			Map<String, String> headers = new HashMap<String, String>();

			headers.put("authority", uri.getAuthority());
			headers.put("fragment", uri.getFragment());
			headers.put("host", uri.getHost());
			headers.put("path", uri.getPath());
			headers.put("port", String.valueOf(uri.getPort()));
			headers.put("query", uri.getQuery());
			if(null != uri.getQuery()) {
				try {
					String query = URLDecoder.decode(uri.getQuery(), "ISO-8859-1");
					for(String s : query.split("&")) {
						String[] parts = s.split("=");
						headers.put(parts[0], parts[1]);
					}
				} catch(UnsupportedEncodingException e) {
					throw new IllegalArgumentException(e);
				}
			}
			headers.put("scheme", uri.getScheme());
			headers.put("userInfo", uri.getUserInfo());

			return headers;
		}
	}

}
