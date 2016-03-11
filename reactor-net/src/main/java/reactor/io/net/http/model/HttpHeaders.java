/*
 * Copyright (c) 2011-2015 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.io.netty.http.model;

/**
 * @author Sebastien Deleuze
 * @author Stephane Maldini
 */
public interface HttpHeaders extends Headers, WritableHeaders<HttpHeaders>, ReadableHeaders {

	/**
	 * The HTTP {@code Accept} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.3.2">Section 5.3.2 of RFC 7231</a>
	 */
	String ACCEPT              = "Accept";
	/**
	 * The HTTP {@code Accept-Charset} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.3.3">Section 5.3.3 of RFC 7231</a>
	 */
	String ACCEPT_CHARSET      = "Accept-Charset";
	/**
	 * The HTTP {@code Accept-Encoding} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.3.4">Section 5.3.4 of RFC 7231</a>
	 */
	String ACCEPT_ENCODING     = "Accept-Encoding";
	/**
	 * The HTTP {@code Accept-Language} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.3.5">Section 5.3.5 of RFC 7231</a>
	 */
	String ACCEPT_LANGUAGE     = "Accept-Language";
	/**
	 * The HTTP {@code Authorization} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7235#section-4.2">Section 4.2 of RFC 7235</a>
	 */
	String AUTHORIZATION       = "Authorization";
	/**
	 * The HTTP {@code Cookie} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc2109#section-4.3.4">Section 4.3.4 of RFC 2109</a>
	 */
	String COOKIE              = "Cookie";
	/**
	 * The HTTP {@code Expect} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.1.1">Section 5.1.1 of RFC 7231</a>
	 */
	String EXPECT              = "Expect";
	/**
	 * The HTTP {@code From} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.5.1">Section 5.5.1 of RFC 7231</a>
	 */
	String FROM                = "From";
	/**
	 * The HTTP {@code Host} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7230#section-5.4">Section 5.4 of RFC 7230</a>
	 */
	String HOST                = "Host";
	/**
	 * The HTTP {@code If-Match} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7232#section-3.1">Section 3.1 of RFC 7232</a>
	 */
	String IF_MATCH            = "If-Match";
	/**
	 * The HTTP {@code If-Modified-Since} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7232#section-3.3">Section 3.3 of RFC 7232</a>
	 */
	String IF_MODIFIED_SINCE   = "If-Modified-Since";
	/**
	 * The HTTP {@code If-None-Match} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7232#section-3.2">Section 3.2 of RFC 7232</a>
	 */
	String IF_NONE_MATCH       = "If-None-Match";
	/**
	 * The HTTP {@code If-Range} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7233#section-3.2">Section 3.2 of RFC 7233</a>
	 */
	String IF_RANGE            = "If-Range";
	/**
	 * The HTTP {@code If-Unmodified-Since} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7232#section-3.4">Section 3.4 of RFC 7232</a>
	 */
	String IF_UNMODIFIED_SINCE = "If-Unmodified-Since";
	/**
	 * The HTTP {@code Max-Forwards} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.1.2">Section 5.1.2 of RFC 7231</a>
	 */
	String MAX_FORWARDS        = "Max-Forwards";
	/**
	 * The HTTP {@code Origin} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc6454">RFC 6454</a>
	 */
	String ORIGIN              = "Origin";
	/**
	 * The HTTP {@code Proxy-Authorization} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7235#section-4.4">Section 4.4 of RFC 7235</a>
	 */
	String PROXY_AUTHORIZATION = "Proxy-Authorization";
	/**
	 * The HTTP {@code Range} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7233#section-3.1">Section 3.1 of RFC 7233</a>
	 */
	String RANGE               = "Range";
	/**
	 * The HTTP {@code Referer} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.5.2">Section 5.5.2 of RFC 7231</a>
	 */
	String REFERER             = "Referer";
	/**
	 * The HTTP {@code TE} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7230#section-4.3">Section 4.3 of RFC 7230</a>
	 */
	String TE                  = "TE";
	/**
	 * The HTTP {@code User-Agent} header field name.
	 *
	 * @see <a href="http://tools.ietf.org/html/rfc7231#section-5.5.3">Section 5.5.3 of RFC 7231</a>
	 */
	String USER_AGENT          = "User-Agent";

}
