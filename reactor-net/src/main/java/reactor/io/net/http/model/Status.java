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

package reactor.io.net.http.model;

public class Status {

	/**
	 * 100 Continue
	 */
	public static final Status CONTINUE = new Status(100, "Continue");

	/**
	 * 101 Switching Protocols
	 */
	public static final Status SWITCHING_PROTOCOLS =
			new Status(101, "Switching Protocols");

	/**
	 * 102 Processing (WebDAV, RFC2518)
	 */
	public static final Status PROCESSING = new Status(102, "Processing");

	/**
	 * 200 OK
	 */
	public static final Status OK = new Status(200, "OK");

	/**
	 * 201 Created
	 */
	public static final Status CREATED = new Status(201, "Created");

	/**
	 * 202 Accepted
	 */
	public static final Status ACCEPTED = new Status(202, "Accepted");

	/**
	 * 203 Non-Authoritative Information (since HTTP/1.1)
	 */
	public static final Status NON_AUTHORITATIVE_INFORMATION =
			new Status(203, "Non-Authoritative Information");

	/**
	 * 204 No Content
	 */
	public static final Status NO_CONTENT = new Status(204, "No Content");

	/**
	 * 205 Reset Content
	 */
	public static final Status RESET_CONTENT = new Status(205, "Reset Content");

	/**
	 * 206 Partial Content
	 */
	public static final Status PARTIAL_CONTENT = new Status(206, "Partial Content");

	/**
	 * 207 Multi-Status (WebDAV, RFC2518)
	 */
	public static final Status MULTI_STATUS = new Status(207, "Multi-Status");

	/**
	 * 300 Multiple Choices
	 */
	public static final Status MULTIPLE_CHOICES = new Status(300, "Multiple Choices");

	/**
	 * 301 Moved Permanently
	 */
	public static final Status MOVED_PERMANENTLY = new Status(301, "Moved Permanently");

	/**
	 * 302 Found
	 */
	public static final Status FOUND = new Status(302, "Found");

	/**
	 * 303 See Other (since HTTP/1.1)
	 */
	public static final Status SEE_OTHER = new Status(303, "See Other");

	/**
	 * 304 Not Modified
	 */
	public static final Status NOT_MODIFIED = new Status(304, "Not Modified");

	/**
	 * 305 Use Proxy (since HTTP/1.1)
	 */
	public static final Status USE_PROXY = new Status(305, "Use Proxy");

	/**
	 * 307 Temporary Redirect (since HTTP/1.1)
	 */
	public static final Status TEMPORARY_REDIRECT = new Status(307, "Temporary Redirect");

	/**
	 * 400 Bad Request
	 */
	public static final Status BAD_REQUEST = new Status(400, "Bad Request");

	/**
	 * 401 Unauthorized
	 */
	public static final Status UNAUTHORIZED = new Status(401, "Unauthorized");

	/**
	 * 402 Payment Required
	 */
	public static final Status PAYMENT_REQUIRED = new Status(402, "Payment Required");

	/**
	 * 403 Forbidden
	 */
	public static final Status FORBIDDEN = new Status(403, "Forbidden");

	/**
	 * 404 Not Found
	 */
	public static final Status NOT_FOUND = new Status(404, "Not Found");

	/**
	 * 405 Method Not Allowed
	 */
	public static final Status METHOD_NOT_ALLOWED = new Status(405, "Method Not Allowed");

	/**
	 * 406 Not Acceptable
	 */
	public static final Status NOT_ACCEPTABLE = new Status(406, "Not Acceptable");

	/**
	 * 407 Proxy Authentication Required
	 */
	public static final Status PROXY_AUTHENTICATION_REQUIRED =
			new Status(407, "Proxy Authentication Required");

	/**
	 * 408 Request Timeout
	 */
	public static final Status REQUEST_TIMEOUT = new Status(408, "Request Timeout");

	/**
	 * 409 Conflict
	 */
	public static final Status CONFLICT = new Status(409, "Conflict");

	/**
	 * 410 Gone
	 */
	public static final Status GONE = new Status(410, "Gone");

	/**
	 * 411 Length Required
	 */
	public static final Status LENGTH_REQUIRED = new Status(411, "Length Required");

	/**
	 * 412 Precondition Failed
	 */
	public static final Status PRECONDITION_FAILED =
			new Status(412, "Precondition Failed");

	/**
	 * 413 Request Entity Too Large
	 */
	public static final Status REQUEST_ENTITY_TOO_LARGE =
			new Status(413, "Request Entity Too Large");

	/**
	 * 414 Request-URI Too Long
	 */
	public static final Status REQUEST_URI_TOO_LONG =
			new Status(414, "Request-URI Too Long");

	/**
	 * 415 Unsupported Media Type
	 */
	public static final Status UNSUPPORTED_MEDIA_TYPE =
			new Status(415, "Unsupported Media Type");

	/**
	 * 416 Requested Range Not Satisfiable
	 */
	public static final Status REQUESTED_RANGE_NOT_SATISFIABLE =
			new Status(416, "Requested Range Not Satisfiable");

	/**
	 * 417 Expectation Failed
	 */
	public static final Status EXPECTATION_FAILED =
			new Status(417, "Expectation Failed");

	/**
	 * 422 Unprocessable Entity (WebDAV, RFC4918)
	 */
	public static final Status UNPROCESSABLE_ENTITY =
			new Status(422, "Unprocessable Entity");

	/**
	 * 423 Locked (WebDAV, RFC4918)
	 */
	public static final Status LOCKED =
			new Status(423, "Locked");

	/**
	 * 424 Failed Dependency (WebDAV, RFC4918)
	 */
	public static final Status FAILED_DEPENDENCY = new Status(424, "Failed Dependency");

	/**
	 * 425 Unordered Collection (WebDAV, RFC3648)
	 */
	public static final Status UNORDERED_COLLECTION =
			new Status(425, "Unordered Collection");

	/**
	 * 426 Upgrade Required (RFC2817)
	 */
	public static final Status UPGRADE_REQUIRED = new Status(426, "Upgrade Required");

	/**
	 * 428 Precondition Required (RFC6585)
	 */
	public static final Status PRECONDITION_REQUIRED =
			new Status(428, "Precondition Required");

	/**
	 * 429 Too Many Requests (RFC6585)
	 */
	public static final Status TOO_MANY_REQUESTS = new Status(429, "Too Many Requests");

	/**
	 * 431 Request Header Fields Too Large (RFC6585)
	 */
	public static final Status REQUEST_HEADER_FIELDS_TOO_LARGE =
			new Status(431, "Request Header Fields Too Large");

	/**
	 * 500 Internal Server Error
	 */
	public static final Status INTERNAL_SERVER_ERROR =
			new Status(500, "Internal Server Error");

	/**
	 * 501 Not Implemented
	 */
	public static final Status NOT_IMPLEMENTED = new Status(501, "Not Implemented");

	/**
	 * 502 Bad Gateway
	 */
	public static final Status BAD_GATEWAY = new Status(502, "Bad Gateway");

	/**
	 * 503 Service Unavailable
	 */
	public static final Status SERVICE_UNAVAILABLE =
			new Status(503, "Service Unavailable");

	/**
	 * 504 Gateway Timeout
	 */
	public static final Status GATEWAY_TIMEOUT = new Status(504, "Gateway Timeout");

	/**
	 * 505 HTTP Version Not Supported
	 */
	public static final Status HTTP_VERSION_NOT_SUPPORTED =
			new Status(505, "HTTP Version Not Supported");

	/**
	 * 506 Variant Also Negotiates (RFC2295)
	 */
	public static final Status VARIANT_ALSO_NEGOTIATES =
			new Status(506, "Variant Also Negotiates");

	/**
	 * 507 Insufficient Storage (WebDAV, RFC4918)
	 */
	public static final Status INSUFFICIENT_STORAGE =
			new Status(507, "Insufficient Storage");

	/**
	 * 510 Not Extended (RFC2774)
	 */
	public static final Status NOT_EXTENDED = new Status(510, "Not Extended");

	/**
	 * 511 Network Authentication Required (RFC6585)
	 */
	public static final Status NETWORK_AUTHENTICATION_REQUIRED =
			new Status(511, "Network Authentication Required");

	/**
	 * Returns the {@link Status} represented by the specified code.
	 * If the specified code is a standard HTTP getStatus code, a cached instance
	 * will be returned.  Otherwise, a new instance will be returned.
	 */
	public static Status valueOf(int code) {
		switch (code) {
			case 100:
				return CONTINUE;
			case 101:
				return SWITCHING_PROTOCOLS;
			case 102:
				return PROCESSING;
			case 200:
				return OK;
			case 201:
				return CREATED;
			case 202:
				return ACCEPTED;
			case 203:
				return NON_AUTHORITATIVE_INFORMATION;
			case 204:
				return NO_CONTENT;
			case 205:
				return RESET_CONTENT;
			case 206:
				return PARTIAL_CONTENT;
			case 207:
				return MULTI_STATUS;
			case 300:
				return MULTIPLE_CHOICES;
			case 301:
				return MOVED_PERMANENTLY;
			case 302:
				return FOUND;
			case 303:
				return SEE_OTHER;
			case 304:
				return NOT_MODIFIED;
			case 305:
				return USE_PROXY;
			case 307:
				return TEMPORARY_REDIRECT;
			case 400:
				return BAD_REQUEST;
			case 401:
				return UNAUTHORIZED;
			case 402:
				return PAYMENT_REQUIRED;
			case 403:
				return FORBIDDEN;
			case 404:
				return NOT_FOUND;
			case 405:
				return METHOD_NOT_ALLOWED;
			case 406:
				return NOT_ACCEPTABLE;
			case 407:
				return PROXY_AUTHENTICATION_REQUIRED;
			case 408:
				return REQUEST_TIMEOUT;
			case 409:
				return CONFLICT;
			case 410:
				return GONE;
			case 411:
				return LENGTH_REQUIRED;
			case 412:
				return PRECONDITION_FAILED;
			case 413:
				return REQUEST_ENTITY_TOO_LARGE;
			case 414:
				return REQUEST_URI_TOO_LONG;
			case 415:
				return UNSUPPORTED_MEDIA_TYPE;
			case 416:
				return REQUESTED_RANGE_NOT_SATISFIABLE;
			case 417:
				return EXPECTATION_FAILED;
			case 422:
				return UNPROCESSABLE_ENTITY;
			case 423:
				return LOCKED;
			case 424:
				return FAILED_DEPENDENCY;
			case 425:
				return UNORDERED_COLLECTION;
			case 426:
				return UPGRADE_REQUIRED;
			case 428:
				return PRECONDITION_REQUIRED;
			case 429:
				return TOO_MANY_REQUESTS;
			case 431:
				return REQUEST_HEADER_FIELDS_TOO_LARGE;
			case 500:
				return INTERNAL_SERVER_ERROR;
			case 501:
				return NOT_IMPLEMENTED;
			case 502:
				return BAD_GATEWAY;
			case 503:
				return SERVICE_UNAVAILABLE;
			case 504:
				return GATEWAY_TIMEOUT;
			case 505:
				return HTTP_VERSION_NOT_SUPPORTED;
			case 506:
				return VARIANT_ALSO_NEGOTIATES;
			case 507:
				return INSUFFICIENT_STORAGE;
			case 510:
				return NOT_EXTENDED;
			case 511:
				return NETWORK_AUTHENTICATION_REQUIRED;
		}

		final String reasonPhrase;

		if (code < 100) {
			reasonPhrase = "Unknown Status";
		} else if (code < 200) {
			reasonPhrase = "Informational";
		} else if (code < 300) {
			reasonPhrase = "Successful";
		} else if (code < 400) {
			reasonPhrase = "Redirection";
		} else if (code < 500) {
			reasonPhrase = "Client Error";
		} else if (code < 600) {
			reasonPhrase = "Server Error";
		} else {
			reasonPhrase = "Unknown Status";
		}

		return new Status(code, reasonPhrase + " (" + code + ')');
	}

	private final int code;

	private final String reasonPhrase;

	/**
	 * Creates a new instance with the specified {@code code} and its
	 * {@code reasonPhrase}.
	 */
	private Status(int code, String reasonPhrase) {
		if (code < 0) {
			throw new IllegalArgumentException(
					"code: " + code + " (expected: 0+)");
		}

		if (reasonPhrase == null) {
			throw new NullPointerException("reasonPhrase");
		}

		for (int i = 0; i < reasonPhrase.length(); i ++) {
			char c = reasonPhrase.charAt(i);
			// Check prohibited characters.
			switch (c) {
				case '\n': case '\r':
					throw new IllegalArgumentException(
							"reasonPhrase contains one of the following prohibited characters: " +
									"\\r\\n: " + reasonPhrase);
			}
		}

		this.code = code;
		this.reasonPhrase = reasonPhrase;
	}

	/**
	 * Returns the code of this {@link Status}.
	 */
	public int getCode() {
		return code;
	}

	/**
	 * Returns the reason phrase of this {@link Status}.
	 */
	public String getReasonPhrase() {
		return reasonPhrase;
	}

	@Override
	public int hashCode() {
		return code;
	}

	/**
	 * Equality of {@link Status} only depends on {@link #code}. The
	 * reason phrase is not considered for equality.
	 */
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof Status)) {
			return false;
		}

		return code == ((Status) o).code;
	}

	@Override
	public String toString() {
		StringBuilder buf = new StringBuilder(reasonPhrase.length() + 5);
		buf.append(code);
		buf.append(' ');
		buf.append(reasonPhrase);
		return buf.toString();
	}
	
}
