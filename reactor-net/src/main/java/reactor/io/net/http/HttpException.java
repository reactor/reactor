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

package reactor.io.net.http;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * An error for signalling that an error occurred during a communication over HTTP protocol
 *
 * @author Anatoly Kadyshev
 */
public class HttpException extends Exception {

    private final HttpResponseStatus responseStatus;

    public HttpException(HttpResponseStatus responseStatus) {
        super("HTTP request failed with code: " + responseStatus.code());
        this.responseStatus = responseStatus;
    }

    public HttpResponseStatus getResponseStatus() {
        return responseStatus;
    }
}
