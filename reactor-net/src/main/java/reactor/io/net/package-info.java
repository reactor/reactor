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

/**
 * Reactive network components are located in this package scope implementing the following exposed contract:
 * A {@link reactor.io.netty.ReactorPeer} NetServer/NetClient is a {@link org.reactivestreams.Publisher} of
 * {@link reactor.io.netty.ReactorChannel} that are themselves {@link org.reactivestreams.Publisher} of input data.
 * This input data will be the received information from a Server perspective and response information from a Client perspective.
 * A channel also expose useful methods to write, close and generally control the lifecycle of the underlying connection.
 */
package reactor.io.net;