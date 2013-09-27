/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor.tcp.spec;

import reactor.function.Supplier;
import reactor.function.Suppliers;
import reactor.tcp.Reconnect;
import reactor.tuple.Tuple;
import reactor.tuple.Tuple2;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;

/**
 * A helper class for configure a new {@code Reconnect}.
 *
 */
public class IncrementalBackoffReconnectSpec implements Supplier<Reconnect> {
    public static final int DEFAULT_INTERVAL     = 5000;
    public static final int DEFAULT_MULTIPLIER   = 1;
    public static final int DEFAULT_MAX_ATTEMPTS = -1;

    private final List<InetSocketAddress> addresses;
    private long interval;
    private long multiplier;
    private long maxInterval;
    private long maxAttempts;

    /**
     *
     */
    public IncrementalBackoffReconnectSpec() {
        this.addresses   = new LinkedList<InetSocketAddress>();
        this.interval    = DEFAULT_INTERVAL;
        this.multiplier  = DEFAULT_MULTIPLIER;
        this.maxInterval = Long.MAX_VALUE;
        this.maxAttempts = DEFAULT_MAX_ATTEMPTS;
    }

    /**
     * Set the reconnection interval.
     *
     * @param interval the period reactor waits between attemps to reconnect disconnected peers
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec interval(long interval) {
        this.interval = interval;
        return this;
    }

    /**
     * Set the maximum reconnection interval that will be applied if the multiplier
     * is set to a value greather than one.
     *
     * @param maxInterval
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec maxInterval(long maxInterval) {
        this.maxInterval = maxInterval;
        return this;
    }

    /**
     * Set the backoff multiplier.
     *
     * @param multiplier
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec multiplier(long multiplier) {
        this.multiplier = multiplier;
        return this;
    }

    /**
     * Sets the number of time that Reactor will attempt to connect or reconnect
     * before giving up.
     *
     * @param maxAttempts The max number of attempts made before failing.
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec maxAttempts(long maxAttempts) {
        this.maxAttempts = maxAttempts;
        return this;
    }

    /**
     * Add an address to the pool of addresses.
     *
     * @param address
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec address(InetSocketAddress address) {
        this.addresses.add(address);
        return this;
    }

    /**
     * Add an address to the pool of addresses.
     *
     * @param host
     * @param port
     * @return {@literal this}
     */
    public IncrementalBackoffReconnectSpec address(String host, int port) {
        this.addresses.add(new InetSocketAddress(host,port));
        return this;
    }


    @Override
    public Reconnect get() {
        if(!addresses.isEmpty()) {
            final Supplier<InetSocketAddress> endpoints =
                Suppliers.roundRobin(addresses.toArray(new InetSocketAddress[]{}));

            return new Reconnect() {
                public Tuple2<InetSocketAddress, Long> reconnect(InetSocketAddress currentAddress, int attempt) {
                    Tuple2<InetSocketAddress, Long> rv = null;
                    synchronized(IncrementalBackoffReconnectSpec.this) {
                        if(IncrementalBackoffReconnectSpec.this.maxAttempts == -1       ||
                           IncrementalBackoffReconnectSpec.this.maxAttempts > attempt ) {
                            rv = Tuple.of(
                                endpoints.get(),
                                (IncrementalBackoffReconnectSpec.this.multiplier > 1)
                                    ? Math.min(
                                        IncrementalBackoffReconnectSpec.this.maxInterval,
                                        IncrementalBackoffReconnectSpec.this.interval * attempt)
                                    : IncrementalBackoffReconnectSpec.this.interval);
                        }
                    }

                    return rv;
                }
            };
        } else {
            throw new RuntimeException("...");
        }
    }
}

