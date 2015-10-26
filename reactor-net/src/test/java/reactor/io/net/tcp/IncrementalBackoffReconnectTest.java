/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.io.net.tcp;

import java.net.InetSocketAddress;

import org.junit.Test;
import reactor.fn.tuple.Tuple2;
import reactor.io.net.ReactiveNet;
import reactor.io.net.Reconnect;
import reactor.io.net.Spec;

import static org.junit.Assert.assertEquals;

public class IncrementalBackoffReconnectTest {
	@Test
	public void testDefaultReconnect() {
		Reconnect rec = ReactiveNet.backoffReconnect().get();

		InetSocketAddress a1 = new InetSocketAddress("129.168.0.1", 1001);
		Tuple2<InetSocketAddress, Long> t1 = rec.reconnect(a1, 0);

		assertEquals(Spec.IncrementalBackoffReconnect.DEFAULT_INTERVAL, t1.getT2().longValue());
		assertEquals(a1, t1.getT1());

		InetSocketAddress a2 = new InetSocketAddress("129.168.0.1", 1001);
		Tuple2<InetSocketAddress, Long> t2 = rec.reconnect(a1, 0);

		assertEquals(Spec.IncrementalBackoffReconnect.DEFAULT_INTERVAL, t2.getT2().longValue());
		assertEquals(a2, t2.getT1());
	}

	@Test
	public void testReconnectIntervalWithCap() {
		InetSocketAddress addr1 = new InetSocketAddress("129.168.0.1", 1001);

		Reconnect rec = ReactiveNet.backoffReconnect()
		  .address(addr1)
		  .interval(5000)
		  .maxInterval(10000)
		  .multiplier(2)
		  .get();

		assertEquals(0L, (long) rec.reconnect(addr1, 0).getT2());
		assertEquals(5000L, (long) rec.reconnect(addr1, 1).getT2());
		assertEquals(10000L, (long) rec.reconnect(addr1, 2).getT2());
		assertEquals(10000L, (long) rec.reconnect(addr1, 3).getT2());
	}

	@Test
	public void testRoundRobinAddresses() {
		InetSocketAddress addr1 = new InetSocketAddress("129.168.0.1", 1001);
		InetSocketAddress addr2 = new InetSocketAddress("129.168.0.2", 1002);
		InetSocketAddress addr3 = new InetSocketAddress("129.168.0.3", 1003);

		Reconnect rec = ReactiveNet.backoffReconnect()
		  .address(addr1)
		  .address(addr2)
		  .address(addr3)
		  .get();

		assertEquals(addr1, rec.reconnect(addr1, 0).getT1());
		assertEquals(addr2, rec.reconnect(addr2, 1).getT1());
		assertEquals(addr3, rec.reconnect(addr3, 2).getT1());
	}
}
