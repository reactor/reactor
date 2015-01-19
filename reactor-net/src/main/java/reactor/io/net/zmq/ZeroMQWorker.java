/*
 * Copyright (c) 2011-2014 Pivotal Software, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package reactor.io.net.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZLoop;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;
import reactor.rx.action.Broadcaster;

import java.util.UUID;

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public abstract class ZeroMQWorker implements Runnable {

	private final Logger log   = LoggerFactory.getLogger(getClass());
	private final ZLoop  zloop = new ZLoop();

	private final UUID                id;
	private final int                 socketType;
	private final int                 ioThreadCount;
	private final ZLoop.IZLoopHandler inputHandler;
	private final Broadcaster<ZMsg>   b;

	private volatile boolean      closed;
	private volatile boolean      shutdownCtx;
	private volatile ZContext     zmq;
	private volatile ZMQ.Socket   socket;
	private volatile ZMQ.PollItem pollin;

	public ZeroMQWorker(UUID id, int socketType, int ioThreadCount, ZContext zmq, final Broadcaster<ZMsg> b) {
		this.id = id;
		this.socketType = socketType;
		this.ioThreadCount = ioThreadCount;
		this.zmq = zmq;
		this.b = b;
		//FIXME must be serialized

		this.inputHandler = new ZLoop.IZLoopHandler() {
			@Override
			public int handle(ZLoop loop, ZMQ.PollItem item, Object arg) {
				ZMsg msg = ZMsg.recvMsg(socket);
				if (null == msg || msg.size() == 0) {
					return 0;
				}
				if (closed) {
					return -1;
				}

				b.onNext(msg);

				return 0;
			}
		};
	}

	@Override
	public void run() {
		try {
			if (closed) {
				return;
			}
			if (null == zmq) {
				zmq = new ZContext(ioThreadCount);
				shutdownCtx = true;
			}
			socket = zmq.createSocket(socketType);
			socket.setIdentity(id.toString().getBytes());
			configure(socket);

			pollin = new ZMQ.PollItem(socket, ZMQ.Poller.POLLIN);
			if (log.isTraceEnabled()) {
				zloop.verbose(true);
			}
			zloop.addPoller(pollin, inputHandler, null);

			start(socket);

			zloop.start();

			zmq.destroySocket(socket);
		}catch (Exception e){
			b.onError(e);
		}
	}

	public void shutdown() {
		if (closed) {
			return;
		}
		zloop.removePoller(pollin);
		zloop.destroy();

		closed = true;

		if (shutdownCtx) {
			zmq.destroy();
		}

		b.onComplete();
	}

	protected abstract void configure(ZMQ.Socket socket);

	protected abstract void start(ZMQ.Socket socket);
}
