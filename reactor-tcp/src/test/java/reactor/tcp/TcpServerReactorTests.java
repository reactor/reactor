package reactor.tcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.net.SocketFactory;

import org.junit.Test;

import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.tcp.codec.LineFeedCodecSupplier;
import reactor.tcp.test.TestUtils;
import reactor.tcp.test.TimeoutUtils;
import reactor.tcp.test.TimeoutUtils.TimeoutCallback;

public class TcpServerReactorTests {

	@Test
	public void tcpServerReactorCanReceiveRequestsAndNotifyConsumers() throws IOException, InterruptedException {

		int port = TestUtils.findAvailableServerSocket();

		TcpServerReactor<String> reactor = new TcpServerReactor<String>(port, new LineFeedCodecSupplier());
		reactor.start();

		final CountDownLatch latch = new CountDownLatch(10);

		reactor.onRequest(new Consumer<Event<String>>() {

			@Override
			public void accept(Event<String> t) {
				latch.countDown();
			}
		});

		awaitAlive(reactor);

		Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
		OutputStream outputStream = socket.getOutputStream();

		for (int i = 0; i < 10; i++) {
			outputStream.write(("" + i + "\n").getBytes());
		}

		try {
			assertTrue(latch.await(5, TimeUnit.SECONDS));
		} finally {
			outputStream.close();
			socket.close();
			reactor.stop();
		}
	}

	@Test
	public void tcpServerReactorCanReceiveRequestsNotifyConsumersAndSendResponses() throws IOException, InterruptedException {

		int port = TestUtils.findAvailableServerSocket();

		final TcpServerReactor<String> reactor = new TcpServerReactor<String>(port, new LineFeedCodecSupplier());
		reactor.start();

		reactor.onRequest(new Consumer<Event<String>>() {

			@Override
			public void accept(Event<String> request) {
				Event<byte[]> response = new Event<byte[]>(request.getHeaders(), ("Response " + request.getData()).getBytes());
				reactor.notify(request.getReplyTo(), response);
			}
		});

		awaitAlive(reactor);

		Socket socket = SocketFactory.getDefault().createSocket("localhost", port);
		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		OutputStream outputStream = socket.getOutputStream();

		for (int i = 0; i < 10; i++) {
			outputStream.write(("" + i + "\n").getBytes());
		}

		int lines = 0;

		while (lines < 10 && reader.readLine() != null) {
			lines++;
		}

		try {
			assertEquals(lines, 10);
		} finally {
			outputStream.close();
			socket.close();
			reactor.stop();
		}
	}

	private void awaitAlive(final TcpServerReactor<?> reactor) throws InterruptedException {
		TimeoutUtils.doWithTimeout(30000, new TimeoutCallback() {

			@Override
			public boolean isComplete() {
				return reactor.isAlive();
			}
		});
	}
}
