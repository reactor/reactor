package reactor.io;

import reactor.alloc.Allocator;
import reactor.alloc.Reference;
import reactor.alloc.ReferenceCountingAllocator;
import reactor.function.Supplier;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * An {@link reactor.alloc.Allocator} implementation that allocates {@link reactor.io.Buffer Buffers}.
 *
 * @author Jon Brisbin
 */
public class BufferAllocator implements Allocator<Buffer> {

	private final Allocator<Buffer> delegate;

	/**
	 * Create a {@code BufferAllocator} of size=256, direct=false, and bufferSize=Buffer.SMALL_BUFFER_SIZE.
	 */
	public BufferAllocator() {
		this(256, false, Buffer.SMALL_BUFFER_SIZE);
	}

	/**
	 * Create a {@code BufferAllocator}.
	 *
	 * @param poolSize
	 * 		The number of Buffers to keep on hand.
	 * @param direct
	 * 		Whether or not to use direct buffers.
	 * @param bufferSize
	 * 		The size of the buffers.
	 */
	public BufferAllocator(int poolSize, final boolean direct, final int bufferSize) {
		this.delegate = new ReferenceCountingAllocator<Buffer>(
				poolSize,
				new Supplier<Buffer>() {
					@Override
					public Buffer get() {
						return new Buffer(direct
						                  ? ByteBuffer.allocateDirect(bufferSize)
						                  : ByteBuffer.allocate(bufferSize));
					}
				}
		);
	}

	@Override
	public Reference<Buffer> allocate() {
		return delegate.allocate();
	}

	@Override
	public List<Reference<Buffer>> allocateBatch(int size) {
		return delegate.allocateBatch(size);
	}

	@Override
	public void release(List<Reference<Buffer>> batch) {
		delegate.release(batch);
	}

}
