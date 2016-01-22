/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.io.buffer;

import java.nio.ByteBuffer;
import java.util.List;

import reactor.alloc.Allocator;
import reactor.alloc.Reference;
import reactor.alloc.ReferenceCountingAllocator;
import reactor.core.util.PlatformDependent;
import reactor.fn.Supplier;

/**
 * An {@link reactor.alloc.Allocator} implementation that allocates {@link Buffer Buffers}.
 *
 * @author Jon Brisbin
 */
public class BufferAllocator implements Allocator<Buffer> {

	private final Allocator<Buffer> delegate;

	/**
	 * Create a {@code BufferAllocator} of size=256, direct=false, and bufferSize=Buffer.SMALL_IO_BUFFER_SIZE.
	 */
	public BufferAllocator() {
		this(256, false, PlatformDependent.SMALL_IO_BUFFER_SIZE);
	}

	/**
	 * Create a {@code BufferAllocator}.
	 *
	 * @param poolSize   The number of Buffers to keep on hand.
	 * @param direct     Whether or not to use direct buffers.
	 * @param bufferSize The size of the buffers.
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
