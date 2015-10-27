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
package reactor.io.net.impl.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.ReferenceCountUtil;
import reactor.io.buffer.Buffer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class NettyBuffer extends Buffer {

	private final ByteBuf byteBuf;
	private final Object msg;
	private final ChannelHandlerContext ctx;


	public static NettyBuffer create(Object o){
		return create(o, null);
	}


	public static NettyBuffer create(Object o, ChannelHandlerContext ctx){
		return new NettyBuffer(ctx, o);
	}

	@SuppressWarnings("unchecked")
	NettyBuffer(ChannelHandlerContext ctx, Object msg) {
		this.msg = msg;
		this.ctx = ctx;
		if(ByteBuf.class.isAssignableFrom(msg.getClass())){
			ReferenceCountUtil.retain(msg);
			this.byteBuf = (ByteBuf)msg;
			this.buffer = byteBuf.nioBuffer();
		}
		else{
			this.byteBuf = null;
		}
	}

	/**
	 *
	 * @param clazz
	 * @param <T>
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public <T> T get(Class<T> clazz){
		if(clazz.isAssignableFrom(msg.getClass())){
			return (T)msg;
		}
		else {
			return null;
		}
	}

	/**
	 *
	 * @return
	 */
	public Object get() {
		return msg;
	}

	/**
	 *
	 * @return
	 */
	public ByteBuf getByteBuf(){
		return byteBuf;
	}

	/**
	 *
	 * @return
	 */
	public ChannelHandlerContext getCtx() {
		return ctx;
	}


}
