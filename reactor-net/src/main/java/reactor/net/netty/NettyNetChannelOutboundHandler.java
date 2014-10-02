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

package reactor.net.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import reactor.tuple.Tuple2;

/**
 * @author Jon Brisbin
 */
public class NettyNetChannelOutboundHandler extends ChannelOutboundHandlerAdapter {
	@SuppressWarnings("unchecked")
	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		if (Tuple2.class.isInstance(msg)) {
			Tuple2<Object, Boolean> tup = (Tuple2<Object, Boolean>) msg;
			if (null != tup.getT1()) {
				super.write(ctx, tup.getT1(), promise);
			}
			if (tup.getT2()) {
				ctx.flush();
			}
		} else {
			super.write(ctx, msg, promise);
			ctx.flush();
		}
	}
}
