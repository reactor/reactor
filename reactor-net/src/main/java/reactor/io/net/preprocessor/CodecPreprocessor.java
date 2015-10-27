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
package reactor.io.net.preprocessor;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.publisher.convert.DependencyUtils;
import reactor.core.support.Bounded;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.codec.DelimitedCodec;
import reactor.io.codec.StandardCodecs;
import reactor.io.codec.StringCodec;
import reactor.io.codec.compress.GzipCodec;
import reactor.io.codec.json.JsonCodec;
import reactor.io.net.Preprocessor;
import reactor.io.net.ReactiveChannel;
import reactor.io.net.http.HttpChannel;
import reactor.io.net.http.HttpProcessor;
import reactor.io.net.http.model.HttpHeaders;
import reactor.io.net.http.model.Method;
import reactor.io.net.http.model.Protocol;
import reactor.io.net.http.model.ResponseHeaders;
import reactor.io.net.http.model.Status;
import reactor.io.net.http.model.Transfer;

/**
 * @author Stephane Maldini
 * @since 2.1
 */
public final class CodecPreprocessor<IN, OUT>
		implements Preprocessor<Buffer, Buffer, ReactiveChannel<Buffer, Buffer>, IN, OUT, ReactiveChannel<IN, OUT>>,
		           HttpProcessor<Buffer, Buffer, HttpChannel<Buffer, Buffer>, IN, OUT, HttpChannel<IN, OUT>> {

	static {
		if (!DependencyUtils.hasReactorCodec()) {
			throw new IllegalStateException("io.projectreactor:reactor-codec:" + DependencyUtils.reactorVersion() +
					" dependency is missing from the classpath.");
		}

	}


	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<String, String> delimitedString(){
		return from(StandardCodecs.DELIMITED_STRING_CODEC);
	}

	/**
	 *
	 * @param charset
	 * @return
	 */
	static public CodecPreprocessor<String, String> delimitedString(Charset charset){
		return delimitedString(charset, Codec.DEFAULT_DELIMITER);
	}

	/**
	 *
	 * @param charset
	 * @param delimiter
	 * @return
	 */
	static public CodecPreprocessor<String, String> delimitedString(Charset charset, byte delimiter){
		return from(new DelimitedCodec<>(new StringCodec(delimiter, charset)));
	}

	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<String, String> linefeed(){
		return from(StandardCodecs.LINE_FEED_CODEC);
	}

	/**
	 *
	 * @param charset
	 * @return
	 */
	static public CodecPreprocessor<String, String> linefeed(Charset charset){
		return from(new DelimitedCodec<>(new StringCodec(charset)));
	}

	/**
	 *
	 * @param charset
	 * @param delimiter
	 * @return
	 */
	static public CodecPreprocessor<String, String> linefeed(Charset charset, byte delimiter){
		return linefeed(charset, delimiter, true);
	}

	/**
	 *
	 * @param charset
	 * @param delimiter
	 * @param stripDelimiter
	 * @return
	 */
	static public CodecPreprocessor<String, String> linefeed(Charset charset, byte delimiter, boolean stripDelimiter){
		return from(new DelimitedCodec<>(delimiter, stripDelimiter, new StringCodec(charset)));
	}

	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<Buffer, Buffer> passthrough(){
		return from(StandardCodecs.PASS_THROUGH_CODEC);
	}

	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<byte[], byte[]> byteArray(){
		return from(StandardCodecs.BYTE_ARRAY_CODEC);
	}

	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<String, String> string(){
		return from(StandardCodecs.DELIMITED_STRING_CODEC);
	}

	/**
	 *
	 * @param charset
	 * @return
	 */
	static public CodecPreprocessor<String, String> string(Charset charset){
		return from(new StringCodec(charset));
	}

	/**
	 *
	 * @param tClass
	 * @param <T>
	 *
	 * @return
	 */
	static public <T> CodecPreprocessor<T, T> json(Class<T> tClass){
		return from(new JsonCodec<T, T>(tClass));
	}

	/**
	 *
	 * @return
	 */
	static public CodecPreprocessor<Buffer, Buffer> gzip(){
		return from(new GzipCodec<>(StandardCodecs.PASS_THROUGH_CODEC));
	}

	/**
	 *
	 * @param encoder
	 * @param decoder
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	static public <IN,OUT> CodecPreprocessor<IN, OUT> from(Codec<Buffer, IN, ?> encoder,
			Codec<Buffer, ?, OUT> decoder){
		return new CodecPreprocessor<>(encoder, decoder);
	}

	/**
	 *
	 * @param codec
	 * @param <IN>
	 * @param <OUT>
	 * @return
	 */
	static public <IN,OUT> CodecPreprocessor<IN, OUT> from(Codec<Buffer, IN, OUT> codec){
		return from(codec, codec);
	}

	private final Codec<Buffer, IN, ?> decoder;
	private final Codec<Buffer, ?, OUT> encoder;

	private CodecPreprocessor(
			Codec<Buffer, IN, ?> decoder,
			Codec<Buffer, ?, OUT> encoder
	) {
		this.encoder = encoder;
		this.decoder = decoder;
	}

	@Override
	public ReactiveChannel<IN, OUT> apply(final ReactiveChannel<Buffer, Buffer> channel) {
		if(channel == null) return null;
		return new CodecChannel<>(decoder, encoder, channel);
	}

	@Override
	public HttpChannel<IN, OUT> transform(HttpChannel<Buffer, Buffer> channel) {
		if(channel == null) return null;
		return new CodecHttpChannel<>(decoder, encoder, channel);
	}

	private static class CodecChannel<IN, OUT> implements ReactiveChannel<IN, OUT> {

		private final Codec<Buffer, IN, ?> decoder;
		private final Codec<Buffer, ?, OUT> encoder;

		private final ReactiveChannel<Buffer, Buffer> channel;

		public CodecChannel(Codec<Buffer, IN, ?> decoder, Codec<Buffer, ?, OUT> encoder,
				ReactiveChannel<Buffer, Buffer> channel) {
			this.encoder = encoder;
			this.decoder = decoder;
			this.channel = channel;
		}

		@Override
		public InetSocketAddress remoteAddress() {
			return channel.remoteAddress();
		}

		@Override
		public Publisher<Void> writeWith(Publisher<? extends OUT> dataStream) {
			return channel.writeWith(encoder.encode(dataStream));
		}

		@Override
		public Publisher<IN> input() {
			return decoder.decode(channel.input());
		}

		@Override
		public ConsumerSpec on() {
			return channel.on();
		}

		@Override
		public Object delegate() {
			return channel.delegate();
		}

		@Override
		public Publisher<Void> writeBufferWith(Publisher<? extends Buffer> dataStream) {
			return channel.writeWith(dataStream);
		}
	}

	private final static class CodecHttpChannel<IN, OUT> extends HttpChannel<IN, OUT> {

		private final Codec<Buffer, IN, ?> decoder;
		private final Codec<Buffer, ?, OUT> encoder;

		private final HttpChannel<Buffer, Buffer> channel;

		public CodecHttpChannel(Codec<Buffer, IN, ?> decoder,
				Codec<Buffer, ?, OUT> encoder, HttpChannel<Buffer, Buffer> channel) {
			super(channel.getCapacity());
			this.decoder = decoder;
			this.encoder = encoder;
			this.channel = channel;
		}

		@Override
		public HttpHeaders headers() {
			return channel.headers();
		}

		@Override
		public boolean isKeepAlive() {
			return channel.isKeepAlive();
		}

		@Override
		public HttpChannel<IN, OUT> keepAlive(boolean keepAlive) {
			channel.keepAlive(keepAlive);
			return this;
		}

		@Override
		public HttpChannel<IN, OUT> addHeader(String name, String value) {
			channel.addHeader(name, value);
			return this;
		}

		@Override
		public Protocol protocol() {
			return channel.protocol();
		}

		@Override
		public String uri() {
			return channel.uri();
		}

		@Override
		public Method method() {
			return channel.method();
		}

		@Override
		public Status responseStatus() {
			return channel.responseStatus();
		}

		@Override
		public HttpChannel<IN, OUT> responseStatus(Status status) {
			channel.responseStatus(status);
			return this;
		}

		@Override
		public HttpChannel<IN, OUT> paramsResolver(
				Function<? super String, Map<String, Object>> headerResolver) {
			channel.paramsResolver(headerResolver);
			return this;
		}

		@Override
		public ResponseHeaders responseHeaders() {
			return channel.responseHeaders();
		}

		@Override
		public HttpChannel<IN, OUT> addResponseHeader(String name, String value) {
			channel.addResponseHeader(name, value);
			return this;
		}

		@Override
		public Publisher<Void> writeHeaders() {
			return channel.writeHeaders();
		}

		@Override
		public HttpChannel<IN, OUT> sse() {
			channel.sse();
			return this;
		}

		@Override
		public Transfer transfer() {
			return channel.transfer();
		}

		@Override
		public HttpChannel<IN, OUT> transfer(Transfer transfer) {
			channel.transfer(transfer);
			return this;
		}

		@Override
		public boolean isWebsocket() {
			return channel.isWebsocket();
		}

		@Override
		public boolean isExposedToOverflow(Bounded parentPublisher) {
			return channel.isExposedToOverflow(parentPublisher);
		}

		@Override
		public long getCapacity() {
			return channel.getCapacity();
		}

		@Override
		public InetSocketAddress remoteAddress() {
			return channel.remoteAddress();
		}

		@Override
		public Publisher<Void> writeBufferWith(Publisher<? extends Buffer> dataStream) {
			return channel.writeBufferWith(dataStream);
		}

		@Override
		public Publisher<IN> input() {
			return decoder.decode(channel.input());
		}

		@Override
		public ConsumerSpec on() {
			return channel.on();
		}

		@Override
		public Object delegate() {
			return channel.delegate();
		}

		@Override
		protected void doHeader(String name, String value) {
		}

		@Override
		protected void doAddHeader(String name, String value) {
		}

		@Override
		protected void doResponseStatus(Status status) {
		}

		@Override
		protected void doResponseHeader(String name, String value) {
		}

		@Override
		protected void doAddResponseHeader(String name, String value) {
		}

		@Override
		protected void doSubscribeHeaders(Subscriber<? super Void> s) {
			channel.writeHeaders().subscribe(s);
		}

		@Override
		public HttpChannel<IN, OUT> header(String name, String value) {
			channel.header(name, value);
			return this;
		}

		@Override
		public Object param(String key) {
			return channel.param(key);
		}

		@Override
		public HttpChannel<IN, OUT> responseHeader(String name, String value) {
			channel.responseHeader(name, value);
			return this;
		}

		@Override
		public Map<String, Object> params() {
			return channel.params();
		}

		@Override
		public Publisher<Void> writeWith(Publisher<? extends OUT> source) {
			return channel.writeWith(encoder.encode(source));
		}

		@Override
		protected Publisher<Void> writeWithAfterHeaders(Publisher<? extends OUT> dataStream) {
			return channel.writeWith(encoder.encode(dataStream));
		}
	}
}
