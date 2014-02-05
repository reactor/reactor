package reactor.io.encoding.protobuf;

import com.google.protobuf.Message;
import reactor.function.Consumer;
import reactor.function.Function;
import reactor.io.encoding.SerializationCodec;
import reactor.util.Assert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Jon Brisbin
 */
public class ProtobufCodec<IN, OUT> extends SerializationCodec<Map<Class<?>, Message>, IN, OUT> {

	public ProtobufCodec() {
		this(true);
	}

	public ProtobufCodec(boolean lengthFieldFraming) {
		super(new ConcurrentHashMap<Class<?>, Message>(), lengthFieldFraming);
	}

	@Override
	protected Function<byte[], IN> deserializer(final Map<Class<?>, Message> messages,
	                                            final Class<IN> type,
	                                            final Consumer<IN> next) {
		Assert.isAssignable(Message.class,
		                    type,
		                    "Can only deserialize Protobuf messages. " +
				                    type.getName() +
				                    " is not an instance of " +
				                    Message.class.getName());
		return new Function<byte[], IN>() {
			@SuppressWarnings("unchecked")
			@Override
			public IN apply(byte[] bytes) {
				try {
					Message msg = messages.get(type);
					if(null == msg) {
						msg = (Message)type.getMethod("getDefaultInstance").invoke(null);
						messages.put(type, msg);
					}
					IN obj = (IN)msg.newBuilderForType().mergeFrom(bytes).build();
					if(null != next) {
						next.accept(obj);
						return null;
					} else {
						return obj;
					}
				} catch(Exception e) {
					throw new IllegalStateException(e.getMessage(), e);
				}
			}
		};
	}

	@Override
	protected Function<OUT, byte[]> serializer(final Map<Class<?>, Message> messages) {
		return new Function<OUT, byte[]>() {
			@Override
			public byte[] apply(Object o) {
				Assert.isInstanceOf(Message.class,
				                    o,
				                    "Can only serialize Protobuf messages. " +
						                    o.getClass().getName() +
						                    " is not an instance of " +
						                    Message.class.getName());
				return ((Message)o).toByteArray();
			}
		};
	}

}
