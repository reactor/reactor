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
public class ProtobufCodec extends SerializationCodec<Map<Class<?>, Message>> {

	public ProtobufCodec() {
		super(new ConcurrentHashMap<Class<?>, Message>());
	}

	@Override
	protected Function<byte[], Object> deserializer(final Map<Class<?>, Message> messages,
	                                                final Class<?> type,
	                                                final Consumer<Object> next) {
		Assert.isAssignable(Message.class,
		                    type,
		                    "Can only deserialize Protobuf messages. " +
				                    type.getName() +
				                    " is not an instance of " +
				                    Message.class.getName());
		return new Function<byte[], Object>() {
			@Override
			public Object apply(byte[] bytes) {
				try {
					Message msg = messages.get(type);
					if(null == msg) {
						msg = (Message)type.getMethod("getDefaultInstance").invoke(null);
						messages.put(type, msg);
					}
					Object obj = msg.newBuilderForType().mergeFrom(bytes).build();
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
	protected Function<Object, byte[]> serializer(final Map<Class<?>, Message> messages) {
		return new Function<Object, byte[]>() {
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
