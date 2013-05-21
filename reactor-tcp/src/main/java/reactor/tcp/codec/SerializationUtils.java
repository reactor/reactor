package reactor.tcp.codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import reactor.support.Assert;

public class SerializationUtils {

	public static void serialize(Object object, OutputStream outputStream) throws IOException {
		Assert.isInstanceOf(Serializable.class, object, object.getClass() + " is not Serializable");
		ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
		objectOutputStream.writeObject(object);
		objectOutputStream.flush();
	}

	public static Object deserialize(InputStream inputStream) throws IOException {
		ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
		try {
			return objectInputStream.readObject();
		}
		catch (ClassNotFoundException ex) {
			throw new RuntimeException("Deserialization failed");
		}
	}


}
