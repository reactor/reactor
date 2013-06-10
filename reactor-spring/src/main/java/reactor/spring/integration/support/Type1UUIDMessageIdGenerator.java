package reactor.spring.integration.support;

import org.springframework.integration.MessageHeaders;

import java.util.UUID;

/**
 * @author Jon Brisbin
 */
public class Type1UUIDMessageIdGenerator implements MessageHeaders.IdGenerator {

	@Override
	public UUID generateId() {
		com.eaio.uuid.UUID uuid = new com.eaio.uuid.UUID();
		return new UUID(uuid.getClockSeqAndNode(), uuid.getTime());
	}

}
