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

package reactor.io.codec.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.UnsafeMemoryInput;
import com.esotericsoftware.kryo.io.UnsafeMemoryOutput;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.buffer.Buffer;
import reactor.io.codec.SerializationCodec;

/**
 * @author Jon Brisbin
 * @author Khayretdinov Dmitriy
 */
public class KryoPoolCodec<IN, OUT> extends SerializationCodec<KryoPool, IN, OUT> {
    public KryoPoolCodec() {
        this(new KryoFactory() {
            @Override
            public Kryo create() {
                return new Kryo();
            }
        }, true);
    }

    public KryoPoolCodec(KryoFactory kryoFactory, boolean lengthFieldFraming) {
        this(new KryoPool.Builder(kryoFactory).softReferences().build(), lengthFieldFraming);
    }

    public KryoPoolCodec(KryoPool engine, boolean lengthFieldFraming) {
        super(engine, lengthFieldFraming);
    }

    @Override
    protected Function<byte[], IN> deserializer(final KryoPool engine,
                                                final Class<IN> type,
                                                final Consumer<IN> next) {
        return new Function<byte[], IN>() {
            @Override
            public IN apply(byte[] bytes) {
                final Kryo kryo = engine.borrow();
                try {
                    IN obj = kryo.readObject(new UnsafeMemoryInput(bytes), type);
                    if (null != next) {
                        next.accept(obj);
                        return null;
                    } else {
                        return obj;
                    }
                } finally {
                    engine.release(kryo);
                }
            }
        };
    }

    @Override
    protected Function<OUT, byte[]> serializer(final KryoPool engine) {
        return new Function<OUT, byte[]>() {
            @Override
            public byte[] apply(OUT o) {
                final Kryo kryo = engine.borrow();
                try {
                    UnsafeMemoryOutput out = new UnsafeMemoryOutput(Buffer.SMALL_BUFFER_SIZE, Buffer.MAX_BUFFER_SIZE);
                    kryo.writeObject(out, o);
                    out.flush();
                    return out.toBytes();
                } finally {
                    engine.release(kryo);
                }
            }
        };
    }
}
