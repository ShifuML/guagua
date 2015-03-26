/*
 * Copyright [2013-2015] eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.shifu.guagua.io;

import static org.jboss.netty.buffer.ChannelBuffers.dynamicBuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;

/**
 * A encoder to encode {@link Bytable} instance to coded bytes array for netty pipeline.
 */
public class NettyBytableEncoder extends ObjectEncoder {

    private static final byte[] LENGTH_PLACEHOLDER = new byte[4];

    /**
     * Bytable serializer used to serialize {@link Bytable} message on Netty pipeline.
     */
    private BytableSerializer<Bytable> serializer;

    /**
     * Estimated length for message.
     */
    private final int estimatedLength;

    /**
     * Creates a new encoder with the estimated length of 64k bytes.
     */
    public NettyBytableEncoder() {
        this(64 * 1024);
    }

    /**
     * Creates a new encoder.
     * 
     * @param estimatedLength
     *            the estimated byte length of the serialized form of an object.If the length of the serialized form
     *            exceeds this value, the internal buffer will be expanded automatically at the cost of memory
     *            bandwidth. If this value is too big, it will also waste memory bandwidth. To avoid unnecessary memory
     *            copy or allocation cost, please specify the properly estimated value.
     */
    public NettyBytableEncoder(int estimatedLength) {
        if(estimatedLength < 0) {
            throw new IllegalArgumentException("estimatedLength: " + estimatedLength);
        }
        this.estimatedLength = estimatedLength;
        this.serializer = new BytableSerializer<Bytable>();
    }

    @Override
    protected Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if(!(msg instanceof Bytable)) {
            throw new IllegalStateException("Only Bytable message can be encoded.");
        }
        ChannelBufferOutputStream bout = new ChannelBufferOutputStream(dynamicBuffer(estimatedLength, ctx.getChannel()
                .getConfig().getBufferFactory()));
        bout.write(LENGTH_PLACEHOLDER);
        byte[] classNameBytes = msg.getClass().getName().getBytes("UTF-8");
        bout.writeInt(classNameBytes.length);
        bout.write(classNameBytes);
        bout.write(this.serializer.objectToBytes((Bytable) msg));
        ChannelBuffer encoded = bout.buffer();
        encoded.setInt(0, encoded.writerIndex() - 4);
        return encoded;
    }
}
