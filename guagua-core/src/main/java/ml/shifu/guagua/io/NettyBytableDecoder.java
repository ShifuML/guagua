/*
 * Copyright [2013-2015] PayPal Software Foundation
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

import java.io.StreamCorruptedException;
import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;

/**
 * A decoder to decode bytes array to {@link Bytable} object.
 */
public class NettyBytableDecoder extends LengthFieldBasedFrameDecoder {

    private BytableSerializer<Bytable> serializer;

    /**
     * Default constructor with max object size 64M.
     */
    public NettyBytableDecoder() {
        this(256 * 1024 * 1024);
    }

    /**
     * Creates a new decoder with the specified maximum object size.
     * 
     * @param maxObjectSize
     *            the maximum byte length of the serialized object. if the length of the received object is greater than
     *            this value, {@link StreamCorruptedException} will be raised.
     */
    public NettyBytableDecoder(int maxObjectSize) {
        super(maxObjectSize, 0, 4, 0, 4);
        this.serializer = new BytableSerializer<Bytable>();
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        ChannelBuffer frame = (ChannelBuffer) super.decode(ctx, channel, buffer);
        if(frame == null) {
            return null;
        }
        int classNameSize = frame.readInt();
        byte[] classNameBytes = new byte[classNameSize];
        frame.readBytes(classNameBytes);
        String className = new String(classNameBytes, Charset.forName("UTF-8"));
        int readableBytes = frame.readableBytes();
        byte[] objectBytes = new byte[readableBytes];
        frame.readBytes(objectBytes);
        Bytable bytesToObject = this.serializer.bytesToObject(objectBytes, className);
        return bytesToObject;
    }

    @Override
    protected ChannelBuffer extractFrame(ChannelBuffer buffer, int index, int length) {
        return buffer.slice(index, length);
    }

}
