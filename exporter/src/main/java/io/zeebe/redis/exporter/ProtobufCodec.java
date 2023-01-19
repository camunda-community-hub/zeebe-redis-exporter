package io.zeebe.redis.exporter;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.codec.ToByteBufEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.*;

public class ProtobufCodec  implements RedisCodec<String, byte[]>, ToByteBufEncoder<String, byte[]> {

    public static final StringCodec UTF8;
    private static final byte[] EMPTY;
    private final Charset charset;

    public ProtobufCodec() {
        this.charset = Charset.forName("UTF-8");
    }

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return Unpooled.wrappedBuffer(bytes).toString(this.charset);
    }

    @Override
    public byte[] decodeValue(ByteBuffer bytes) {
        return getBytes(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return this.encodeAndAllocateBuffer(key);
    }

    @Override
    public ByteBuffer encodeValue(byte[] value) {
        return value == null ? ByteBuffer.wrap(EMPTY) : ByteBuffer.wrap(value);
    }

    @Override
    public void encodeKey(String key, ByteBuf target) {
        this.encode(key, target);
    }

    @Override
    public void encodeValue(byte[] value, ByteBuf target) {
        if (value != null) {
            target.writeBytes(value);
        }
    }

    @Override
    public int estimateSize(Object keyOrValue) {
        if (keyOrValue == null) return 0;
        return keyOrValue instanceof String
                ? ByteBufUtil.utf8MaxBytes((String) keyOrValue)
                : ((byte[]) keyOrValue).length;
    }

    private ByteBuffer encodeAndAllocateBuffer(String key) {
        if (key == null) {
            return ByteBuffer.wrap(EMPTY);
        } else {
            ByteBuffer buffer = ByteBuffer.allocate(ByteBufUtil.utf8MaxBytes(key));
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer);
            byteBuf.clear();
            this.encode(key, byteBuf);
            buffer.limit(byteBuf.writerIndex());
            return buffer;
        }
    }

    public void encode(String str, ByteBuf target) {
        if (str != null) {
                ByteBufUtil.writeUtf8(target, str);
        }
    }

    private static byte[] getBytes(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return EMPTY;
        } else {
            byte[] b = new byte[remaining];
            buffer.get(b);
            return b;
        }
    }

    static {
        UTF8 = new StringCodec(StandardCharsets.UTF_8);
        EMPTY = new byte[0];
    }

}
