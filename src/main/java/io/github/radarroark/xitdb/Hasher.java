package io.github.radarroark.xitdb;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

public record Hasher(ThreadLocal<MessageDigest> md, int id) {
    public Hasher(MessageDigest md) {
        this(wrapMessageDigest(md), 0);
    }

    public Hasher(MessageDigest md, int id) {
        this(wrapMessageDigest(md), id);
    }

    private static ThreadLocal<MessageDigest> wrapMessageDigest(MessageDigest md) {
        var tl = new ThreadLocal<MessageDigest>();
        tl.set(md);
        return tl;
    }

    public byte[] digest(byte[] input) {
        return this.md().get().digest(input);
    }

    public MessageDigest getMD() {
        return this.md.get();
    }

    public static int stringToId(String hashIdName) throws UnsupportedEncodingException {
        var bytes = hashIdName.getBytes("UTF-8");
        if (bytes.length != 4) {
            throw new IllegalArgumentException("Name must be exactly four bytes long");
        }
        var buffer = ByteBuffer.allocate(4);
        buffer.put(bytes);
        buffer.position(0);
        return buffer.getInt();
    }

    public static String idToString(int id) {
        var buffer = ByteBuffer.allocate(4);
        buffer.putInt(id);
        return new String(buffer.array());
    }
}
