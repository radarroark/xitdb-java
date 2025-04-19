package net.haxy.xitdb;

import java.nio.ByteBuffer;
import java.security.MessageDigest;

public record Hash(MessageDigest digest, int id) {
    public static int stringToId(String hashIdName) {
        var bytes = hashIdName.getBytes();
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
