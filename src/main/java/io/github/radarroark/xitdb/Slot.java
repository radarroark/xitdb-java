package io.github.radarroark.xitdb;

import java.nio.ByteBuffer;

public record Slot(long value, Tag tag, boolean full) implements Database.WriteableData {
    public static int length = 9;

    public Slot() {
        this(0, Tag.NONE, false);
    }

    public Slot(long value, Tag tag) {
        this(value, tag, false);
    }

    public Slot withTag(Tag tag) {
        return new Slot(this.value, tag, this.full);
    }

    public Slot withFull(boolean full) {
        return new Slot(this.value, this.tag, full);
    }

    public byte[] toBytes() {
        var buffer = ByteBuffer.allocate(length);
        var tagInt = this.full ? 0b1000_0000 : 0;
        tagInt = tagInt | this.tag.ordinal();
        buffer.put((byte)tagInt);
        buffer.putLong(this.value);
        return buffer.array();
    }

    public static Slot fromBytes(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var tagByte = buffer.get();
        var full = (tagByte & 0b1000_0000) != 0;
        var tag = Tag.valueOf(tagByte & 0b0111_1111);
        var value = buffer.getLong();
        return new Slot(value, tag, full);
    }
}
