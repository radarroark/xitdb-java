package io.github.radarroark.xitdb;

public enum Tag {
    NONE,
    INDEX,
    ARRAY_LIST,
    LINKED_ARRAY_LIST,
    HASH_MAP,
    KV_PAIR,
    BYTES,
    SHORT_BYTES,
    UINT,
    INT,
    FLOAT,
    COUNTED_HASH_MAP;

    public static Tag valueOf(int n) {
        return Tag.values()[n];
    }
}
