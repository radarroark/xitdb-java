package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadCountedHashMap extends ReadHashMap {
    public ReadCountedHashMap(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, COUNTED_HASH_MAP -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public long count() throws IOException {
        return this.cursor.count();
    }
}
