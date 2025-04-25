package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadLinkedArrayList {
    public ReadCursor cursor;

    public ReadLinkedArrayList(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, LINKED_ARRAY_LIST -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public long count() throws IOException {
        return this.cursor.count();
    }

    public ReadCursor.Iterator iterator() throws IOException {
        return this.cursor.iterator();
    }

    public ReadCursor getCursor(long index) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index)
        });
    }

    public Slot getSlot(long index) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index)
        });
    }
}
