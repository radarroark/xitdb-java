package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadArrayList {
    public ReadCursor cursor;

    public ReadArrayList(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, ARRAY_LIST -> {
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
            new Database.ArrayListGet(index)
        });
    }

    public Slot getSlot(long index) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.ArrayListGet(index)
        });
    }
}
