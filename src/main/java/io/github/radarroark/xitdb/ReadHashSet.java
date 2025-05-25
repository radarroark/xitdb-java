package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadHashSet {
    public ReadCursor cursor;

    protected ReadHashSet() {
    }

    public ReadHashSet(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, HASH_MAP, HASH_SET -> {
                this.cursor = cursor;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public ReadCursor.Iterator iterator() throws IOException {
        return this.cursor.iterator();
    }

    // methods that take a string key and hash it for you

    public ReadCursor getCursor(String key) throws Exception {
        return getCursor(this.cursor.db.md.digest(key.getBytes("UTF-8")));
    }

    public Slot getSlot(String key) throws Exception {
        return getSlot(this.cursor.db.md.digest(key.getBytes("UTF-8")));
    }

    // methods that take a Database.Bytes key and hash it for you

    public ReadCursor getCursor(Database.Bytes key) throws Exception {
        return getCursor(this.cursor.db.md.digest(key.value()));
    }

    public Slot getSlot(Database.Bytes key) throws Exception {
        return getSlot(this.cursor.db.md.digest(key.value()));
    }

    // methods that take a hash directly

    public ReadCursor getCursor(byte[] hash) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }

    public Slot getSlot(byte[] hash) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }
}
