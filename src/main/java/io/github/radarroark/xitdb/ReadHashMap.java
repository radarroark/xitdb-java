package io.github.radarroark.xitdb;

import java.io.IOException;

public class ReadHashMap {
    public ReadCursor cursor;

    public ReadHashMap(ReadCursor cursor) {
        switch (cursor.slotPtr.slot().tag()) {
            case NONE, HASH_MAP -> {
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
        return getCursor(this.cursor.db.md.digest(key.getBytes()));
    }

    public Slot getSlot(String key) throws Exception {
        return getSlot(this.cursor.db.md.digest(key.getBytes()));
    }

    public ReadCursor getKeyCursor(String key) throws Exception {
        return getKeyCursor(this.cursor.db.md.digest(key.getBytes()));
    }

    public Slot getKeySlot(String key) throws Exception {
        return getKeySlot(this.cursor.db.md.digest(key.getBytes()));
    }

    public ReadCursor.KeyValuePairCursor getKeyValuePair(String key) throws Exception {
        return getKeyValuePair(this.cursor.db.md.digest(key.getBytes()));
    }

    // methods that take a hash directly

    public ReadCursor getCursor(byte[] hash) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash))
        });
    }

    public Slot getSlot(byte[] hash) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash))
        });
    }

    public ReadCursor getKeyCursor(byte[] hash) throws Exception {
        return this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }

    public Slot getKeySlot(byte[] hash) throws Exception {
        return this.cursor.readPathSlot(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash))
        });
    }

    public ReadCursor.KeyValuePairCursor getKeyValuePair(byte[] hash) throws Exception {
        var cursor = this.cursor.readPath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKVPair(hash))
        });
        if (cursor == null) {
            return null;
        } else {
            return cursor.readKeyValuePair();
        }
    }
}
