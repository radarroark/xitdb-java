package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteHashSet extends ReadHashMap {
    protected WriteHashSet() {
    }

    public WriteHashSet(WriteCursor cursor) throws Exception {
        super(cursor.writePath(new Database.PathPart[]{
            new Database.HashMapInit(false, true)
        }));
    }

    @Override
    public WriteCursor.Iterator iterator() throws IOException {
        return ((WriteCursor)this.cursor).iterator();
    }

    // methods that take a string key and hash it for you

    public void put(String key) throws Exception {
        var bytes = key.getBytes("UTF-8");
        put(this.cursor.db.md.digest(bytes), new Database.Bytes(bytes));
    }

    public WriteCursor putCursor(String key) throws Exception {
        return putCursor(this.cursor.db.md.digest(key.getBytes("UTF-8")));
    }

    public boolean remove(String key) throws Exception {
        return remove(this.cursor.db.md.digest(key.getBytes("UTF-8")));
    }

    // methods that take a Database.Bytes key and hash it for you

    public void put(Database.Bytes key) throws Exception {
        put(this.cursor.db.md.digest(key.value()), key);
    }

    public WriteCursor putCursor(Database.Bytes key) throws Exception {
        return putCursor(this.cursor.db.md.digest(key.value()));
    }

    public boolean remove(Database.Bytes key) throws Exception {
        return remove(this.cursor.db.md.digest(key.value()));
    }

    // methods that take a hash directly

    public void put(byte[] hash, Database.WriteableData data) throws Exception {
        var cursor = ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
        });
        // keys are only written if empty, because their value should always
        // be the same at a given hash.
        cursor.writeIfEmpty(data);
    }

    public WriteCursor putCursor(byte[] hash) throws Exception {
        return ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
        });
    }

    public boolean remove(byte[] hash) throws Exception {
        try {
            ((WriteCursor)this.cursor).writePath(new Database.PathPart[]{
                new Database.HashMapRemove(hash)
            });
        } catch (Database.KeyNotFoundException e) {
            return false;
        }
        return true;
    }
}
