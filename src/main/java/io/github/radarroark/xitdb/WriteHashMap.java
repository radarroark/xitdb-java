package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteHashMap extends ReadHashMap {
    public WriteCursor cursor;

    public WriteHashMap(WriteCursor cursor) throws Exception {
        super(cursor);
        this.cursor = cursor.writePath(new Database.PathPart[]{
            new Database.HashMapInit()
        });
    }

    @Override
    public WriteCursor.Iterator iterator() throws IOException {
        return this.cursor.iterator();
    }

    // methods that take a string key and hash it for you

    public void put(String key, Database.WriteableData data) throws Exception {
        var hash = this.cursor.db.md.digest(key.getBytes());
        // this overload also stores the key
        putKey(hash, new Database.Bytes(key));
        put(hash, data);
    }

    public WriteCursor putCursor(String key) throws Exception {
        var hash = this.cursor.db.md.digest(key.getBytes());
        // this overload also stores the key
        putKey(hash, new Database.Bytes(key));
        return putCursor(hash);
    }

    public void putKey(String key, Database.WriteableData data) throws Exception {
        putKey(this.cursor.db.md.digest(key.getBytes()), data);
    }

    public WriteCursor putKeyCursor(String key) throws Exception {
        return putKeyCursor(this.cursor.db.md.digest(key.getBytes()));
    }

    public boolean remove(String key) throws Exception {
        return remove(this.cursor.db.md.digest(key.getBytes()));
    }

    // methods that take a hash directly

    public void put(byte[] hash, Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash)),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(byte[] hash) throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetValue(hash)),
        });
    }

    public void putKey(byte[] hash, Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putKeyCursor(byte[] hash) throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.HashMapGet(new Database.HashMapGetKey(hash)),
        });
    }

    public boolean remove(byte[] hash) throws Exception {
        try {
            this.cursor.writePath(new Database.PathPart[]{
                new Database.HashMapRemove(hash)
            });
        } catch (Database.KeyNotFoundException e) {
            return false;
        }
        return true;
    }
}
