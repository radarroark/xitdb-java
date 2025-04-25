package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteArrayList extends ReadArrayList {
    public WriteCursor cursor;

    public WriteArrayList(WriteCursor cursor) throws Exception {
        super(cursor);
        this.cursor = cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit()
        });
    }

    @Override
    public WriteCursor.Iterator iterator() throws IOException {
        return this.cursor.iterator();
    }

    public void put(long index, Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListGet(index),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(long index) throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListGet(index),
        });
    }

    public void append(Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
            new Database.WriteData(data),
        });
    }

    public WriteCursor appendCursor() throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
        });
    }

    public void appendContext(Database.WriteableData data, Database.ContextFunction fn) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListAppend(),
            new Database.WriteData(data),
            new Database.Context(fn)
        });
        // flush all writes from the transaction to disk
        this.cursor.db.core.sync();
    }

    public void slice(long size) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.ArrayListSlice(size)
        });
    }
}
