package io.github.radarroark.xitdb;

import java.io.IOException;

public class WriteLinkedArrayList extends ReadLinkedArrayList {
    public WriteCursor cursor;

    public WriteLinkedArrayList(WriteCursor cursor) throws Exception {
        super(cursor);
        this.cursor = cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListInit()
        });
    }

    @Override
    public WriteCursor.Iterator iterator() throws IOException {
        return this.cursor.iterator();
    }

    public void put(long index, Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index),
            new Database.WriteData(data)
        });
    }

    public WriteCursor putCursor(long index) throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListGet(index),
        });
    }

    public void append(Database.WriteableData data) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListAppend(),
            new Database.WriteData(data),
        });
    }

    public WriteCursor appendCursor() throws Exception {
        return this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListAppend(),
        });
    }

    public void slice(long offset, long size) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListSlice(offset, size)
        });
    }

    public void concat(Slot list) throws Exception {
        this.cursor.writePath(new Database.PathPart[]{
            new Database.LinkedArrayListConcat(list)
        });
    }
}
