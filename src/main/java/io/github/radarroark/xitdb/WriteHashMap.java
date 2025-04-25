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
}
