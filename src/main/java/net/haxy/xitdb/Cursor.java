package net.haxy.xitdb;

import java.io.IOException;

public class Cursor {
    SlotPointer slotPtr;
    Database db;

    public Cursor(SlotPointer slotPtr, Database db) {
        this.slotPtr = slotPtr;
        this.db = db;
    }

    public Cursor readPath(Database.PathPart[] path) throws Exception {
        try {
            var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_ONLY, path, this.slotPtr);
            return new Cursor(slotPtr, this.db);
        } catch (Database.KeyNotFoundException e) {
            return null;
        }
    }

    public Reader getReader() throws Exception {
        var reader = this.db.core.getReader();

        switch (this.slotPtr.slot().tag()) {
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var size = reader.readLong();
                var startPosition = this.db.core.position();
                return new Reader(this, size, startPosition, 0);
            }
            case SHORT_BYTES -> {
                var bytes = this.slotPtr.slot().toBytes();
                var valueSize = 0;
                for (byte b : bytes) {
                    if (b == 0) break;
                    valueSize += 1;
                }
                // add one to get past the tag byte
                var startPosition = this.slotPtr.position() + 1;
                return new Reader(this, valueSize, startPosition, 0);
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public static class Reader {
        Cursor parent;
        long size;
        long startPosition;
        long relativePosition;

        public Reader(Cursor parent, long size, long startPosition, long relativePosition) {
            this.parent = parent;
            this.size = size;
            this.startPosition = startPosition;
            this.relativePosition = relativePosition;
        }

        public int read(byte[] buffer) throws Exception {
            if (this.size < this.relativePosition) throw new IOException("End of stream");
            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var readSize = Math.min(buffer.length, (int) (this.size - this.relativePosition));
            var reader = this.parent.db.core.getReader();
            reader.readFully(buffer, 0, readSize);
            this.relativePosition += readSize;
            return readSize;
        }
    }
}
