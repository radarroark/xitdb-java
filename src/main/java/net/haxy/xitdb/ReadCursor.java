package net.haxy.xitdb;

import java.io.IOException;
import java.util.Arrays;

public class ReadCursor {
    SlotPointer slotPtr;
    Database db;

    public ReadCursor(SlotPointer slotPtr, Database db) {
        this.slotPtr = slotPtr;
        this.db = db;
    }

    public ReadCursor readPath(Database.PathPart[] path) throws Exception {
        try {
            var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_ONLY, path, this.slotPtr);
            return new ReadCursor(slotPtr, this.db);
        } catch (Database.KeyNotFoundException e) {
            return null;
        }
    }

    public Slot readPathSlot(Database.PathPart[] path) throws Exception {
        try {
            var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_ONLY, path, this.slotPtr);
            if (slotPtr.slot().tag() != Tag.NONE || slotPtr.slot().full()) {
                return slotPtr.slot();
            } else {
                return null;
            }
        } catch (Database.KeyNotFoundException e) {
            return null;
        }
    }

    public byte[] readBytes(long maxSize) throws Exception {
        var reader = this.db.core.getReader();

        switch (this.slotPtr.slot().tag()) {
            case NONE -> {
                return new byte[0];
            }
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var valueSize = reader.readLong();

                if (valueSize > maxSize) {
                    throw new Database.StreamTooLongException();
                }

                var value = new byte[(int)valueSize];
                reader.readFully(value);
                return value;
            }
            case SHORT_BYTES -> {
                var bytes = this.slotPtr.slot().toBytes();
                var valueSize = 0;
                for (byte b : bytes) {
                    if (b == 0) break;
                    valueSize += 1;
                }

                if (valueSize > maxSize) {
                    throw new Database.StreamTooLongException();
                }

                return Arrays.copyOfRange(bytes, 0, valueSize);
            }
            default -> throw new Database.UnexpectedTagException();
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

    public long count() throws Exception {
        var reader = this.db.core.getReader();
        switch (this.slotPtr.slot().tag()) {
            case NONE -> {
                return 0;
            }
            case ARRAY_LIST -> {
                this.db.core.seek(this.slotPtr.slot().value());
                var headerBytes = new byte[Database.ArrayListHeader.length];
                reader.readFully(headerBytes);
                var header = Database.ArrayListHeader.fromBytes(headerBytes);
                return header.size();
            }
            case LINKED_ARRAY_LIST -> throw new Exception("Not implemented");
            case BYTES -> {
                this.db.core.seek(this.slotPtr.slot().value());
                return reader.readLong();
            }
            case SHORT_BYTES -> {
                var size = 0;
                for (byte b : this.slotPtr.slot().toBytes()) {
                    if (b == 0) break;
                    size += 1;
                }
                return size;
            }
            default -> throw new Database.UnexpectedTagException();
        }
    }

    public static class Reader {
        ReadCursor parent;
        long size;
        long startPosition;
        long relativePosition;

        public Reader(ReadCursor parent, long size, long startPosition, long relativePosition) {
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
