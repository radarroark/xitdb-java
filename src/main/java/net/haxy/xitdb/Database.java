package net.haxy.xitdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Database {
    Core core;
    Header header;
    Long txStart;

    public Database(Core core, Options opts) throws Exception {
        this.core = core;

        core.seek(0);
        if (core.length() == 0) {
            this.header = this.writeHeader(opts);
        } else {
            this.header = Header.read(core);
            this.header.validate();
        }

        this.txStart = null;
    }

    public static record Options (HashId hashId, short hashSize) {}

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = "xit".getBytes();
    public static final int DATABASE_START = 12;

    public static record Header (
        HashId hashId,
        short hashSize,
        short version,
        Tag tag,
        byte[] magicNumber
    ) {
        public ByteBuffer getBytes() {
            var buffer = ByteBuffer.allocate(DATABASE_START);
            buffer.put(this.magicNumber);
            buffer.put((byte)this.tag.ordinal());
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId.id);
            return buffer;
        }

        public static Header read(Core core) throws IOException {
            var reader = core.getReader();
            var magicNumber = new byte[3];
            reader.readFully(magicNumber);
            Tag tag = Tag.valueOf(reader.readByte());
            var version = reader.readShort();
            var hashSize = reader.readShort();
            var hashId = reader.readInt();
            return new Header(new HashId(hashId), hashSize, version, tag, magicNumber);
        }

        public void validate() throws InvalidDatabaseException, InvalidVersionException {
            if (!Arrays.equals(this.magicNumber, MAGIC_NUMBER)) {
                throw new InvalidDatabaseException();
            }
            if (this.version > VERSION) {
                throw new InvalidVersionException();
            }
        }

        public class InvalidDatabaseException extends Exception {}
        public class InvalidVersionException extends Exception {}
    }

    public static record HashId(int id) {
        public static HashId fromString(String hashIdName) {
            var bytes = hashIdName.getBytes();
            if (bytes.length != 4) {
                throw new IllegalArgumentException();
            }
            var buffer = ByteBuffer.allocate(4);
            buffer.put(bytes);
            buffer.position(0);
            int id = buffer.getInt();
            return new HashId(id);
        }

        public String toString() {
            var buffer = ByteBuffer.allocate(4);
            buffer.putInt(this.id);
            return new String(buffer.array());
        }
    }

    public static record Slot(long value, Tag tag, boolean full) {
        public Slot() {
            this(0, Tag.NONE, false);
        }

        public Slot(long value, Tag tag) {
            this(value, tag, false);
        }
    }

    public static record SlotPointer(Long position, Slot slot) {}

    public static enum Tag {
        NONE,
        INDEX,
        ARRAY_LIST,
        LINKED_ARRAY_LIST,
        HASH_MAP,
        KV_PAIR,
        BYTES,
        SHORT_BYTES,
        UINT,
        INT,
        FLOAT;

        public static Tag valueOf(byte b) {
            int i = 0;
            for (Tag t : values()) {
                if (i == b) {
                    return t;
                }
                i += 1;
            }
            throw new IllegalArgumentException();
        }
    }

    public static enum WriteMode {
        READ_ONLY,
        READ_WRITE
    }

    public static sealed interface PathPart permits ArrayListInit {}
    public static final class ArrayListInit implements PathPart {}

    public class KeyNotFound extends Exception {}
    public class WriteNotAllowed extends Exception {}

    public ReadWriteCursor rootCursor() {
        return new ReadWriteCursor(new SlotPointer(null, new Slot(DATABASE_START, this.header.tag)), this);
    }

    private Header writeHeader(Options opts) throws IOException {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, Tag.NONE, MAGIC_NUMBER);
        var writer = this.core.getWriter();
        writer.write(header.getBytes().array());
        return header;
    }

    private SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, SlotPointer slotPointer) throws Exception {
        if (path.length == 0) {
            if (writeMode == WriteMode.READ_ONLY && slotPointer.slot.tag == Tag.NONE) {
                throw new KeyNotFound();
            }
            return slotPointer;
        }
        var part = path[0];

        var isTopLevel = slotPointer.slot.value == DATABASE_START;

        var isTxStart = isTopLevel && this.header.tag == Tag.ARRAY_LIST && this.txStart == null;
        if (isTxStart) {
            this.txStart = this.core.length();
        }

        try {
            switch (part) {
                case ArrayListInit arrayListInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowed();

                    return slotPointer;
                }
            }
        } finally {
            this.txStart = null;
        }
    }

    public static class ReadOnlyCursor {
        SlotPointer slotPtr;
        Database db;

        public ReadOnlyCursor(SlotPointer slotPtr, Database db) {
            this.slotPtr = slotPtr;
            this.db = db;
        }

        public ReadOnlyCursor readPath(PathPart[] path) throws Exception {
            try {
                var slotPtr = this.db.readSlotPointer(WriteMode.READ_ONLY, path, this.slotPtr);
                return new ReadOnlyCursor(slotPtr, this.db);
            } catch (KeyNotFound e) {
                return null;
            }
        }
    }

    public static class ReadWriteCursor extends ReadOnlyCursor {
        public ReadWriteCursor(SlotPointer slotPtr, Database db) {
            super(slotPtr, db);
        }
    }
}