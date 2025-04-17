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
    public static final int DATABASE_START = Header.length;
    public static final int BIT_COUNT = 4;
    public static final int SLOT_COUNT = 1 << BIT_COUNT;
    public static final int INDEX_BLOCK_SIZE = Slot.length * SLOT_COUNT;

    public static record Header (
        HashId hashId,
        short hashSize,
        short version,
        Tag tag,
        byte[] magicNumber
    ) {
        public static int length = 12;

        public byte[] getBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.magicNumber);
            buffer.put((byte)this.tag.ordinal());
            buffer.putShort(this.version);
            buffer.putShort(this.hashSize);
            buffer.putInt(this.hashId.id);
            return buffer.array();
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

        public Header withTag(Tag tag) {
            return new Header(this.hashId, this.hashSize, this.version, tag, this.magicNumber);
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

        public Slot withTag(Tag tag) {
            return new Slot(this.value, tag, this.full);
        }

        public static int length = 9;
    }

    public static record SlotPointer(Long position, Slot slot) {
        public SlotPointer withSlot(Slot slot) {
            return new SlotPointer(this.position, slot);
        }
    }

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

    public static record ArrayListHeader(long ptr, long size) {
        public static int length = 16;

        public byte[] getBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.ptr);
            return buffer.array();
        }
    }
    public static record TopLevelArrayListHeader(long fileSize, ArrayListHeader parent) {
        public static int length = 8 + ArrayListHeader.length;

        public byte[] getBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.parent.getBytes());
            buffer.putLong(this.fileSize);
            return buffer.array();
        }
    }

    public static sealed interface PathPart permits ArrayListInit {}
    public static final class ArrayListInit implements PathPart {}

    public class KeyNotFound extends Exception {}
    public class WriteNotAllowed extends Exception {}

    public WriteCursor rootCursor() {
        return new WriteCursor(new SlotPointer(null, new Slot(DATABASE_START, this.header.tag)), this);
    }

    private Header writeHeader(Options opts) throws IOException {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, Tag.NONE, MAGIC_NUMBER);
        var writer = this.core.getWriter();
        writer.write(header.getBytes());
        return header;
    }

    private SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, SlotPointer slotPtr) throws Exception {
        if (path.length == 0) {
            if (writeMode == WriteMode.READ_ONLY && slotPtr.slot.tag == Tag.NONE) {
                throw new KeyNotFound();
            }
            return slotPtr;
        }
        var part = path[0];

        var isTopLevel = slotPtr.slot.value == DATABASE_START;

        var isTxStart = isTopLevel && this.header.tag == Tag.ARRAY_LIST && this.txStart == null;
        if (isTxStart) {
            this.txStart = this.core.length();
        }

        try {
            switch (part) {
                case ArrayListInit arrayListInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowed();

                    if (isTopLevel) {
                        var writer = this.core.getWriter();

                        // if the top level array list hasn't been initialized
                        if (this.header.tag == Tag.NONE) {
                            // write the array list header
                            this.core.seek(DATABASE_START);
                            var arrayListPtr = DATABASE_START + TopLevelArrayListHeader.length;
                            writer.write((new TopLevelArrayListHeader(
                                0,
                                new ArrayListHeader(arrayListPtr, 0))
                            ).getBytes());

                            // write the first block
                            writer.write(new byte[INDEX_BLOCK_SIZE]);

                            // update db header
                            this.core.seek(0);
                            this.header = this.header.withTag(Tag.ARRAY_LIST);
                            writer.write(this.header.getBytes());
                        }

                        var nextSlotPtr = slotPtr.withSlot(slotPtr.slot.withTag(Tag.ARRAY_LIST));
                        return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                    }

                    return slotPtr;
                }
            }
        } finally {
            this.txStart = null;
        }
    }

    public static class Cursor {
        SlotPointer slotPtr;
        Database db;

        public Cursor(SlotPointer slotPtr, Database db) {
            this.slotPtr = slotPtr;
            this.db = db;
        }

        public Cursor readPath(PathPart[] path) throws Exception {
            try {
                var slotPtr = this.db.readSlotPointer(WriteMode.READ_ONLY, path, this.slotPtr);
                return new Cursor(slotPtr, this.db);
            } catch (KeyNotFound e) {
                return null;
            }
        }
    }

    public static class WriteCursor extends Cursor {
        public WriteCursor(SlotPointer slotPtr, Database db) {
            super(slotPtr, db);
        }

        public WriteCursor writePath(PathPart[] path) throws Exception {
            var slotPtr = this.db.readSlotPointer(WriteMode.READ_WRITE, path, this.slotPtr);
            return new WriteCursor(slotPtr, this.db);
        }
    }
}