package net.haxy.xitdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Database {
    Core core;
    Header header;
    Long txStart;

    public static record Options (HashId hashId, short hashSize) {}

    public static final short VERSION = 0;
    public static final byte[] MAGIC_NUMBER = "xit".getBytes();
    public static final int DATABASE_START = Header.length;
    public static final int BIT_COUNT = 4;
    public static final int SLOT_COUNT = 1 << BIT_COUNT;
    public static final long MASK = SLOT_COUNT - 1;
    public static final int INDEX_BLOCK_SIZE = Slot.length * SLOT_COUNT;

    public static record Header (
        HashId hashId,
        short hashSize,
        short version,
        Tag tag,
        byte[] magicNumber
    ) {
        public static int length = 12;

        public byte[] toBytes() {
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
            var tag = Tag.valueOf(reader.readByte() & 0b0111_1111);
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

    public static enum WriteMode {
        READ_ONLY,
        READ_WRITE
    }

    public static record ArrayListHeader(long ptr, long size) {
        public static int length = 16;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.putLong(this.size);
            buffer.putLong(this.ptr);
            return buffer.array();
        }

        public static ArrayListHeader fromBytes(byte[] bytes) {
            var buffer = ByteBuffer.wrap(bytes);
            var size = buffer.getLong();
            var ptr = buffer.getLong();
            return new ArrayListHeader(ptr, size);
        }

        public ArrayListHeader withPtr(long ptr) {
            return new ArrayListHeader(ptr, this.size);
        }
    }
    public static record TopLevelArrayListHeader(long fileSize, ArrayListHeader parent) {
        public static int length = 8 + ArrayListHeader.length;

        public byte[] toBytes() {
            var buffer = ByteBuffer.allocate(length);
            buffer.put(this.parent.toBytes());
            buffer.putLong(this.fileSize);
            return buffer.array();
        }
    }

    public static sealed interface PathPart permits ArrayListInit, ArrayListGet, ArrayListAppend, HashMapInit, WriteData {}
    public static record ArrayListInit() implements PathPart {}
    public static record ArrayListGet(long index) implements PathPart {}
    public static record ArrayListAppend() implements PathPart {}
    public static record HashMapInit() implements PathPart {}
    public static record WriteData(WriteableData data) implements PathPart {}

    public static sealed interface WriteableData permits Slot, Bytes {}
    public static record Bytes(byte[] value) implements WriteableData {
        public boolean isShort() {
            if (this.value.length > 8) return false;
            for (byte b : this.value) {
                if (b == 0) return false;
            }
            return true;
        }
    }

    public static class InvalidDatabaseException extends Exception {}
    public static class InvalidVersionException extends Exception {}
    public static class KeyNotFoundException extends Exception {}
    public static class WriteNotAllowedException extends Exception {}
    public static class UnexpectedTagException extends Exception {}
    public static class CursorNotWriteableException extends Exception {}
    public static class ExpectedTxStartException extends Exception {}

    // init

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

    public WriteCursor rootCursor() {
        return new WriteCursor(new SlotPointer(null, new Slot(DATABASE_START, this.header.tag)), this);
    }

    // private

    private Header writeHeader(Options opts) throws IOException {
        var header = new Header(opts.hashId, opts.hashSize, VERSION, Tag.NONE, MAGIC_NUMBER);
        var writer = this.core.getWriter();
        writer.write(header.toBytes());
        return header;
    }

    protected SlotPointer readSlotPointer(WriteMode writeMode, PathPart[] path, SlotPointer slotPtr) throws Exception {
        if (path.length == 0) {
            if (writeMode == WriteMode.READ_ONLY && slotPtr.slot().tag() == Tag.NONE) {
                throw new KeyNotFoundException();
            }
            return slotPtr;
        }
        var part = path[0];

        var isTopLevel = slotPtr.slot().value() == DATABASE_START;

        var isTxStart = isTopLevel && this.header.tag == Tag.ARRAY_LIST && this.txStart == null;
        if (isTxStart) {
            this.txStart = this.core.length();
        }

        try {
            switch (part) {
                case ArrayListInit arrayListInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

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
                            ).toBytes());

                            // write the first block
                            writer.write(new byte[INDEX_BLOCK_SIZE]);

                            // update db header
                            this.core.seek(0);
                            this.header = this.header.withTag(Tag.ARRAY_LIST);
                            writer.write(this.header.toBytes());
                        }

                        var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(Tag.ARRAY_LIST));
                        return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                    }

                    if (slotPtr.position() == null) throw new CursorNotWriteableException();
                    long position = slotPtr.position();

                    switch (slotPtr.slot().tag()) {
                        case Tag.NONE -> {
                            // if slot was empty, insert the new list
                            var writer = this.core.getWriter();
                            this.core.seek(this.core.length());
                            var arrayListStart = this.core.length();
                            var arrayListPtr = arrayListStart + ArrayListHeader.length;
                            writer.write(new ArrayListHeader(
                                arrayListPtr,
                                0
                            ).toBytes());
                            writer.write(new byte[INDEX_BLOCK_SIZE]);
                            // make slot point to list
                            var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                            this.core.seek(position);
                            writer.write(nextSlotPtr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                        }
                        case Tag.ARRAY_LIST -> {
                            var reader = this.core.getReader();
                            var writer = this.core.getWriter();

                            var arrayListStart = slotPtr.slot().value();

                            // copy it to the end unless it was made in this transaction
                            if (this.txStart != null) {
                                if (arrayListStart < this.txStart) {
                                    // read existing block
                                    this.core.seek(arrayListStart);
                                    var headerBytes = new byte[ArrayListHeader.length];
                                    reader.readFully(headerBytes);
                                    var header = ArrayListHeader.fromBytes(headerBytes);
                                    this.core.seek(header.ptr);
                                    var arrayListIndexBlock = new byte[INDEX_BLOCK_SIZE];
                                    reader.readFully(arrayListIndexBlock);
                                    // copy to the end
                                    this.core.seek(this.core.length());
                                    arrayListStart = this.core.length();
                                    var nextArrayListPtr = arrayListStart + ArrayListHeader.length;
                                    header = header.withPtr(nextArrayListPtr);
                                    writer.write(header.toBytes());
                                    writer.write(arrayListIndexBlock);
                                }
                            } else if (this.header.tag() == Tag.ARRAY_LIST) {
                                throw new ExpectedTxStartException();
                            }

                            // make slot point to list
                            var nextSlotPtr = new SlotPointer(position, new Slot(arrayListStart, Tag.ARRAY_LIST));
                            this.core.seek(position);
                            writer.write(nextSlotPtr.slot().toBytes());
                            return readSlotPointer(writeMode, Arrays.copyOfRange(path, 0, path.length), nextSlotPtr);
                        }
                        default -> throw new UnexpectedTagException();
                    }
                }
                case ArrayListGet arrayListGet -> {
                    var tag = isTopLevel ? this.header.tag : slotPtr.slot().tag();
                    switch (tag) {
                        case NONE -> throw new KeyNotFoundException();
                        case ARRAY_LIST -> {}
                        default -> throw new UnexpectedTagException();
                    }

                    var nextArrayListStart = slotPtr.slot().value();
                    var index = arrayListGet.index();

                    this.core.seek(nextArrayListStart);
                    var reader = this.core.getReader();
                    var headerBytes = new byte[ArrayListHeader.length];
                    reader.readFully(headerBytes);
                    var header = ArrayListHeader.fromBytes(headerBytes);
                    if (index >= header.size || index < -header.size) {
                        throw new KeyNotFoundException();
                    }

                    var key = index < 0 ? header.size - Math.abs(index) : index;
                    var lastKey = header.size - 1;
                    var shift = (byte) (lastKey < SLOT_COUNT ? 0 : Math.log(lastKey) / Math.log(SLOT_COUNT));
                    var finalSlotPtr = readArrayListSlot(header.ptr, key, shift, writeMode, isTopLevel);

                    return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), finalSlotPtr);
                }
                case ArrayListAppend arrayListAppend -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    var tag = isTopLevel ? this.header.tag : slotPtr.slot().tag();
                    if (tag != Tag.ARRAY_LIST) throw new UnexpectedTagException();

                    var nextArrayListStart = slotPtr.slot().value();

                    var appendResult = readArrayListSlotAppend(nextArrayListStart, writeMode, isTopLevel);
                    var finalSlotPtr = readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), appendResult.slotPtr());

                    var writer = this.core.getWriter();
                    if (isTopLevel) {
                        this.core.seek(this.core.length());
                        var fileSize = this.core.length();
                        var header = new TopLevelArrayListHeader(fileSize, appendResult.header);

                        // update header
                        this.core.seek(nextArrayListStart);
                        writer.write(header.toBytes());
                    } else {
                        // update header
                        this.core.seek(nextArrayListStart);
                        writer.write(appendResult.header().toBytes());
                    }

                    return finalSlotPtr;
                }
                case HashMapInit hashMapInit -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (isTopLevel) {
                        var writer = this.core.getWriter();

                        // if the top level hash map hasn't been initialized
                        if (this.header.tag == Tag.NONE) {
                            // write the first block
                            this.core.seek(DATABASE_START);
                            writer.write(new byte[INDEX_BLOCK_SIZE]);

                            // update db header
                            this.core.seek(0);
                            this.header = this.header.withTag(Tag.HASH_MAP);
                            writer.write(this.header.toBytes());
                        }

                        var nextSlotPtr = slotPtr.withSlot(slotPtr.slot().withTag(Tag.HASH_MAP));
                        return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                    }

                    throw new Exception("not implemented");
                }
                case WriteData writeData -> {
                    if (writeMode == WriteMode.READ_ONLY) throw new WriteNotAllowedException();

                    if (slotPtr.position() == null) throw new CursorNotWriteableException();
                    long position = slotPtr.position();

                    var writer = this.core.getWriter();

                    Slot slot = switch (writeData.data()) {
                        case Slot s -> s;
                        case Bytes bytes -> {
                            if (bytes.isShort()) {
                                var buffer = ByteBuffer.allocate(8);
                                buffer.put(bytes.value());
                                buffer.position(0);
                                yield new Slot(buffer.getLong(), Tag.SHORT_BYTES);
                            } else {
                                var nextCursor = new WriteCursor(slotPtr, this);
                                var cursorWriter = nextCursor.getWriter();
                                cursorWriter.write(bytes.value());
                                cursorWriter.finish();
                                yield cursorWriter.slot;
                            }
                        }
                    };

                    // this bit allows us to distinguish between a slot explicitly set to NONE
                    // and a slot that hasn't been set yet
                    if (slot.tag() == Tag.NONE) {
                        slot = slot.withFull(true);
                    }

                    this.core.seek(position);
                    writer.write(slot.toBytes());

                    var nextSlotPtr = new SlotPointer(slotPtr.position(), slot);
                    return readSlotPointer(writeMode, Arrays.copyOfRange(path, 1, path.length), nextSlotPtr);
                }
            }
        } finally {
            this.txStart = null;
        }
    }

    // array_list

    public static record ArrayListAppendResult(ArrayListHeader header, SlotPointer slotPtr) {}

    private ArrayListAppendResult readArrayListSlotAppend(long indexStart, WriteMode writeMode, boolean isTopLevel) throws Exception {
        var reader = this.core.getReader();
        var writer = this.core.getWriter();

        this.core.seek(indexStart);
        var headerBytes = new byte[ArrayListHeader.length];
        reader.readFully(headerBytes);
        var header = ArrayListHeader.fromBytes(headerBytes);
        var indexPos = header.ptr();

        var key = header.size;

        var prevShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key - 1) / Math.log(SLOT_COUNT));
        var nextShift = (byte) (key < SLOT_COUNT ? 0 : Math.log(key) / Math.log(SLOT_COUNT));

        if (prevShift != nextShift) {
            // root overflow
            this.core.seek(this.core.length());
            var nextIndexPos = this.core.length();
            writer.write(new byte[INDEX_BLOCK_SIZE]);
            this.core.seek(nextIndexPos);
            writer.write(new Slot(indexPos, Tag.INDEX).toBytes());
            indexPos = nextIndexPos;
        }

        var slotPtr = readArrayListSlot(indexPos, key, nextShift, writeMode, isTopLevel);
        return new ArrayListAppendResult(new ArrayListHeader(indexPos, header.size() + 1), slotPtr);
    }

    private SlotPointer readArrayListSlot(long indexPos, long key, byte shift, WriteMode writeMode, boolean isTopLevel) throws Exception {
        var reader = this.core.getReader();

        var i = (key >> (shift * BIT_COUNT)) & MASK;
        var slotPos = indexPos + (Slot.length * i);
        this.core.seek(slotPos);
        var slotBytes = new byte[Slot.length];
        reader.readFully(slotBytes);
        var slot = Slot.fromBytes(slotBytes);

        if (shift == 0) {
            return new SlotPointer(slotPos, slot);
        }

        var ptr = slot.value();

        switch (slot.tag()) {
            case NONE -> {
                switch (writeMode) {
                    case READ_ONLY -> throw new KeyNotFoundException();
                    case READ_WRITE -> {
                        var writer = this.core.getWriter();
                        this.core.seek(this.core.length());
                        var nextIndexPos = this.core.length();
                        writer.write(new byte[INDEX_BLOCK_SIZE]);
                        // if top level array list, update the file size in the list
                        // header to prevent truncation from destroying this block
                        if (isTopLevel) {
                            this.core.seek(this.core.length());
                            var fileSize = this.core.length();
                            this.core.seek(DATABASE_START + ArrayListHeader.length);
                            writer.writeLong(fileSize);
                        }
                        this.core.seek(slotPos);
                        writer.write(new Slot(nextIndexPos, Tag.INDEX).toBytes());
                        return readArrayListSlot(nextIndexPos, key, (byte)(shift - 1), writeMode, isTopLevel);
                    }
                    default -> throw new Exception();
                }
            }
            case INDEX -> {
                var nextPtr = ptr;
                if (writeMode == WriteMode.READ_WRITE && !isTopLevel) {
                    if (this.txStart != null) {
                        if (nextPtr < this.txStart) {
                            // read existing block
                            this.core.seek(ptr);
                            var indexBlock = new byte[INDEX_BLOCK_SIZE];
                            reader.readFully(indexBlock);
                            // copy it to the end
                            var writer = this.core.getWriter();
                            this.core.seek(this.core.length());
                            nextPtr = this.core.length();
                            writer.write(indexBlock);
                            // make slot point to block
                            this.core.seek(slotPos);
                            writer.write(new Slot(nextPtr, Tag.INDEX).toBytes());
                        }
                    } else if (this.header.tag() == Tag.ARRAY_LIST) {
                        throw new ExpectedTxStartException();
                    }
                }
                return readArrayListSlot(nextPtr, key, (byte)(shift - 1), writeMode, isTopLevel);
            }
            default -> throw new UnexpectedTagException();
        }
    }
}