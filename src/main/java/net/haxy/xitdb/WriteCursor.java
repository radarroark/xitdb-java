package net.haxy.xitdb;

public class WriteCursor extends ReadCursor {
    public WriteCursor(SlotPointer slotPtr, Database db) {
        super(slotPtr, db);
    }

    public WriteCursor writePath(Database.PathPart[] path) throws Exception {
        var slotPtr = this.db.readSlotPointer(Database.WriteMode.READ_WRITE, path, this.slotPtr);
        return new WriteCursor(slotPtr, this.db);
    }

    public Writer getWriter() throws Exception {
        var writer = this.db.core.getWriter();
        this.db.core.seek(this.db.core.length());
        var ptrPos = this.db.core.length();
        writer.writeLong(0);
        var startPosition = this.db.core.length();

        return new Writer(this, 0, new Slot(ptrPos, Tag.BYTES), startPosition, 0);
    }

    public static class Writer {
        WriteCursor parent;
        long size;
        Slot slot;
        long startPosition;
        long relativePosition;

        public Writer(WriteCursor parent, long size, Slot slot, long startPosition, long relativePosition) {
            this.parent = parent;
            this.size = size;
            this.slot = slot;
            this.startPosition = startPosition;
            this.relativePosition = relativePosition;
        }

        public void write(byte[] buffer) throws Exception {
            if (this.size < this.relativePosition) throw new Database.EndOfStreamException();
            this.parent.db.core.seek(this.startPosition + this.relativePosition);
            var writer = this.parent.db.core.getWriter();
            writer.write(buffer);
            this.relativePosition = buffer.length;
            if (this.relativePosition > this.size) {
                this.size = this.relativePosition;
            }
        }

        public void finish() throws Exception {
            var writer = this.parent.db.core.getWriter();

            this.parent.db.core.seek(this.slot.value());
            writer.writeLong(this.size);

            if (this.parent.slotPtr.position() == null) throw new Database.CursorNotWriteableException();
            long position = this.parent.slotPtr.position();
            this.parent.db.core.seek(position);
            writer.write(this.slot.toBytes());

            this.parent.slotPtr = this.parent.slotPtr.withSlot(this.slot);
        }
    }
}
