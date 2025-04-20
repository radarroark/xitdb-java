package net.haxy.xitdb;

import java.io.DataInput;
import java.io.DataOutput;

public class CoreMemory implements Core {
    RandomAccessMemory memory;

    public CoreMemory(RandomAccessMemory memory) {
        this.memory = memory;
    }

    @Override
    public DataInput getReader() {
        return this.memory;
    }

    @Override
    public DataOutput getWriter() {
        return this.memory;
    }

    @Override
    public long length() throws Exception {
        return this.memory.size();
    }

    @Override
    public void seek(long pos) throws Exception {
        this.memory.seek((int)pos);
    }

    @Override
    public long position() throws Exception {
        return this.memory.position;
    }

    @Override
    public void setLength(long len) throws Exception {
        this.memory.setLength((int)len);
    }
}
