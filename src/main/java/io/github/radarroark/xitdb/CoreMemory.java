package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CoreMemory implements Core {
    ThreadLocal<RandomAccessMemory> memory;

    public CoreMemory(RandomAccessMemory memory) {
        this.memory = new ThreadLocal<>();
        this.memory.set(memory);
    }

    @Override
    public DataInput reader() {
        return this.memory.get();
    }

    @Override
    public DataOutput writer() {
        return this.memory.get();
    }

    @Override
    public long length() throws IOException {
        return this.memory.get().size();
    }

    @Override
    public void seek(long pos) throws IOException {
        this.memory.get().seek((int)pos);
    }

    @Override
    public long position() throws IOException {
        return this.memory.get().position;
    }

    @Override
    public void setLength(long len) throws IOException {
        this.memory.get().setLength((int)len);
    }

    @Override
    public void sync() throws IOException {
    }
}
