package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

public class CoreFile implements Core {
    ThreadLocal<RandomAccessFile> file;

    public CoreFile(RandomAccessFile file) {
        this.file = new ThreadLocal<>() {
            @Override
            protected RandomAccessFile initialValue() {
                return file;
            }
        };
    }

    @Override
    public DataInput reader() {
        return this.file.get();
    }

    @Override
    public DataOutput writer() {
        return this.file.get();
    }

    @Override
    public long length() throws IOException {
        return this.file.get().length();
    }

    @Override
    public void seek(long pos) throws IOException {
        this.file.get().seek(pos);
    }

    @Override
    public long position() throws IOException {
        return this.file.get().getFilePointer();
    }

    @Override
    public void setLength(long len) throws IOException {
        this.file.get().setLength(len);
    }

    @Override
    public void sync() throws IOException {
        this.file.get().getFD().sync();
    }
}
