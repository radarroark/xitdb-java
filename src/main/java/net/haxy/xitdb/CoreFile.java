package net.haxy.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.RandomAccessFile;

public class CoreFile implements Core {
    RandomAccessFile file;

    public CoreFile(RandomAccessFile file) {
        this.file = file;
    }

    @Override
    public DataInput reader() {
        return this.file;
    }

    @Override
    public DataOutput writer() {
        return this.file;
    }

    @Override
    public long length() throws IOException {
        return this.file.length();
    }

    @Override
    public void seek(long pos) throws IOException {
        this.file.seek(pos);
    }

    @Override
    public long position() throws IOException {
        return this.file.getFilePointer();
    }

    @Override
    public void setLength(long len) throws IOException {
        this.file.setLength(len);
    }
    
}
