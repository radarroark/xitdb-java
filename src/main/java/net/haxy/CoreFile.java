package net.haxy;

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
    public DataInput getReader() {
        return this.file;
    }

    @Override
    public DataOutput getWriter() {
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
    
}
