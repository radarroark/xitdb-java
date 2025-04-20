package net.haxy.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Core {
    public DataInput getReader();

    public DataOutput getWriter();

    public long length() throws IOException;

    public void seek(long pos) throws IOException;

    public long position() throws IOException;

    public void setLength(long len) throws IOException;
}
