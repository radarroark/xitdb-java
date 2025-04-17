package net.haxy.xitdb;

import java.io.DataInput;
import java.io.DataOutput;

public interface Core {
    public DataInput getReader();

    public DataOutput getWriter();

    public long length() throws Exception;

    public void seek(long pos) throws Exception;

    public void setLength(long len) throws Exception;
}
