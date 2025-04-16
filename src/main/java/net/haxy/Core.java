package net.haxy;

import java.io.InputStream;
import java.io.OutputStream;

public interface Core {
    public InputStream getInputStream() throws Exception;

    public OutputStream getOutputStream() throws Exception;

    public long length();
}
