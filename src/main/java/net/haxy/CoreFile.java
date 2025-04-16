package net.haxy;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

public class CoreFile implements Core {
    File file;

    public CoreFile(File file) {
        this.file = file;
    }

    @Override
    public InputStream getInputStream() throws FileNotFoundException {
        return new FileInputStream(this.file);
    }

    @Override
    public OutputStream getOutputStream() throws FileNotFoundException {
        return new FileOutputStream(this.file);
    }

    @Override
    public long length() {
        return this.file.length();
    }
    
}
