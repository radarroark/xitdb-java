package io.github.radarroark.xitdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessBufferedFile implements DataOutput, DataInput, AutoCloseable {
    RandomAccessFile file;
    RandomAccessMemory memory;
    int bufferSize; // flushes when the memory is >= this size

    public RandomAccessBufferedFile(File file, String mode) throws FileNotFoundException {
        this(file, mode, 8 * 1024 * 1024);
    }

    public RandomAccessBufferedFile(File file, String mode, int bufferSize) throws FileNotFoundException {
        this.file = new RandomAccessFile(file, mode);
        this.memory = new RandomAccessMemory();
        this.bufferSize = bufferSize;
    }

    public void seek(long pos) throws IOException {
        long filePos = this.file.getFilePointer();
        if (pos >= filePos && pos <= filePos + this.memory.size()) {
            this.memory.seek((int) (pos - filePos));
        } else {
            flush();
            this.file.seek(pos);
        }
    }

    public long length() throws IOException {
        return Math.max(this.file.getFilePointer() + this.memory.size(), this.file.length());
    }

    public long position() throws IOException {
        return this.file.getFilePointer() + this.memory.position.get();
    }

    public void setLength(long len) throws IOException {
        flush();
        this.file.setLength(len);
    }

    public void flush() throws IOException {
        if (this.memory.size() > 0) {
            this.file.write(this.memory.toByteArray());
            this.memory.reset();
        }
    }

    public void flushMaybe() throws IOException {
        if (this.memory.size() >= this.bufferSize) {
            flush();
        }
    }

    public void sync() throws IOException {
        flush();
        this.file.getFD().sync();
    }

    // AutoCloseable

    @Override
    public void close() throws Exception {
        flush();
        this.file.close();
        this.memory.close();
    }

    // DataOutput

    @Override
    public void write(byte[] buffer) throws IOException {
        this.memory.write(buffer);
        flushMaybe();
    }

    @Override
    public void write(int b) throws IOException {
        this.memory.write(b);
        flushMaybe();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.memory.write(b, off, len);
        flushMaybe();
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.memory.writeBoolean(v);
        flushMaybe();
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.memory.writeByte(v);
        flushMaybe();
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.memory.writeShort(v);
        flushMaybe();
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.memory.writeChar(v);
        flushMaybe();
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.memory.writeInt(v);
        flushMaybe();
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.memory.writeLong(v);
        flushMaybe();
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.memory.writeFloat(v);
        flushMaybe();
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.memory.writeDouble(v);
        flushMaybe();
    }

    @Override
    public void writeBytes(String s) throws IOException {
        this.memory.writeBytes(s);
        flushMaybe();
    }

    @Override
    public void writeChars(String s) throws IOException {
        this.memory.writeChars(s);
        flushMaybe();
    }

    @Override
    public void writeUTF(String s) throws IOException {
        this.memory.writeUTF(s);
        flushMaybe();
    }

    // DataInput

    @Override
    public void readFully(byte[] b) throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        if (memPos + b.length <= this.memory.size()) {
            this.memory.readFully(b);
        } else {
            flush();
            this.file.seek(filePos + memPos);
            this.file.readFully(b);
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        this.file.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readChar();
    }

    @Override
    public int readInt() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readInt();
    }

    @Override
    public long readLong() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        long filePos = this.file.getFilePointer();
        int memPos = this.memory.position.get();
        flush();
        this.file.seek(filePos + memPos);
        return this.file.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readLine'");
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("Unimplemented method 'readUTF'");
    }
}
