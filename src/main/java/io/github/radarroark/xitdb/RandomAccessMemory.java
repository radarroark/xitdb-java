package io.github.radarroark.xitdb;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RandomAccessMemory extends ByteArrayOutputStream implements DataOutput, DataInput {
    ThreadLocal<Integer> position;

    public RandomAccessMemory() {
        this.position = new ThreadLocal<>();
        this.position.set(0);
    }

    @Override
    public void write(byte[] buffer) throws IOException {
        int pos = this.position.get();
        if (pos < this.count) {
            int bytesBeforeEnd = Math.min(buffer.length, this.count - pos);
            for (int i = 0; i < bytesBeforeEnd; i++) {
                this.buf[pos + i] = buffer[i];
            }

            if (bytesBeforeEnd < buffer.length) {
                int bytesAfterEnd = buffer.length - bytesBeforeEnd;
                super.write(Arrays.copyOfRange(buffer, buffer.length - bytesAfterEnd, buffer.length));
            }
        } else {
            super.write(buffer);
        }

        this.position.set(pos + buffer.length);
    }

    @Override
    public void reset() {
        super.reset();
        this.position.set(0);
    }

    public void seek(int pos) {
        if (pos > this.count) {
            this.position.set(this.count);
        } else {
            this.position.set(pos);
        }
    }

    public void setLength(int len) throws IOException {
        if (len == 0) {
            reset();
        } else {
            if (len > size()) throw new IllegalArgumentException();
            var bytes = toByteArray();
            var originalPos = this.position.get();
            reset();
            write(Arrays.copyOfRange(bytes, 0, len));
            seek(originalPos);
        }
    }

    // DataOutput

    @Override
    public void writeBoolean(boolean v) throws IOException {
        write(new byte[]{(byte) (v ? 1 : 0)});
    }

    @Override
    public void writeByte(int v) throws IOException {
        write(new byte[]{(byte) (v & 0b1111_1111)});
    }

    @Override
    public void writeShort(int v) throws IOException {
        var buffer = ByteBuffer.allocate(2);
        buffer.putShort((short) (v & 0b1111_1111_1111_1111));
        write(buffer.array());
    }

    @Override
    public void writeChar(int v) throws IOException {
        var buffer = ByteBuffer.allocate(2);
        buffer.putChar((char) (v & 0b1111_1111_1111_1111));
        write(buffer.array());
    }

    @Override
    public void writeInt(int v) throws IOException {
        var buffer = ByteBuffer.allocate(4);
        buffer.putInt(v);
        write(buffer.array());
    }

    @Override
    public void writeLong(long v) throws IOException {
        var buffer = ByteBuffer.allocate(8);
        buffer.putLong(v);
        write(buffer.array());
    }

    @Override
    public void writeFloat(float v) throws IOException {
        var buffer = ByteBuffer.allocate(4);
        buffer.putFloat(v);
        write(buffer.array());
    }

    @Override
    public void writeDouble(double v) throws IOException {
        var buffer = ByteBuffer.allocate(8);
        buffer.putDouble(v);
        write(buffer.array());
    }

    @Override
    public void writeBytes(String s) throws IOException {
        write(s.getBytes("UTF-8"));
    }

    @Override
    public void writeChars(String s) throws IOException {
        write(s.getBytes("UTF-8"));
    }

    @Override
    public void writeUTF(String s) throws IOException {
        write(s.getBytes("UTF-8"));
    }

    // DataInput

    @Override
    public void readFully(byte[] b) throws IOException {
        this.readFully(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int pos = this.position.get();
        int size = len - off;

        if (pos + size > this.count) {
            throw new IOException("End of stream");
        }

        for (int i = 0; i < size; i++) {
            b[off + i] = this.buf[pos + i];
        }

        this.position.set(pos + size);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        int pos = this.position.get();
        int bytesToSkip = Math.min(n, this.count - pos);
        pos += bytesToSkip;
        return bytesToSkip;
    }

    @Override
    public boolean readBoolean() throws IOException {
        var bytes = new byte[1];
        this.readFully(bytes);
        return bytes[0] == 0 ? false : true;
    }

    @Override
    public byte readByte() throws IOException {
        var bytes = new byte[1];
        this.readFully(bytes);
        return bytes[0];
    }

    @Override
    public int readUnsignedByte() throws IOException {
        var bytes = new byte[1];
        this.readFully(bytes);
        return bytes[0];
    }

    @Override
    public short readShort() throws IOException {
        var bytes = new byte[2];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        var bytes = new byte[2];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getShort();
    }

    @Override
    public char readChar() throws IOException {
        var bytes = new byte[2];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getChar();
    }

    @Override
    public int readInt() throws IOException {
        var bytes = new byte[4];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getInt();
    }

    @Override
    public long readLong() throws IOException {
        var bytes = new byte[8];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getLong();
    }

    @Override
    public float readFloat() throws IOException {
        var bytes = new byte[4];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getFloat();
    }

    @Override
    public double readDouble() throws IOException {
        var bytes = new byte[8];
        this.readFully(bytes);
        var buffer = ByteBuffer.wrap(bytes);
        return buffer.getDouble();
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
