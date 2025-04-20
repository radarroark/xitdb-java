package net.haxy.xitdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.security.MessageDigest;

class DatabaseTest {
    static long MAX_READ_BYTES = 1024;

    @Test
    void testLowLevelApi() throws Exception {
        try (var ram = new RandomAccessMemory()) {
            var core = new CoreMemory(ram);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testLowLevelApi(core, hasher);
        }

        var file = File.createTempFile("database", "");
        file.deleteOnExit();

        try (var raf = new RandomAccessFile(file, "rw")) {
            var core = new CoreFile(raf);
            var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
            testLowLevelApi(core, hasher);
        }
    }

    void testLowLevelApi(Core core, Hasher hasher) throws Exception {
        // open and re-open database
        {
            // make empty database
            core.setLength(0);
            new Database(core, hasher);

            // re-open without error
            var db = new Database(core, hasher);
            var writer = db.core.getWriter();
            db.core.seek(0);
            writer.writeByte('g');

            // re-open with error
            assertThrows(Database.InvalidDatabaseException.class, () -> new Database(core, hasher));

            // modify the version
            db.core.seek(0);
            writer.writeByte('x');
            db.core.seek(4);
            writer.writeShort(Database.VERSION + 1);

            // re-open with error
            assertThrows(Database.InvalidVersionException.class, () -> new Database(core, hasher));
        }

        // save hash id in header
        {
            var hashId = Hasher.stringToId("sha1");
            var hasherWithHashId = new Hasher(MessageDigest.getInstance("SHA-1"), hashId);

            // make empty database
            core.setLength(0);
            var db = new Database(core, hasherWithHashId);

            assertEquals(hashId, db.header.hashId());
            assertEquals("sha1", Hasher.idToString(db.header.hashId()));
        }

        // array_list of hash_maps
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // write foo -> bar with a writer
            var fooKey = db.md.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertEquals(Tag.NONE, cursor.slot().tag());
                    var writer = cursor.getWriter();
                    writer.write("bar".getBytes());
                    writer.finish();
                })
            });

            // read foo
            {
                var barCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                });
                assertEquals(3, barCursor.count());
                var barValue = barCursor.readBytes(MAX_READ_BYTES);
                assertEquals("bar", new String(barValue));

                // TODO: test wrapping ReadCursor.Reader in a BufferedReader
            }

            // read foo from ctx
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertNotEquals(Tag.NONE, cursor.slot().tag());

                    var value = cursor.readBytes(MAX_READ_BYTES);
                    assertEquals("bar", new String(value));

                    var barReader = cursor.getReader();

                    // read into buffer
                    var barBytes = new byte[10];
                    var barSize = barReader.read(barBytes);
                    assertEquals("bar", new String(barBytes, 0, barSize));
                    barReader.seek(0);
                    assertEquals(3, barReader.read(barBytes));
                    assertEquals("bar", new String(barBytes, 0, 3));

                    // read one char at a time
                    {
                        var ch = new byte[1];
                        barReader.seek(0);

                        barReader.readFully(ch);
                        assertEquals("b", new String(ch));

                        barReader.readFully(ch);
                        assertEquals("a", new String(ch));

                        barReader.readFully(ch);
                        assertEquals("r", new String(ch));

                        assertThrows(Database.EndOfStreamException.class, () -> barReader.readFully(ch));

                        barReader.seek(1);
                        assertEquals('a', (char)barReader.readByte());

                        barReader.seek(0);
                        assertEquals('b', (char)barReader.readByte());
                    }
                })
            });

            // overwrite foo -> baz
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.Context((cursor) -> {
                    assertNotEquals(Tag.NONE, cursor.slot().tag());

                    var writer = cursor.getWriter();
                    writer.write("x".getBytes());
                    writer.write("x".getBytes());
                    writer.write("x".getBytes());
                    writer.seek(0);
                    writer.write("b".getBytes());
                    writer.seek(2);
                    writer.write("z".getBytes());
                    writer.seek(1);
                    writer.write("a".getBytes());
                    writer.finish();

                    var value = cursor.readBytes(MAX_READ_BYTES);
                    assertEquals("baz", new String(value));
                })
            });

            // write bar -> longstring
            var barKey = db.md.digest("bar".getBytes());
            {
                var barCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                barCursor.write(new Database.Bytes("longstring".getBytes()));

                // the slot tag is BYTES because the byte array is > 8 bytes long
                assertEquals(Tag.BYTES, barCursor.slot().tag());

                // writing again returns the same slot
                {
                    var nextBarCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    nextBarCursor.writeIfEmpty(new Database.Bytes("longstring".getBytes()));
                    assertEquals(barCursor.slot().value(), nextBarCursor.slot().value());
                }

                // writing with write returns a new slot
                {
                    var nextBarCursor = rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                    });
                    nextBarCursor.write(new Database.Bytes("longstring".getBytes()));
                    assertNotEquals(barCursor.slot().value(), nextBarCursor.slot().value());
                }
            }

            // read bar
            {
                var fooCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
                assertEquals("longstring", new String(fooValue));
            }

            // write bar -> shortstr
            {
                var barCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(barKey))
                });
                barCursor.write(new Database.Bytes("shortstr".getBytes()));

                // the slot tag is SHORT_BYTES because the byte array is <= 8 bytes long
                assertEquals(Tag.SHORT_BYTES, barCursor.slot().tag());
                assertEquals(8, barCursor.count());

                // make sure that SHORT_BYTES can be read with a reader
                var barReader = barCursor.getReader();
                var barValue = new byte[(int)barCursor.count()];
                barReader.readFully(barValue);
                assertEquals("shortstr", new String(barValue));
            }

            // if error in ctx, db doesn't change
            {
                var sizeBefore = core.length();

                try {
                    rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                        new Database.Context((cursor) -> {
                            var writer = cursor.getWriter();
                            writer.write("this value won't be visible".getBytes());
                            writer.finish();
                            throw new Exception();
                        })
                    });
                } catch (Exception e) {}

                // read foo
                var valueCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                });
                var value = valueCursor.readBytes(null); // make sure null max size works
                assertEquals("baz", new String(value));

                // verify that the db is properly truncated back to its original size after error
                var sizeAfter = core.length();
                assertEquals(sizeBefore, sizeAfter);
            }

            // read foo into buffer
            {
                var barCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                });
                var barBufferValue = barCursor.readBytes(MAX_READ_BYTES);
                assertEquals("baz", new String(barBufferValue));
            }

            // write bar and get a pointer to it
            var barSlot = rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(barKey)),
                new Database.WriteData(new Database.Bytes("bar".getBytes()))
            }).slot();

            // overwrite foo -> bar using the bar pointer
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.WriteData(barSlot)
            });
            var barCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var barValue = barCursor.readBytes(MAX_READ_BYTES);
            assertEquals("bar", new String(barValue));

            // can still read the old value
            var bazCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var bazValue = bazCursor.readBytes(MAX_READ_BYTES);
            assertEquals("baz", new String(bazValue));

            // key not found
            var notFoundKey = db.md.digest("this doesn't exist".getBytes());
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(notFoundKey))
            }));
        }
    }
}