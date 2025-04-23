package net.haxy.xitdb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;

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

    void testSlice(Core core, Hasher hasher, int originalSize, long sliceOffset, long sliceSize) throws Exception {
        core.setLength(0);
        var db = new Database(core, hasher);
        var rootCursor = db.rootCursor();

        rootCursor.writePath(new Database.PathPart[]{
            new Database.ArrayListInit(),
            new Database.ArrayListAppend(),
            new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
            new Database.HashMapInit(),
            new Database.Context((cursor) -> {
                var values = new ArrayList<Long>();

                // create list
                for (int i = 0; i < originalSize; i++) {
                    long n = i * 2;
                    values.add(n);
                    cursor.writePath(new Database.PathPart[]{
                        new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes()))),
                        new Database.LinkedArrayListInit(),
                        new Database.LinkedArrayListAppend(),
                        new Database.WriteData(new Database.Uint(n))
                    });
                }

                // slice list
                var evenListCursor = cursor.readPath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even".getBytes())))
                });
                var evenListSliceCursor = cursor.writePath(new Database.PathPart[]{
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("even-slice".getBytes()))),
                    new Database.WriteData(evenListCursor.slotPtr.slot()),
                    new Database.LinkedArrayListInit(),
                    new Database.LinkedArrayListSlice(sliceOffset, sliceSize)
                });
            })
        });
    }

    void testLowLevelApi(Core core, Hasher hasher) throws Exception {
        // open and re-open database
        {
            // make empty database
            core.setLength(0);
            new Database(core, hasher);

            // re-open without error
            var db = new Database(core, hasher);
            var writer = db.core.writer();
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
            var hasherWithHashId = new Hasher(MessageDigest.getInstance("SHA-256"), hashId);

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
                    var writer = cursor.writer();
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

                    var barReader = cursor.reader();

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

                    var writer = cursor.writer();
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
                var barReader = barCursor.reader();
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
                            var writer = cursor.writer();
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

            // write key that conflicts with foo the first two bytes
            var smallConflictKey = db.md.digest("small conflict".getBytes());
            smallConflictKey[smallConflictKey.length-1] = fooKey[fooKey.length-1];
            smallConflictKey[smallConflictKey.length-2] = fooKey[fooKey.length-2];
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                new Database.WriteData(new Database.Bytes("small".getBytes()))
            });

            // write key that conflicts with foo the first four bytes
            var conflictKey = db.md.digest("conflict".getBytes());
            conflictKey[conflictKey.length-1] = fooKey[fooKey.length-1];
            conflictKey[conflictKey.length-2] = fooKey[fooKey.length-2];
            conflictKey[conflictKey.length-3] = fooKey[fooKey.length-3];
            conflictKey[conflictKey.length-4] = fooKey[fooKey.length-4];
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                new Database.WriteData(new Database.Bytes("hello".getBytes()))
            });

            // read conflicting key
            var helloCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var helloValue = helloCursor.readBytes(MAX_READ_BYTES);
            assertEquals("hello", new String(helloValue));

            // we can still read foo
            var barCursor2 = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
            });
            var barValue2 = barCursor2.readBytes(MAX_READ_BYTES);
            assertEquals("bar", new String(barValue2));

            // overwrite conflicting key
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                new Database.WriteData(new Database.Bytes("goodbye".getBytes()))
            });
            var goodbyeCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var goodbyeValue = goodbyeCursor.readBytes(MAX_READ_BYTES);
            assertEquals("goodbye", new String(goodbyeValue));

            // we can still read the old conflicting key
            var helloCursor2 = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-2),
                new Database.HashMapGet(new Database.HashMapGetValue(conflictKey))
            });
            var helloValue2 = helloCursor2.readBytes(MAX_READ_BYTES);
            assertEquals("hello", new String(helloValue2));

            // remove the conflicting keys
            {
                // foo's slot is an INDEX slot due to the conflict
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.INDEX, slot.tag());
                }

                // remove the small conflict key
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapRemove(smallConflictKey)
                });

                // the conflict key still exists in history
                assertNotEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));

                // the conflict key doesn't exist in the latest moment
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));

                // the other conflict key still exists
                assertNotEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                }));

                // foo's slot is still an INDEX slot due to the other conflicting key
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.INDEX, slot.tag());
                }

                // remove the conflict key
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapRemove(conflictKey)
                });

                // the conflict keys don't exist in the latest moment
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(smallConflictKey)),
                }));
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(conflictKey)),
                }));

                // foo's slot is now a KV_PAIR slot, becaus the branch was shortened
                {
                    var mapCursor = rootCursor.readPath(new Database.PathPart[]{
                        new Database.ArrayListGet(-1),
                    });
                    var indexPos = mapCursor.slot().value();
                    assertEquals(Tag.HASH_MAP, mapCursor.slot().tag());

                    var reader = core.reader();

                    var i = new BigInteger(fooKey).and(Database.BIG_MASK).intValueExact();
                    var slotPos = indexPos + (Slot.length * i);
                    core.seek(slotPos);
                    var slotBytes = new byte[Slot.length];
                    reader.readFully(slotBytes);
                    var slot = Slot.fromBytes(slotBytes);

                    assertEquals(Tag.KV_PAIR, slot.tag());
                }
            }

            {
                // overwrite foo with a uint
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Uint(42))
                });

                // read foo
                var uintValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readUint();
                assertEquals(42, uintValue);
            }

            {
                // overwrite foo with a int
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Int(-42))
                });

                // read foo
                var intValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readInt();
                assertEquals(-42, intValue);
            }

            {
                // overwrite foo with a float
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                    new Database.WriteData(new Database.Float(42.5))
                });

                // read foo
                var intValue = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(fooKey))
                }).readFloat();
                assertEquals(42.5, intValue);
            }

            // remove foo
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(fooKey)
            });

            // remove key that does not exist
            assertThrows(Database.KeyNotFoundException.class, () -> rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(db.md.digest("doesn't exist".getBytes()))
            }));

            // make sure foo doesn't exist anymore
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
            }));

            // non-top-level list
            {
                // write apple
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("apple".getBytes()))
                });

                // read apple
                var appleCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
                assertEquals("apple", new String(appleValue));

                // write banana
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("banana".getBytes()))
                });

                // read banana
                var bananaCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var bananaValue = bananaCursor.readBytes(MAX_READ_BYTES);
                assertEquals("banana", new String(bananaValue));

                // can't read banana in older array_list
                assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(1),
                }));

                // write pear
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("pear".getBytes()))
                });

                // write grape
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes("grape".getBytes()))
                });

                // read pear
                var pearCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-2),
                });
                var pearValue = pearCursor.readBytes(MAX_READ_BYTES);
                assertEquals("pear", new String(pearValue));

                // read grape
                var grapeCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(db.md.digest("fruits".getBytes()))),
                    new Database.ArrayListGet(-1),
                });
                var grapeValue = grapeCursor.readBytes(MAX_READ_BYTES);
                assertEquals("grape", new String(grapeValue));
            }
        }

        // append to top-level array_list many times, filling up the array_list until a root overflow occurs
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            var watKey = db.md.digest("wat".getBytes());
            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                    new Database.WriteData(new Database.Bytes(value.getBytes()))
                });
            }

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(i),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // add more slots to cause a new index block to be created.
            // a new index block will be created when i == 32 (the 33rd append).
            // during that transaction, return an error so the transaction is
            // cancelled, causing truncation to happen. this test ensures that
            // the new index block is NOT truncated. this is prevented by updating
            // the file size in the header immediately after making a new index block.
            // see `readArrayListSlot` for more.
            for (int i = Database.SLOT_COUNT + 1; i < Database.SLOT_COUNT * 2 + 1; i++) {
                var value = "wat" + i;

                final int index = i;

                try {
                    rootCursor.writePath(new Database.PathPart[]{
                        new Database.ArrayListInit(),
                        new Database.ArrayListAppend(),
                        new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                        new Database.HashMapInit(),
                        new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                        new Database.WriteData(new Database.Bytes(value.getBytes())),
                        new Database.Context((cursor) -> {
                            if (index == 32) {
                                throw new Exception();
                            }
                        })
                    });
                } catch (Exception e) {}
            }

            // try another append to make sure we still can.
            // if truncation destroyed the index block, this would fail.
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                new Database.WriteData(new Database.Bytes("wat32".getBytes()))
            });

            // slice so it contains exactly SLOT_COUNT,
            // so we have the old root again
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListSlice(Database.SLOT_COUNT)
            });

            // we can iterate over the remaining slots
            for (int i = 0; i < Database.SLOT_COUNT; i ++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(i),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey))
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // but we can't get the value that we sliced out of the array list
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(Database.SLOT_COUNT + 1)
            }));
        }

        // append to inner array_list many times, filling up the array_list until a root overflow occurs
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes(value.getBytes()))
                });
            }

            for (int i = 0; i < Database.SLOT_COUNT + 1; i++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(i),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // slice the inner array list so it contains exactly SLOT_COUNT,
            // so we have the old root again
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.ArrayListInit(),
                new Database.ArrayListSlice(Database.SLOT_COUNT)
            });

            // we can iterate over the remaining slots
            for (int i = 0; i < Database.SLOT_COUNT; i ++) {
                var value = "wat" + i;
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(i),
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // but we can't get the value that we sliced out of the array list
            assertEquals(null, rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1),
                new Database.ArrayListGet(Database.SLOT_COUNT + 1)
            }));

            // overwrite the last value with hello
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.WriteData(new Database.Bytes("hello".getBytes()))
            });

            // read last value
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("hello", value);
            }

            // overwrite the last value with goodbye
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.ArrayListInit(),
                new Database.ArrayListGet(-1),
                new Database.WriteData(new Database.Bytes("goodbye".getBytes()))
            });

            // read last value
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("goodbye", value);
            }

            // previous last value is still hello
            {
                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-2),
                    new Database.ArrayListGet(-1)
                });
                var value = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals("hello", value);
            }
        }

        // iterate over inner array_list
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // add wats
            for (int i = 0; i < 10; i++) {
                var value = "wat" + i;
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(new Database.Bytes(value.getBytes()))
                });

                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListGet(-1)
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // iterate over array_list
            {
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var nextCursor = iter.next();
                    var value = "wat" + i;
                    var value2 = new String(nextCursor.readBytes(MAX_READ_BYTES));
                    assertEquals(value, value2);
                    i += 1;
                }
                assertEquals(10, i);
            }

            // set first slot to .none and make sure iteration still works.
            // this validates that it correctly returns .none slots if
            // their flag is set, rather than skipping over them.
            {
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListGet(-1),
                    new Database.ArrayListInit(),
                    new Database.ArrayListGet(0),
                    new Database.WriteData(null)
                });
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    iter.next();
                    i += 1;
                }
                assertEquals(10, i);
            }

            // get list slot
            var listCursor = rootCursor.readPath(new Database.PathPart[]{
                new Database.ArrayListGet(-1)
            });
            assertEquals(10, listCursor.count());
        }

        // iterate over inner hash_map
        {
            core.setLength(0);
            var db = new Database(core, hasher);
            var rootCursor = db.rootCursor();

            // add wats
            for (int i = 0; i < 10; i++) {
                var value = "wat" + i;
                var watKey = db.md.digest(value.getBytes());
                rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                    new Database.HashMapInit(),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey)),
                    new Database.WriteData(new Database.Bytes(value.getBytes()))
                });

                var cursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1),
                    new Database.HashMapGet(new Database.HashMapGetValue(watKey))
                });
                var value2 = new String(cursor.readBytes(MAX_READ_BYTES));
                assertEquals(value, value2);
            }

            // add foo
            var fooKey = db.md.digest("foo".getBytes());
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetKey(fooKey)),
                new Database.WriteData(new Database.Bytes("foo".getBytes()))
            });
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapGet(new Database.HashMapGetValue(fooKey)),
                new Database.WriteData(new Database.Uint(42))
            });

            // remove a wat
            rootCursor.writePath(new Database.PathPart[]{
                new Database.ArrayListInit(),
                new Database.ArrayListAppend(),
                new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                new Database.HashMapInit(),
                new Database.HashMapRemove(db.md.digest("wat0".getBytes()))
            });

            // iterate over hash_map
            {
                var innerCursor = rootCursor.readPath(new Database.PathPart[]{
                    new Database.ArrayListGet(-1)
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    if (Arrays.equals(kvPair.hash, fooKey)) {
                        var key = new String(kvPair.keyCursor.readBytes(MAX_READ_BYTES));
                        assertEquals("foo", key);
                        assertEquals(42, kvPair.valueCursor.slotPtr.slot().value());
                    } else {
                        var value = kvPair.valueCursor.readBytes(MAX_READ_BYTES);
                        assert(Arrays.equals(kvPair.hash, db.md.digest(value)));
                    }
                    i += 1;
                }
                assertEquals(10, i);
            }

            // iterate over hash_map with writeable cursor
            {
                var innerCursor = rootCursor.writePath(new Database.PathPart[]{
                    new Database.ArrayListInit(),
                    new Database.ArrayListAppend(),
                    new Database.WriteData(rootCursor.readPathSlot(new Database.PathPart[]{new Database.ArrayListGet(-1)})),
                });
                var iter = innerCursor.iterator();
                int i = 0;
                while (iter.hasNext()) {
                    var kvPairCursor = iter.next();
                    var kvPair = kvPairCursor.readKeyValuePair();
                    if (Arrays.equals(kvPair.hash, fooKey)) {
                        kvPair.keyCursor.write(new Database.Bytes("bar".getBytes()));
                    }
                    i += 1;
                }
                assertEquals(10, i);
            }
        }

        {
            // slice linked_array_list
            testSlice(core, hasher, Database.SLOT_COUNT * 5 + 1, 10, 5);
        }
    }
}