xitdb is an immutable database written in Java. It is available [on Clojars](https://clojars.org/io.github.radarroark/xitdb).

* Each transaction efficiently creates a new "copy" of the database, and past copies can still be read from.
* It supports writing to a file as well as purely in-memory use.
* No query engine of any kind. You just write data structures (primarily an `ArrayList` and `HashMap`) that can be nested arbitrarily.
* No dependencies besides the Java standard library (currently requires Java 17).
* This project is a port of the [original Zig version](https://github.com/radarroark/xitdb).
* To use it from Clojure, see [xitdb-clj](https://github.com/codeboost/xitdb-clj) for a nice wrapper library, or just [use java interop](https://github.com/radarroark/xitdb-clj-example).

This database was originally made for the [xit version control system](https://github.com/radarroark/xit), but I bet it has a lot of potential for other projects. The combination of being immutable and having an API similar to in-memory data structures is pretty powerful. Consider using it instead of SQLite for your Java projects: it's simpler, it's pure Java, and it creates no impedence mismatch with your program the way SQL databases do.

## Example

In this example, we create a new database, write some data in a transaction, and read the data afterwards.

```java
try (var raf = new RandomAccessBufferedFile(new File("main.db"), "rw")) {
    // init the db
    var core = new CoreBufferedFile(raf);
    var hasher = new Hasher(MessageDigest.getInstance("SHA-1"));
    var db = new Database(core, hasher);

    // to get the benefits of immutability, the top-level data structure
    // must be an ArrayList, so each transaction is stored as an item in it
    var history = new WriteArrayList(db.rootCursor());

    // this is how a transaction is executed. we call history.appendContext,
    // providing it with the most recent copy of the db and a context
    // object. the context object has a method that will run before the
    // transaction has completed. this method is where we can write
    // changes to the db. if any error happens in it, the transaction
    // will not complete and the db will be unaffected.
    //
    // after this transaction, the db will look like this if represented
    // as JSON (in reality the format is binary):
    //
    // {"foo": "foo",
    //  "bar": "bar",
    //  "fruits": ["apple", "pear", "grape"],
    //  "people": [
    //    {"name": "Alice", "age": 25},
    //    {"name": "Bob", "age": 42}
    //  ]}
    history.appendContext(history.getSlot(-1), (cursor) -> {
        var moment = new WriteHashMap(cursor);

        moment.put("foo", new Database.Bytes("foo"));
        moment.put("bar", new Database.Bytes("bar"));

        var fruitsCursor = moment.putCursor("fruits");
        var fruits = new WriteArrayList(fruitsCursor);
        fruits.append(new Database.Bytes("apple"));
        fruits.append(new Database.Bytes("pear"));
        fruits.append(new Database.Bytes("grape"));

        var peopleCursor = moment.putCursor("people");
        var people = new WriteArrayList(peopleCursor);

        var aliceCursor = people.appendCursor();
        var alice = new WriteHashMap(aliceCursor);
        alice.put("name", new Database.Bytes("Alice"));
        alice.put("age", new Database.Uint(25));

        var bobCursor = people.appendCursor();
        var bob = new WriteHashMap(bobCursor);
        bob.put("name", new Database.Bytes("Bob"));
        bob.put("age", new Database.Uint(42));
    });

    // get the most recent copy of the database, like a moment
    // in time. the -1 index will return the last index in the list.
    var momentCursor = history.getCursor(-1);
    var moment = new ReadHashMap(momentCursor);

    // we can read the value of "foo" from the map by getting
    // the cursor to "foo" and then calling readBytes on it
    var fooCursor = moment.getCursor("foo");
    var fooValue = fooCursor.readBytes(MAX_READ_BYTES);
    assertEquals("foo", new String(fooValue));

    // to get the "fruits" list, we get the cursor to it and
    // then pass it to the ArrayList constructor
    var fruitsCursor = moment.getCursor("fruits");
    var fruits = new ReadArrayList(fruitsCursor);
    assertEquals(3, fruits.count());

    // now we can get the first item from the fruits list and read it
    var appleCursor = fruits.getCursor(0);
    var appleValue = appleCursor.readBytes(MAX_READ_BYTES);
    assertEquals("apple", new String(appleValue));
}
```

## Initializing a Database

A `Database` is initialized with an implementation of the `Core` interface, which determines how the i/o is done. There are three implementations of `Core` in this library: `CoreBufferedFile`, `CoreFile`, and `CoreMemory`.

* `CoreBufferedFile` databases, like in the example above, write to a file while using an in-memory buffer to dramatically improve performance. This is highly recommended if you want to create a file-based database.
* `CoreFile` databases use no buffering when reading and writing data. You can initialize it like in the example above, except with a `RandomAccessFile` instance. This is almost never necessary but it's useful as a benchmark comparison with `CoreBufferedFile` databases.
* `CoreMemory` databases work completely in memory. You can initialize it like in the example above, except with a `RandomAccessMemory` instance.

Usually, you want to use a top-level `ArrayList` like in the example above, because that allows you to store a reference to each copy of the database (which I call a "moment"). This is how it supports transactions, despite not having any rollback journal or write-ahead log. It's an append-only database, so the data you are writing is invisible to any reader until the very last step, when the top-level list's header is updated.

You can also use a top-level `HashMap`, which is useful for ephemeral databases where immutability or transaction safety isn't necessary. Since xitdb supports in-memory databases, you could use it as an over-the-wire serialization format. Much like "Cap'n Proto", xitdb has no encoding/decoding step: you just give the buffer to xitdb and it can immediately read from it.

## Types

In xitdb there are a variety of immutable data structures that you can nest arbitrarily:

* `HashMap` contains key-value pairs stored with a hash
* `HashSet` is like a `HashMap` that only sets the keys; it is useful when only checking for membership
* `CountedHashMap` and `CountedHashSet` are just a `HashMap` and `HashSet` that maintain a count of their contents
* `ArrayList` is a growable array
* `LinkedArrayList` is like an `ArrayList` that can also be efficiently sliced and concatenated

All data structures use the hash array mapped trie, invented by Phil Bagwell. The `LinkedArrayList` is based on his later work on RRB trees. These data structures were originally made immutable and widely available by Rich Hickey in Clojure. To my knowledge, they haven't been available in any open source database until xitdb.

There are also scalar types you can store in the above-mentioned data structures:

* `Bytes` is a byte array
* `Uint` is an unsigned 64-bit int
* `Int` is a signed 64-bit int
* `Float` is a 64-bit float

You may also want to define custom types. For example, you may want to store a big integer that can't fit in 64 bits. You could just store this with `Bytes`, but when reading the byte array there wouldn't be any indication that it should be interpreted as a big integer.

In xitdb, you can optionally store a format tag with a byte array. A format tag is a 2 byte tag that is stored alongside the byte array. Readers can use it to decide how to interpret the byte array. Here's an example of storing a random 256-bit number with `bi` as the format tag:

```java
var randomBigInt = new BigInteger(256, new java.util.Random());
moment.put("random-number", new Database.Bytes(randomBigInt.toByteArray(), "bi".getBytes()));
```

Then, you can read it like this:

```java
var randomNumberCursor = moment.getCursor("random-number");
var randomNumber = randomNumberCursor.readBytesObject(MAX_READ_BYTES);
assertEquals("bi", new String(randomNumber.formatTag()));
var randomBigInt = new BigInteger(randomNumber.value());
```

There are many types you may want to store this way. Maybe an ISO-8601 date like `2026-01-01T18:55:48Z` could be stored with `dt` as the format tag. It's also great for storing custom classes. Just define the class, serialize it as a byte array using whatever mechanism you wish, and store it with a format tag. Keep in mind that format tags can be *any* 2 bytes, so there are 65536 possible format tags.

## Thread Safety

It is possible to read the database from multiple threads without locks, even while writes are happening. This is a big benefit of immutable databases. However, each thread needs to use its own `Database` instance. You can do this by creating a `ThreadLocal`. See [the multithreading test](https://github.com/radarroark/xitdb-java/blob/d7cf0869cf0f66eca823051dfbdec0ab5e5a09cb/src/test/java/io/github/radarroark/xitdb/DatabaseTest.java#L201) for an example of this. Also, keep in mind that writes still need to come from one thread at a time.
