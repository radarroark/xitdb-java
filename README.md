xitdb is an immutable database written in Java. It is available [on Clojars](https://clojars.org/io.github.radarroark/xitdb).

* Each transaction efficiently creates a new "copy" of the database, and past copies can still be read from.
* It supports writing to a file as well as purely in-memory use.
* No query engine of any kind. You just write data structures (primarily an `ArrayList` and `HashMap`) that can be nested arbitrarily.
* No dependencies besides the Java standard library (currently requires Java 21).
* This project is a port of the [original Zig version](https://github.com/radarroark/xitdb).
* To learn how to use this library from Clojure, see [this example project](https://github.com/radarroark/xitdb-clj-example).

This database was originally made for the [xit version control system](https://github.com/radarroark/xit), but I bet it has a lot of potential for other projects. The combination of being immutable and having an API similar to in-memory data structures is pretty powerful. Consider using it instead of SQLite for your Java projects: it's simpler, it's pure Java, and it creates no impedence mismatch with your program the way SQL databases do.

Usually, you want to use a top-level `ArrayList` like in the example below, because that allows you to store a reference to each copy of the database (which I call a "moment"). This is how it supports transactions, despite not having any rollback journal or write-ahead log. It's an append-only database, so the data you are writing is invisible to any reader until the very last step, when the top-level list's header is updated.

You can also use a top-level `HashMap`, which is useful for ephemeral databases where immutability or transaction safety isn't necessary. Since xitdb supports in-memory databases, you could use it as an over-the-wire serialization format. Much like "Cap'n Proto", xitdb has no encoding/decoding step: you just give the buffer to xitdb and it can immediately read from it.

The `HashMap` and `ArrayList` are based on the hash array mapped trie from Phil Bagwell. There is also a `LinkedArrayList`, which is based on the RRB tree, also from Phil Bagwell. It is similar to an `ArrayList`, except it can be efficiently sliced and concatenated. At some point I'll get off my lazy ass and provide better documentation, but for now, check out the example below and the tests.

```java
try (var raf = new RandomAccessFile(new File("main.db"), "rw")) {
    // init the db
    var core = new CoreFile(raf);
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
