superq
================

A flexible Python collection scaling to need.

The superq module provides functionality similar to an object-relational mapping (ORM) interface over a distributed data store. It can be used in place of standard collections like dictionaries and Queues and potentially eliminates a lot of network and db-oriented plumbing code.

Reducing the need to write code is what it's all about. Superqs allow you to focus on core logic without worrying about how you're going to move and manipulate data as you scale code from prototype to production.

Using the superq module, powerful functionality can be leveraged with just a few lines of code, helping developers quickly create scalable and performant applications.

## Design Philosophy

Under the hood, the superq module uses sqlite to provide query capability and support persistence.
In some ways superq could be considered to extend the sqlite philosophy and use cases. Just as sqlite provides a powerful lightweight alternative to the traditional client-server RDBMS, superq provides a lightweight, zero-configuration alternative to distributed datastores such as FoundationDB and MongoDB.

## Architecture

A superq instance can be owned by a thread, a process, or the network. This allows for maximum functionality without sacrificing performance and is the key to scalability.

The base case is a _detached_ superq intended for single-thread use. In this case superq essentially provides a versatile alternative to synchronized Python collections such as provided by the queue module. Important to note is that query functionality is not available to detached superqs and correspondingly access is very fast.

An _attached_ superq can be accessed from multiple threads and supports queries. This is accomplished through a local instance of the superq datastore backed by sqlite.

A _hosted_ superq is owned by a superq network node process which manages a public datastore instance. The node process provides local and remote access and supports secure connections through SSL.

The final type, a _distributed_ superq is under development but will eventually provide advanced datastore characteristics such as availability and partitioning.

## Usage

### Getting started

    from superq import superq, superqelem

### Creating the superq

    # a local non-shared, high-performance superq from a list
    sq = superq([1, 2, 3])

    # shareable within a process
    sq = superq(['a', 'b', 'c'], attach = True)

    # shareable across processes and the network
    sq = superq([.1, .2, .3], host = '127.0.0.1:9990', attach = True) 

### Non-scalar superq

The above examples show how superqs can be used to manage collections of scalar values. The more typical usage would be for non-scalar values. Here is an example storing custom objects in a superq:

    class Foo():
        def __init__(self, a, b):
            self.a = a
            self.b = b

    sq = superq([Foo('a', 1), Foo('b', 2)], keyCol = 'a', name = 'sq1', attach = True)

In the above example the superq is given a name and a class field is specified as the "key column". If keyCol is not specified, as in the scalar examples, an id field will be automatically generated and assigned to each value in the superq.

### Looking up an existing superq

    sq = superq('sq1', attach = True)

This is how you would look up a pre-existing superq that belongs to the process data store. The attach flag means that changes to the superq will be permanent and visible to other threads.

### Looking up a superqelem

superqs (sqs) are composed of superqelems (sqes) in a similar relationship to how database tables are composed of rows.

Generally the user wants to concern themselves only with user-defined classes. Superqs attempt to make this possible, but sometimes it's necessary to be aware of the underlying implementation.

    # here a superq element is retrieved by key
    foo = sq['a']

    # here a superq element is retrieved by index (assuming the keyCol is not an overlapping int)
    foo = sq[2]

In the above examples, a superqelem object will be returned if the superq is not aware of the user type. On the other hand, if the user type has already been specified, an object of that type will be returned.

One way to specify the type is by setting objSample before doing the lookup.

    sq.objSample = Foo('a', 1)

Now insteading of returning superqelems, the superq will make copies of the specified objSample object and de-marshal superqelems into those copies. In this way, the same superqelems can be mapped into different user classes depending on their desired use.

To explicitly retrieve a superqelem, use the superq .n method like so:

    foo = sq.n('a')

### Modifying a superqelem

    sq.n('a').b = 5

And just like that it is possible to propagate a change all the way to disk on another machine.

If the superq is unaware of any user type, it is possible to simplify this:

    sq['a'].b = 5

Or, if you have the user object, you can update the data store like this:

    foo.b = 5
    sq.update_elem(foo)

This above method requires that the user object support dynamic field assignments. When the superqelem is de-marshalled into the user object, prior to being returned to the user, a hidden key field will be assigned to it, so that the superq can look the object back up. If it is not possible to make that assignment (in `__slots__`-supporting classes for instance), then after retrieving the user object, you must retrieve the superqelem to perform an update.

### Querying a superq for a single value

    sqResult = sq.query(['a'], ['<self>'], 'b == 5')
    val = sqResult['a']

The above query method can be read like a simple SQL statement where the first field is the desired columns, the second field contains the relevant tables, and the third field specifies a conditional. So, read alternatively as:

    SELECT a FROM sq WHERE b == 5;

query() returns a new "detached" superq. `<self>` is simply a way to indicate the primary sq backing table.

### Querying a superq for multiple user objects

    sqResult = sq.query(['a', 'b'], ['<self>'], 'b > 4')
    sqResult.objSample = Foo('a', 1)

Now user objects can be simply retrieved:

    foo = sqResult[0]

Incidentally superqs also support iteration, so you could say:

    for foo in sqResult:
        foo.bar()

Assuming the existence of a bar method in the Foo class.

## Additional basic functionality

For now, please consult test.py for the exact set of supported superq functionality and additional examples of working with superqs.

## Advanced functionality

Would you like a network-accessible secure system log implemented as a circular queue?

    superq([], name = 'networkLog', host = 'ssl:1.1.1.1:1', maxlen = 1000, attach = True)

Now any of your applications can attach to the superq and simply .push() log messages onto it.

How about a network-enabled mutex?

    sq = superq([], 'name = networkMutex', host = '1.1.1.1:1', maxlen = 1, attach = True)
    sq.push(1)

Now just .pop() to acquire the mutex and .push(1) to release it.

Superqs could easily provide a thin comm layer for Python-enabled mobile clients.

Or be used simply as a thread-safe, multi-producer, multi-consumer queue.

There's really a tremendous number of uses superqs can be put to, from providing powerful synchronization and networking primitives to offering full querying capabilities without the need for database setup.

## Current status

Superqs are definitely not production-ready. I consider the code proof-of-concept right now. Despite the proto-stage of development that it is in, superq does already provide some interesting functionality as an inherently network-accessible, queryable Python collection.

Eventually version 1.0 will represent a complete and stable version of the API while 2.0 should provide full distributed functionality.

## About

I started this project to learn more about data and scalability and intend to eventually produce a useful object-relational mapping front-end integrated with a scalable, distributed datastore.

My hope is that the superq interface can help Python developers by providing a fundamentally-scalable collection that is as easy to use as the standard collections but far more powerful.

Future versions should greatly expand superq scalability, performance, capability, and stability. So stick around, fire me off some ideas, tell me how crappy my code is so I can fix it, or maybe use it as a start for something you want to build.

## License

The MIT License (MIT)

Copyright (c) 2015 Daniel Manesajian

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

