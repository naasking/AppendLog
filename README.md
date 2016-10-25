# AppendLog

This library provides an efficient, asynchronous, append-only log abstraction,
which is most often found in distributed consensus algorithms like Paxos and Raft,
and more recently in event sourcing.

The log contents are completely opaque as it exposes a streaming API for atomically
writing to the log:

    /// <summary>
    /// Interface for an atomic, durable transaction log.
    /// </summary>
    public interface IAppendLog : IDisposable
    {
        /// <summary>
        /// The first transaction.
        /// </summary>
        TransactionId First { get;  }

        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <returns>A stream for writing.</returns>
        IDisposable Append(out Stream output, out TransactionId tx);

        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="lastEvent"/>.
        /// </summary>
        /// <param name="lastEvent">The last event seen.</param>
        /// <returns>An enumerator over the transactions that occurred since the given event.</returns>
        IEventEnumerator Replay(TransactionId lastEvent);
    }

    /// <summary>
    /// An event enumerator.
    /// </summary>
    public interface IEventEnumerator : IDisposable
    {
        /// <summary>
        /// The current transaction.
        /// </summary>
        TransactionId Transaction { get; }

        /// <summary>
        /// The stream containing the current event.
        /// </summary>
        Stream Stream { get; }

        /// <summary>
        /// Asynchronously advance the enumerator to the next event.
        /// </summary>
        /// <returns></returns>
        Task<bool> MoveNext();
    }

Writing to the log involves simply calling `Append()`, which returns
a bounded stream to which you can write, and a handle representing the
transaction. When you dispose the handle, the output stream is checked
for any data, and if data was written, then the log is atomically
updated with that data. Only a single writer can call Append() at any
given time.

You can utilize any serialization API that uses the standard .NET
System.IO.Stream abstractions to write to the output stream, so the
actual content of the event log is entirely domain-specific. You
could use JSON, XML, BinaryFormatter, or some mix of all of the above
if desired.

Each TransactionId is a marker for the beginning of an `Append()`
event, which can be later used to replay the log's events.
`IAppendLog.First` defines the log's first transaction.

The replay API provides a simple and efficient interface for reading
log contents from a given marker. This is often used for log
replication in distributed consensus, and for building caches in
event sourcing.

# Status

I believe the API to be complete and sufficient, but more testing is
needed, particularly for BoundedStream. The simple file format is mostly
settled for now.

On an AMD FX-8120 with an SSD, I get ~80,000-100,000 writes/second,
which corresponds roughly to the raw buffered I/O speed of FileStream.
This is precisely what's expected given log append is just a thin
wrapper around FileStream.

Durable commits require flushing to disk after all writes are
complete, which yields about ~800 tx/second due to two flushes. The
first ensures that an appended entry is written to disk, the second
ensures the log header is updated to point to the new entry.

# Future Work

 * The append handle is currently just an opaque IDisposable, which
   breaks with the ubiquitous async API of IAppendLog. Perhaps define
   a new async disposable interface.
 * The single writer still needs to do a bit of seeking to write the
   header at the log start, but there might be a disk format that
   requires no seeking. For instance, a layout where the log file
   consists of fixed-size chunks, so no matter the state of the Log
   we know where the chunk headers are and we can find the chunk
   corresponding to the last complete transaction by back tracking
   from the position at the file's end.
 * Concurrent writing via an IAppendLog wrapper around FileLog.
 * Log replication via an IAppendLog implemented backed by Raft?

# LICENSE

LGPL v2.1