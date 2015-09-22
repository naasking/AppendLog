# AppendLog

This library provides an efficient append-only log abstraction, which is
most often found in distributed consensus algorithms like Paxos and Raft,
and more recently in event sourcing.

The log contents are completely opaque as it exposes a streaming API
for atomically writing to the log:

    /// <summary>
    /// Interface for an atomic, durable transaction log.
    /// </summary>
    public interface IAppendLog : IDisposable
    {
        /// <summary>
        /// Enumerate the sequence of transactions since <paramref name="lastEvent"/>.
        /// </summary>
        /// <param name="lastEvent">The last event seen.</param>
        /// <returns>A sequence of transactions since the given event.</returns>
        IEnumerable<KeyValuePair<TransactionId, Stream>> Replay(TransactionId lastEvent);

        /// <summary>
        /// Replay the log to a stream.
        /// </summary>
        /// <param name="lastEvent">The event at which to replay.</param>
        /// <param name="output">The stream to write to.</param>
        void ReplayTo(TransactionId lastEvent, Stream output);
		
        /// <summary>
        /// Atomically append data to the durable store.
        /// </summary>
        /// <param name="txid">Transaction identifier.</param>
        /// <returns>A stream for writing.</returns>
        Stream Append(out TransactionId txid);
    }

Writing to the log involves simply calling `Append()`, which returns
a stream to which you can write, but not read. When you dispose the
stream, the log is updated atomically with the new data. Only a single
writer can call Append() at any given time, and this exclusion works
across processes too. Writing to the stream will most often utilize
some serialization API to write out application-specific types, ie.
commands for distributed consensus, events for event sourcing, etc.

Each TransactionId is a marker for the beginning of an `Append()`
event, which can be later used to replay the log's events.
`TransactionId.First` defines the universal first transaction.

The replay API provides a simple and efficient interface for reading
log contents from a given marker. This is often used for log
replication in distributed consensus, and for building caches in
event sourcing.

# Status

I believe the API to be complete and sufficient, and I also believe
the current implement to be correct, but this hasn't yet been tested.

The file format is still in flux until an official release.

# Future Work

I considered a memory-mapped implementation, but locking semantics
across processes weren't clear, and the benchmarks I'd seen didn't
convey much benefit to memory mapped files for this purpose. Streams
are also quite ubiquitous, particularly for de/serialization.

# LICENSE

LGPL v2.1