package com.ecommerce.streaming.support;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A {@link KeyValueIterator} over a fixed list, so a local state-store read can be stubbed
 * without standing up RocksDB. Records whether it was closed — the service reads stores inside
 * try-with-resources and leaking a RocksDB iterator in production pins memory.
 */
public final class ListKeyValueIterator<K, V> implements KeyValueIterator<K, V> {

    private final Iterator<KeyValue<K, V>> delegate;
    private final List<KeyValue<K, V>> entries;
    private boolean closed;
    private int position;

    public ListKeyValueIterator(List<KeyValue<K, V>> entries) {
        this.entries = List.copyOf(entries);
        this.delegate = this.entries.iterator();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public K peekNextKey() {
        if (position >= entries.size()) {
            throw new NoSuchElementException();
        }
        return entries.get(position).key;
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public KeyValue<K, V> next() {
        position++;
        return delegate.next();
    }
}
