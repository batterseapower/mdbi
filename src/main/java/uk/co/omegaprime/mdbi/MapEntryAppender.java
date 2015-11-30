package uk.co.omegaprime.mdbi;

interface MapEntryAppender<K, V> {
    V append(K key, V oldValue, V newValue);
}
