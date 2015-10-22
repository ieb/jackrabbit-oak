package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

/**
 * The <tt>NoopStore</tt> returns null or empty for all query and find methods, while failing is a modification is requested
 *
 */
class NoopStore implements DocumentStore {
    
    static final NoopStore INSTANCE = new NoopStore();

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return null;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return null;
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return Collections.emptyList();
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        return Collections.emptyList();
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        throw new UnsupportedOperationException();

    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<Key, Condition>> toRemove) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        return null;
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        return null;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
    }

    @Override
    public void dispose() {
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return null;
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {

    }

    @Override
    public CacheStats getCacheStats() {
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        return null;
    }

    @Override
    public void setDocumentCreationCustomiser(DocumentCreationCustomiser customiser) {
    }

    @Override
    public String toMapPath(String relativePath, String absolutePath) {
        return relativePath;
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        return 0;
    }
}
