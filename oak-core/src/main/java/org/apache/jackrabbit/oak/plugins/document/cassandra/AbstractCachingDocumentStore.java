package org.apache.jackrabbit.oak.plugins.document.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.cache.NodeDocumentCache;
import org.apache.jackrabbit.oak.plugins.document.locks.NodeDocumentLocks;
import org.apache.jackrabbit.oak.plugins.document.locks.StripedNodeDocumentLocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by ieb on 15/02/2016.
 */
public abstract class AbstractCachingDocumentStore implements DocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCachingDocumentStore.class);
    protected static final boolean USECMODCOUNT = true;
    /**
     * Optional counter for changes to "_collisions" map ({@link NodeDocument#COLLISIONS}).
     */
    public static final String COLLISIONSMODCOUNT = "_collisionsModCount";
    private static final String MODCOUNT = "_modCount";

    private final Map<String, String> metadata;
    final DocumentStoreStatsCollector stats;
    NodeDocumentCache nodesCache;
    private Map<String, Long> cnUpdates = new ConcurrentHashMap<String, Long>();

    private NodeDocumentLocks locks;

    public AbstractCachingDocumentStore(DocumentMK.Builder builder, ImmutableMap<String, String> metadata) {
        this.metadata =  metadata;
        this.stats = builder.getDocumentStoreStatsCollector();
        this.locks = new StripedNodeDocumentLocks();
        this.nodesCache = builder.buildNodeDocumentCache(this, locks);

    }


    abstract <T extends Document> void internalDelete(Collection<T> collection, String key);

    abstract <T extends Document> void internalDelete(Collection<T> collection, List<String> key);

    abstract <T extends Document> int internalDelete(Collection<T> collection,
                                                    Map<String, Map<UpdateOp.Key, UpdateOp.Condition>> toRemove);

    abstract <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updateOps);

    abstract <T extends Document> void internalUpdate(Collection<T> collection, List<String> keys, UpdateOp updateOp);

    abstract <T extends Document> T internalCreateOrUpdate(Collection<T> collection, UpdateOp update, boolean b, boolean b1);

    @CheckForNull
    abstract <T extends Document> T internalReadDocumentsUncached(Collection<T> collection, String id, NodeDocument cachedDoc);

    abstract <T extends Document> Map<String, T> internalReadDocumentsUncached(Collection<T> collection, Set<String> keys);

    @CheckForNull
    private static NodeDocument unwrap(@Nonnull NodeDocument doc) {
        return doc == NodeDocument.NULL ? null : doc;
    }

    @Nonnull
    private static NodeDocument wrap(@CheckForNull NodeDocument doc) {
        return doc == null ? NodeDocument.NULL : doc;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Document> T castAsT(NodeDocument doc) {
        return (T) doc;
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return readDocumentCached(collection, key, maxCacheAge);
    }

    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        internalDelete(collection, key);
        invalidateCache(collection, key, true);
    }


    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        internalDelete(collection, keys);
        for (String key : keys) {
            invalidateCache(collection, key, true);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<UpdateOp.Key, UpdateOp.Condition>> toRemove) {
        int num = internalDelete(collection, toRemove);
        for (String id : toRemove.keySet()) {
            invalidateCache(collection, id, true);
        }
        return num;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        return internalCreate(collection, updateOps);
    }


    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        UpdateUtils.assertUnconditional(updateOp);
        internalUpdate(collection, keys, updateOp);
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        UpdateUtils.assertUnconditional(update);
        return internalCreateOrUpdate(collection, update, true, false);
    }


    @Override
    public <T extends Document> List<T> createOrUpdate(Collection<T> collection, List<UpdateOp> updateOps) {
        List<T> results = new ArrayList<T>(updateOps.size());
        for (UpdateOp update : updateOps) {
            results.add(createOrUpdate(collection, update));
        }
        return results;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        return internalCreateOrUpdate(collection, update, false, true);
    }


    @Override
    public CacheInvalidationStats invalidateCache() {
        for (NodeDocument nd : nodesCache.values()) {
            nd.markUpToDate(0);
        }
        return null;
    }

    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        //TODO: optimize me
        return invalidateCache();
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String id) {
        invalidateCache(collection, id, false);
    }

    private <T extends Document> void invalidateCache(Collection<T> collection, String id, boolean remove) {
        if (collection == Collection.NODES) {
            invalidateNodesCache(id, remove);
        }
    }

    private void invalidateNodesCache(String id, boolean remove) {
        Lock lock = locks.acquire(id);
        try {
            if (remove) {
                nodesCache.invalidate(id);
            } else {
                NodeDocument entry = nodesCache.getIfPresent(id);
                if (entry != null) {
                    entry.markUpToDate(0);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void dispose() {
        try {
            this.nodesCache.close();
        } catch (IOException ex) {
            LOG.warn("Error occurred while closing nodes cache", ex);
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String id) {
        if (collection != Collection.NODES) {
            return null;
        } else {
            NodeDocument doc = unwrap(nodesCache.getIfPresent(id));
            return castAsT(doc);
        }
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        // ignore
    }

    @Override
    public Iterable<CacheStats> getCacheStats() {
        return nodesCache.getCacheStats();
    }

    @Override
    public Map<String, String> getMetadata() {
        return metadata;
    }

    <T extends Document> Map<String, T> readDocumentCached(Collection<T> collection, Set<String> keys) {
        Map<String, T> documents = new HashMap<String, T>();

        if (collection == Collection.NODES) {
            for (String key : keys) {
                NodeDocument cached = nodesCache.getIfPresent(key);
                if (cached != null && cached != NodeDocument.NULL) {
                    T doc = castAsT(unwrap(cached));
                    documents.put(doc.getId(), doc);
                }
            }
        }

        Set<String> documentsToRead = Sets.difference(keys, documents.keySet());
        Map<String, T> readDocuments = internalReadDocumentsUncached(collection, documentsToRead);
        documents.putAll(readDocuments);

        if (collection == Collection.NODES) {
            for (T doc : readDocuments.values()) {
                nodesCache.putIfAbsent((NodeDocument) doc);
            }
        }

        return documents;
    }

    <T extends Document> T readDocumentCached(final Collection<T> collection, final String id, int maxCacheAge) {
        if (collection != Collection.NODES) {
            return internalReadDocumentsUncached(collection, id, null);
        } else {
            NodeDocument doc = null;
            if (maxCacheAge > 0) {
                // first try without lock
                doc = nodesCache.getIfPresent(id);
                if (doc != null) {
                    long lastCheckTime = doc.getLastCheckTime();
                    if (lastCheckTime != 0) {
                        if (maxCacheAge == Integer.MAX_VALUE || System.currentTimeMillis() - lastCheckTime < maxCacheAge) {
                            stats.doneFindCached(Collection.NODES, id);
                            return castAsT(unwrap(doc));
                        }
                    }
                }
            }
            try {
                Lock lock = locks.acquire(id);
                try {
                    // caller really wants the cache to be cleared
                    if (maxCacheAge == 0) {
                        invalidateNodesCache(id, true);
                        doc = null;
                    }
                    final NodeDocument cachedDoc = doc;
                    doc = nodesCache.get(id, new Callable<NodeDocument>() {
                        @Override
                        public NodeDocument call() throws Exception {
                            NodeDocument doc = (NodeDocument) internalReadDocumentsUncached(collection, id, cachedDoc);
                            if (doc != null) {
                                doc.seal();
                            }
                            return wrap(doc);
                        }
                    });
                    // inspect the doc whether it can be used
                    long lastCheckTime = doc.getLastCheckTime();
                    if (lastCheckTime != 0 && (maxCacheAge == 0 || maxCacheAge == Integer.MAX_VALUE)) {
                        // we either just cleared the cache or the caller does
                        // not care;
                    } else if (lastCheckTime != 0 && (System.currentTimeMillis() - lastCheckTime < maxCacheAge)) {
                        // is new enough
                    } else {
                        // need to at least revalidate
                        NodeDocument ndoc = (NodeDocument) internalReadDocumentsUncached(collection, id, cachedDoc);
                        if (ndoc != null) {
                            ndoc.seal();
                        }
                        doc = wrap(ndoc);
                        nodesCache.put(doc);
                    }
                } finally {
                    lock.unlock();
                }
                return castAsT(unwrap(doc));
            } catch (ExecutionException e) {
                throw new IllegalStateException("Failed to load document with " + id, e);
            }
        }
    }




    void maintainUpdateStats(Collection collection, String key) {
        if (collection == Collection.CLUSTER_NODES) {
            synchronized (this) {
                Long old = cnUpdates.get(key);
                old = old == null ? Long.valueOf(1) : old + 1;
                cnUpdates.put(key, old);
            }
        }
    }

    static void addUpdateCounters(UpdateOp update) {
        if (hasChangesToCollisions(update)) {
            update.increment(COLLISIONSMODCOUNT, 1);
        }
        update.increment(MODCOUNT, 1);
    }
    private static boolean hasChangesToCollisions(UpdateOp update) {
        if (!USECMODCOUNT) {
            return false;
        } else {
            for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> e : checkNotNull(update).getChanges().entrySet()) {
                UpdateOp.Key k = e.getKey();
                UpdateOp.Operation op = e.getValue();
                if (op.type == UpdateOp.Operation.Type.SET_MAP_ENTRY) {
                    if (NodeDocument.COLLISIONS.equals(k.getName())) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

}
