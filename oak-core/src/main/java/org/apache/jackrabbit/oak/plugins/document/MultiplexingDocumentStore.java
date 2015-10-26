package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.util.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The <tt>MultiplexingDocumentStore</tt> wraps two or more <tt>DocumentStore</tt> instances
 * 
 * <p>Multiplexing is performed only for the {@link Collection#NODES nodes} collection.</p>
 * 
 * <p>This store contains one root store, which by default holds all the nodes, and at least
 * one sub-store, which holds all document nodes below certain paths. This concept is similar
 * to the Unix mounts, where a filesystem can be mounted below a certain point.</p>
 * 
 */
public class MultiplexingDocumentStore implements DocumentStore {
    
    // === Implementation Notes === 
    // 
    // The MultiplexingDocumentStore assumes that paths are most of the time hierarchical and
    // uses those paths to decide on which store to operate. There are notable exceptions to
    // this approach, namely:
    //
    // 1. Split documents
    // 2. Long (hashed) paths
    // 
    // These exceptions need special handling, and some of those are (for the moment ) not optimal
    // from a design and / or performance point of view. The approaches must evolve for this 
    // implementation to make it back into Oak proper.
    // 
    // == Creating documents which don't have a hierarchical id == 
    // 
    // When creating a document we must have a path defined for the document. If the key is not
    // hierarchical we need to infer the path using other means. 
    //
    // For split documents, we assume that the creator of the document will have set the 
    // original document's path in the UpdateOp, as the split document must reside in the same
    // store as the original document.
    //
    // For long paths we expect the change to contain a 'path' property which indicates the path
    // of the document.
    //
    // == Other operations for documents which don't have a hierarchical id ==
    //
    // For all other operations we assume that if the document exists it only exists in one of 
    // the stores. With that assumption we can simply call find or query on all stores and then
    // use the one that holds it as the owner.
    
    private final DocumentStore root;
    
    private final List<DocumentStoreMount> mounts;

    private MultiplexingDocumentStore(DocumentStore root, List<DocumentStoreMount> mounts) {
        this.root = root;
        
        this.mounts = Lists.newArrayList();
        this.mounts.add(new DocumentStoreMount(root, "/", "", Builder.MAPPED_PATHS));
        this.mounts.addAll(mounts);
        Collections.sort(this.mounts);
        
        for ( DocumentStoreMount mount : this.mounts ) {
            mount.getStore().setDocumentCreationCustomiser(new DefaultDocumentCreationCustomiser(this));
        }
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    private boolean doNotMap(String path) {
        return path.startsWith("/oak:");
    }
    
    private DocumentKey asDocumentKey(String key) {
        return DocumentKey.fromKey(key);
    }
    
    private DocumentStore findOwnerStore(String key, Collection<?> collection, OnFailure onFailure) {
        return findOwnerStore(asDocumentKey(key), collection, onFailure);
    }
    
    private DocumentStore findOwnerStore(UpdateOp update, Collection<?> collection, OnFailure onFailure) {
        DocumentKey key;
        final UpdateOp.Operation op = update.getChanges().get(new UpdateOp.Key(NodeDocument.PATH,null));
        if(op != null) {
            // If a long path was transformed to a hash, use the original path here
            key = DocumentKey.fromPath(op.value.toString());
        } else if  ( update.splitFrom != null ){
            // For split documents, use the originator document's id
            key = DocumentKey.fromKey(update.splitFrom);
        } else { 
            // Otherwise, we build from the id
            key = DocumentKey.fromKey(update.getId());
        }
        if(doNotMap(key.getPath())) {
            return root;
        }
        return findOwnerStore(key, collection, onFailure);
    }
    

    private DocumentStore findOwnerStore(DocumentKey key, Collection<?> collection, OnFailure onFailure) {
        
        if ( collection != Collection.NODES ) {
            return root;
        }
        
        String path = key.getPath();

        if ( path != null ) {
        
            if(doNotMap(path)) {
                return root;
            }
            
            // mounts is ordered longest mount path first, so the first match is the desired mount.
            for ( DocumentStoreMount mount : mounts ) {
                if ( mount.contains(path) ) {
                    return mount.getStore();
                }
            }
            
        }

        if ( onFailure == OnFailure.CALL_FIND_FOR_MATCHING_KEY) {
            // split documents don't have reasonable id so we can't locate
            // a store for them beforehand ;
            for ( DocumentStoreMount mount : mounts ) {
                if ( mount.getStore().find(Collection.NODES, key.getValue()) != null ) {
                    return mount.getStore();
                }
            }

            // if we have looked for the documents everywhere and still have not fond them return a no-op store
            // this is safe since for query operations it will return no results ( which we already know to be true )
            // and for any modifications it will fail fast
            return NoopStore.INSTANCE;
        }

        throw new IllegalArgumentException("Could not find an owning store for key " + key.getValue() + " ( matched path = " + key.getPath() + ")");
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        
        return findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
            .find(collection, key, maxCacheAge);
    }
    
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {

        if ( collection != Collection.NODES ) {
            return root.query(collection, fromKey, toKey, limit);
        }
        
        DocumentKey from = asDocumentKey(fromKey);
        DocumentKey to = asDocumentKey(toKey);
        
        List<T> main;
        if ( from.getPath() != null && to.getPath() != null ) {
        
            DocumentStore owner = findOwnerStore(from, collection, OnFailure.FAIL_FAST);
            main = owner.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        } else {
            main = Lists.newArrayList();
        }
        // TODO - do we need a query on the contributing stores or is a 'find' enough?
        for ( DocumentStore contributing : findStoresContainedBetween(from, to)) {
            // TODO - stop the query if we know that we have enough results, e.g. we
            // have hit the limit with results between fromKey and contributing.getMountPath()  
            main.addAll(contributing.query(collection, fromKey, toKey, indexedProperty, startValue, limit));
        }
        
        // TODO - merge the results instead of full sorting
        Collections.sort(main, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return o1.getId().compareTo(o2.getId());
            }
        });
        
        return main.size() > limit ? main.subList(0, limit) : main;
    }    

    private List<DocumentStore> findStoresContainedBetween(DocumentKey from, DocumentKey to) {
        
        boolean hasUncertainPaths = from.getPath() == null || to.getPath() == null;
        List<DocumentStore> contained = Lists.newArrayList();
        for ( DocumentStoreMount mount : mounts ) {
            String storePath = mount.getMountPath();
            if ( hasUncertainPaths || 
                    (from.getPath().compareTo(storePath) < 0 && storePath.compareTo(to.getPath()) < 0 ) ) {
                contained.add(mount.getStore());
            }
        }
        return contained;
    }
    
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        remove(collection, Collections.singletonList(key));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        if ( collection != Collection.NODES ) {
            root.remove(collection, keys);
            return;
        }
        
        for ( String key : keys ) {
            findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .remove(collection, key);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<Key, Condition>> toRemove) {
        if ( collection != Collection.NODES) {
            return root.remove(collection, toRemove);
        }
        
        // map each owner to store to the specific removals it will handle
        Map<DocumentStore, Map<String, Map<Key, Condition>>> storesToRemovals = Maps.newHashMap();
        
        for ( Map.Entry<String, Map<Key, Condition>> entry : toRemove.entrySet()) {
            
            DocumentStore ownerStore = findOwnerStore(entry.getKey(), collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY);
            
            Map<String, Map<Key, Condition>> removals = storesToRemovals.get(ownerStore);
            if ( removals == null ) {
                removals = Maps.newHashMap();
                storesToRemovals.put(ownerStore, removals);
            }
            
            removals.put(entry.getKey(), entry.getValue());
        }
        
        int removals = 0;
        
        // process removals for each store
        for ( Map.Entry<DocumentStore, Map<String, Map<Key, Condition>>> entry : storesToRemovals.entrySet()  ) {
           removals += entry.getKey().remove(collection, entry.getValue());
        }
        
        return removals;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {

        if(updateOps.isEmpty()) {
            // The DocumentStore API is not fully clear about this case
            return false;
        }
        
        boolean created = true;
        
        for ( UpdateOp updateOp: updateOps ) {
            created &= findOwnerStore(updateOp, collection, OnFailure.FAIL_FAST)
                   .create(collection, Collections.singletonList(updateOp));
        }
        
        return created;
    }
    
    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        
        for ( String key : keys) {
            findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .update(collection, Collections.singletonList(key), updateOp);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        
        return findOwnerStore(update, collection, OnFailure.FAIL_FAST)
                .createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        
        return findOwnerStore(update, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .findAndUpdate(collection, update);
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().invalidateCache();
        }
        // TODO return aggregate stats
        return null;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        
        findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY).invalidateCache(collection, key);
    }
    
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        // TODO optimize
        return invalidateCache();
    }

    @Override
    public void dispose() {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().dispose();
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        return findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .getIfCached(collection, key);
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().setReadWriteMode(readWriteMode);
        }
    }

    @Override
    public CacheStats getCacheStats() {
        // TODO return aggregate stats
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        // TODO return aggregate metadata?
        return Collections.emptyMap();
    }
    
    @Override
    public void setDocumentCreationCustomiser(DocumentCreationCustomiser customiser) {
        // TODO - forward to mounts?
        throw new UnsupportedOperationException();
    }

    @Override
    public String toMapPath(String relativePath, String absolutePath) {
        for (DocumentStoreMount m : mounts) {
            if ( m.contains(absolutePath)) {
                return m.toMappedPath(relativePath);
            }
        }
        return relativePath;
    }

    /**
     * Helper class used to create <tt>MultiplexingDocumentStore</tt> instances
     * 
     * <p>It is required to set at least the {@link #root(DocumentStore) root document store} 
     * and one {@link #mount(String, DocumentStore) mount} before {@link #build() building}
     * the instance.</p>
     *
     */
    public static class Builder {

        public static final String[] MAPPED_PATHS = new String[]{
                // TODO, check.
                "^\\/jcr:system\\/rep:versionStore\\/",
                "^\\/jcr:system\\/rep:permissionStore\\/.*?\\/"
        };
        private DocumentStore root;
        private List<DocumentStoreMount> mounts = Lists.newArrayList();
        
        public Builder root(DocumentStore store) {
            
            root = checkNotNull(store); 
            
            return this;
        }
        
        public Builder mount(String path, String id, DocumentStore store) {
            
            // TODO - check for duplicate mounts?
            // TODO - check for duplicate mount IDs.
            
            checkNotNull(store);
            checkNotNull(path);
            if ( !Text.isDescendant("/", path)) {
                throw new IllegalArgumentException("Invalid mount path '" + path +"'");
            }
            
            
            mounts.add(new DocumentStoreMount(store, path, id, MAPPED_PATHS));
            
            return this;
        }
        
        public MultiplexingDocumentStore build() {
            
            Preconditions.checkArgument(root != null, "No %s instance mounted at '/'", DocumentStore.class.getSimpleName());
            
            Preconditions.checkArgument(mounts.size() > 0, 
                    "Expected at least 1 mounts but got %s.", mounts.size());
            
            return new MultiplexingDocumentStore(root, mounts); 
        }
    }
    
    /**
     * Private abstraction to simplify storing information about mounts
     */
    static class DocumentStoreMount implements Comparable<DocumentStoreMount> {
        private final DocumentStore store;
        private final String mountPath;
        private final String mountId;
        private Pattern[] mapPaths;
        private long id;

        /**
         * Create a definition of a document store mount mounting a store at a mountPath with a mountId and
         * set of paths which are mapped. Mapped paths are paths where multiple stores are present as children
         * of the same parent node. The path is encoded int he form mappedPaths[I]+mountId eg /jcr:system/rep:permissionStore/mm_1213121
         * In this example the path belongs to the mount id mm.
         * @param store
         * @param mountPath
         * @param mountId
         * @param mappedPathPatterns
         */
        public DocumentStoreMount(DocumentStore store, String mountPath, String mountId, String[] mappedPathPatterns) {
            this.store = store;
            this.mountPath = mountPath;
            this.mountId = mountId;
            mapPaths = new Pattern[mappedPathPatterns.length];
            for (int i = 0; i < mappedPathPatterns.length; i++) {
                mapPaths[i] = Pattern.compile(mappedPathPatterns[i]+mountId+"_");
            }
        }
        
        public DocumentStore getStore() {
            return store;
        }
        
        public String getMountPath() {
            return mountPath;
        }

        /**
         * Sort first by length, then if the length is equal by alpha.
         * Longest paths come first. Needs test coverage to make certain order is correct.
         * @param o
         * @return
         */
        @Override
        public int compareTo(DocumentStoreMount o) {
            int i = o.mountPath.length() - mountPath.length();
            if ( i == 0 ) {
                i = mountPath.compareTo(o.mountPath);
            }
            return i;
        }

        /**
         * If the mount contains the absolute path, returns true. Paths below the mount point
         * and paths mapped into the mount point return true.
         * @param absolutePath the absolute path
         * @return true if contains the absolute path.
         */
        public boolean contains(String absolutePath) {
            if (Text.isDescendantOrEqual(mountPath, absolutePath)) {
                return true;
            }
            // this is not correct.
            // some paths dont have a static prefix, we need to use a pattern.

            for (Pattern p : mapPaths) {
                return p.matcher(absolutePath).matches();
            }
            return false;
        }

        /**
         * Convert a relative path into a relative path mapped into this mount.
         * @param relativePath the relative path unmapped
         * @return the relative path mapped into this mount.
         */
        public String toMappedPath(String relativePath) {
            return mountId+"_"+relativePath;
        }

        @Override
        public String toString() {
            return "id:"+mountId+", path:"+mountPath;
        }

        public String getMountId() {
            return mountId;
        }
    }
    
    /**
     * Policy which indicates what action should be taken if a document's owning store can't be identified from the path
     * 
     * <p>Even if the DocumentNodeStore generally has a hierarchical usage pattern, some operations ( e.g. splitting documents ) 
     * make use of non-hierarchical identifiers.</p>
     *
     */
    private enum OnFailure {
        
        FAIL_FAST, CALL_FIND_FOR_MATCHING_KEY;
    }

    @Override
    public long determineServerTimeDifferenceMillis() {
        // TODO - the semantics are a bit unclear, what should we return here?
        // For now let's go with the root store
        return root.determineServerTimeDifferenceMillis();
    }

}
