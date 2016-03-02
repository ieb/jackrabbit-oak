package org.apache.jackrabbit.oak.plugins.document.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mongodb.util.JSON;
import org.apache.commons.codec.binary.Base64;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.json.JsopReader;
import org.apache.jackrabbit.oak.commons.json.JsopTokenizer;
import org.apache.jackrabbit.oak.plugins.document.*;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.*;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.CHAR2OCTETRATIO;
import static org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.asBytes;

/**
 * Created by ieb on 15/02/2016.
 */
public class C8DocumentStore extends AbstractCachingDocumentStore {

    private static final Logger LOG = LoggerFactory.getLogger(C8DocumentStore.class);
    private final C8Client client;
    private static final String MODIFIED = "_modified";
    private static final String MODCOUNT = "_modCount";
    private static final String CMODCOUNT = "_collisionsModCount";
    private static final String ID = "_id";
    private static final String PARENTID = "_parentid";
    private static final String HASBINARY = NodeDocument.HAS_BINARY_FLAG;
    private static final String DELETEDONCE = NodeDocument.DELETED_ONCE;


    public C8DocumentStore(C8Client client, DocumentMK.Builder builder) {
        super(builder, ImmutableMap.<String, String>builder()
                .put("type", "cassndra")
                .put("db", "Cassandra")
                .put("version", "3.0")
                .build());
        this.client = client;
    }



    @Nonnull
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, String indexedProperty, long startValue, int limit) {
        // this method appears to have been introduced to address a lack of efficient query support in MongoDB and is quite MongoDB specific. It assumes that the
        // collection is ordered by a pre-determined collation, and that its efficient to query over multiple partitions. Even MongoDB in sharded mode may not be able to perform this
        // as each key within the range may be on any partition, and hence every query produced by this method may have to be executed on on shards.
        // there appear to be 3 use cases depending on the collection being used, but its hard ot tell as the API may be exposed outside the core Oak bundle.
        // Use case 1.
        // Find child nodes of 1 parent node.
        // parent = 5:a/b/c/d/e
        // from = 6:a/b/c/d/e/
        // to = 6:a/b/x/d/e0
        LOG.info("Query collection:{} fromKey:{} toKey:{} indexedProperty:{} startValue:{} limit:{} ",new Object[]{ collection, fromKey, toKey, indexedProperty, startValue, limit});
        List<T> results = new ArrayList<T>();
        boolean supported = false;
        String indexedField = client.mapIndexedFiled(collection, indexedProperty);
        if ( indexedProperty != null && indexedField == null) {
            LOG.warn("Query being performed on unindexed property, {} will filter in memory ", indexedProperty);
        }
        String parentPath = Utils.getParentIdFromLowerLimit(fromKey);
        ResultSet rs = null;

        if (collection == Collection.NODES) {
            if (parentPath != null ) {
                // this is a child range query,
                // If the path key  is already hashed this might not work as we cant get back to the parent path.
                if (indexedField == null) {
                    rs = client.getSession().execute(
                            client.getPreparedStatement(collection, "findbyparentidnoproperty", String.valueOf(limit))
                                    .bind(parentPath));
                } else {
                    rs = client.getSession().execute(
                            client.getPreparedStatement(collection, "findbyparentid", indexedField, String.valueOf(limit))
                                    .bind(parentPath, startValue));
                }
                supported = true;
            }
        }
        if (!supported){
            LOG.warn("Range query not supported, will select all and filter for {} to {} ", fromKey, toKey);
            if ( collection == Collection.NODES ) {
                LOG.error("Range Query must be eliminated, ++++ FULL TABLE SCAN BEING PERFORMED ON NODES ++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            }
            if (indexedField == null) {

                rs = client.getSession().execute(
                        client.getStatement(collection, "findallnoproperty"));
            } else {
                rs = client.getSession().execute(
                        client.getPreparedStatement(collection, "findall").bind(startValue));
            }
        }
        if (rs != null) {
            for (Row r : rs.all()) {
                T doc = fromRow(collection, r);
                if (!supported) {
                    // filter by fromKey toKey
                    String id = doc.getId();
                    if ( id.compareTo(fromKey) < 0 || id.compareTo(toKey) >= 0 ) {
                        continue;
                    }
                }
                if (indexedProperty != null && indexedField == null) {
                    // filter by a field that was not indexed.
                    long v = convertToLong(doc.get(indexedProperty));
                    if (v < startValue) {
                        continue;
                    }
                }
                results.add(doc);
            }
        }
        return results;
    }

    private long convertToLong(Object o) {
        if (o == null) {
            return 0;
        } else if ( o instanceof Long ) {
            return ((Long)o);
        } else if ( o instanceof Integer) {
            return ((Integer)o).longValue();
        } else if ( o instanceof Float) {
            return ((Float) o).longValue();
        } else if ( o instanceof Double) {
            return ((Double) o).longValue();
        } else if ( o instanceof Boolean) {
            return ((Boolean) o).booleanValue()?1:0;
        }
        return 0;
    }


    @Override
    public long determineServerTimeDifferenceMillis() {
        // TODO
        return 0;
    }

    // --------------------------------------------------------------------




    <T extends Document> void internalDelete(Collection<T> collection, String key) {
        LOG.info("internalDelete collection:{} key:{}  ",collection, key);
        BoundStatement delete = client.getBoundStatement(collection, "deleteone");
        client.getSession().executeAsync(delete.bind(key));
    }

    <T extends Document> void internalDelete(Collection<T> collection, List<String> keys) {
        LOG.info("internalDelete collection:{} key:{} ", collection, keys);
        for (List<String> sublist : Lists.partition(keys, 64)) {
            BoundStatement delete = client.getBoundStatement(collection, "deletemany");
            client.getSession().executeAsync(delete.bind(sublist));
        }
    }

    <T extends Document> int internalDelete(Collection<T> collection,
                                                    Map<String, Map<UpdateOp.Key, UpdateOp.Condition>> toRemove) {
        LOG.info("internalDelete collection:{} toRemove:{} ",collection, toRemove);
        String idfld = client.mapField(collection, ID);
        String modifiedfld = client.mapField(collection, MODIFIED);
        List<String> todelete = new ArrayList<String>();
        ResultSet rs = client.getSession().execute(client.getBoundStatement(collection, "findbyids").bind(new ArrayList<String>(toRemove.keySet())));
        for (Row row : rs ) {
            String id = row.getString(idfld);
            LOG.info("Found candidate to remove {} ", id);
            Map<UpdateOp.Key, UpdateOp.Condition> conditions = toRemove.get(id);
            for (Map.Entry<UpdateOp.Key, UpdateOp.Condition> c : conditions.entrySet()) {
                if (!c.getKey().getName().equals(NodeDocument.MODIFIED_IN_SECS)) {
                    throw new DocumentStoreException("Unsupported condition: " + c);
                }
                Long modified = row.get(modifiedfld, Long.class);
                if (c.getValue().type == UpdateOp.Condition.Type.EQUALS && c.getValue().value instanceof Long) {
                    if (modified != null && modified.longValue() == ((Long) c.getValue().value).longValue()) {
                        LOG.info("Added becuase modified equals {} ", id);
                        todelete.add(id);
                    } else {
                        LOG.info("modified not equals {} ", modified);

                    }
                } else if (c.getValue().type == UpdateOp.Condition.Type.EXISTS) {
                    if (modified != null) {
                        todelete.add(id);
                    } else {
                        LOG.info("modified doesnt exist for {} ", id);
                    }
                }
            }
        }
        LOG.info("To delete is {} ", todelete);
        rs = client.getSession().execute(client.getBoundStatement(collection, "deletemany").bind(todelete));
        return todelete.size();
    }

    /**
     * Perform the same update to all the documents identified by keys.
     * @param collection
     * @param keys
     * @param updateOp
     * @param <T>
     */
    <T extends Document> void internalUpdate(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        LOG.info("internalUpdate collection:{} keys:{} updateOp:{}",new Object[]{collection, keys, updateOp});
        List<UpdateOp> updates = new ArrayList<UpdateOp>();
        for(String key : keys) {
            updates.add(updateOp.shallowCopy(key));
        }
        internalCreate(collection, updates);
    }

    <T extends Document> T internalCreateOrUpdate(Collection<T> collection, UpdateOp update, boolean allowCreate, boolean checkConditions) {
        LOG.info("internalCreateOrUpdate collection:{} update:{} allowCreate:{} checkConditions:{} ",new Object[]{collection, update, allowCreate, checkConditions });
        Map<String, T> nodeDocs = new HashMap<String, T>();
        List<UpdateOp> updates = new ArrayList<UpdateOp>();
        updates.add(update);
        if ( internalCreate(collection, updates, true, false, nodeDocs) ) {
            return nodeDocs.get(update.getId());
        }
        return null;
    }

    <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates) {
        LOG.info("internalCreate collection:{} updates:{} ", new Object[]{collection, updates});
        return internalCreate(collection, updates, true, false, null);
    }

    <T extends Document> boolean internalCreate(Collection<T> collection, List<UpdateOp> updates, boolean allowCreate, boolean checkConditions, Map<String, T> nodeDocs) {
        LOG.info("internalCreate collection:{} updates:{} allowCreate:{} checkConditions:{} nodeDocs:{} ",new Object[]{collection, updates, allowCreate, checkConditions, nodeDocs });

        final Stopwatch watch = Stopwatch.createStarted();
        List<String> ids = new ArrayList<String>(updates.size());
        boolean success = true;
        LOG.info("Performing internal Create on {} with {} {} {} {}", new Object[]{collection, updates, allowCreate, checkConditions, nodeDocs});
        try {
            Set<String> keys = new HashSet<String>();
            for (UpdateOp update : updates) {
                keys.add(update.getId());
            }
            // get a copy of existing documents.
            Map<String, T> documents = readDocumentCached(collection, keys);
            List<Object[]> futures = new ArrayList<Object[]>();
            for (UpdateOp update : updates) {


                // each update contains a list of changes to the document.
                // it has been optimised for use with MongoDB and the update operation
                // contains operations performed inside the database.
                // SET: sets a value
                // MAX: sets the value to MAX
                // SET_MAP_ENTRY: sets and entry in a MAP
                // REMOVE_MAP_ENTRY.
                // CQL adds to the end of a map using set m = m + AdditionalValues.
                //     sets with set x = ?
                //     removes with delete map[key] from collection
                //     increments with set m = fmax(m,<newvalue>)
                // If the record exists, insert wont work.
                //


                String key = update.getId();
                T currentDocument = documents.get(key);
                T updatedDocument = collection.newDocument(this);
                if ( currentDocument != null ) {
                    currentDocument.deepCopy(updatedDocument);
                }
                // other updates
                List<Map.Entry<UpdateOp.Key, UpdateOp.Operation>> setUpdates = new ArrayList<Map.Entry<UpdateOp.Key, UpdateOp.Operation>>();
                List<Map.Entry<UpdateOp.Key, UpdateOp.Operation>> maxUpdates = new ArrayList<Map.Entry<UpdateOp.Key, UpdateOp.Operation>>();
                List<Map.Entry<UpdateOp.Key, UpdateOp.Operation>> incUpdates = new ArrayList<Map.Entry<UpdateOp.Key, UpdateOp.Operation>>();
                List<Map.Entry<UpdateOp.Key, UpdateOp.Operation>> unsetUpdates = new ArrayList<Map.Entry<UpdateOp.Key, UpdateOp.Operation>>();

                for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> entry : update.getChanges().entrySet()) {
                    UpdateOp.Key k = entry.getKey();
                    if (currentDocument != null && k.getName().equals(Document.ID)) {
                        // avoid exception "Mod on _id not allowed"
                        continue;
                    }
                    StringBuilder updateOperation = new StringBuilder();
                    UpdateOp.Operation op = entry.getValue();

                    switch (op.type) {
                        case SET:
                        case SET_MAP_ENTRY: {  //
                            setUpdates.add(entry);
                            break;
                        }
                        case MAX: {
                            maxUpdates.add(entry);
                            break;
                        }
                        case INCREMENT: {
                            incUpdates.add(entry);
                            break;
                        }
                        case REMOVE_MAP_ENTRY: {
                            unsetUpdates.add(entry);
                            break;
                        }
                    }
                }

                /**
                 * Nodes.
                 * Although Cassandra under the covers is a collumn database, it does not expose a true column database
                 * natively via CQL. For that you have to use Thrift, and Thrift usage has been discouraged for some time.
                 * The way of achiving this via CQL is to use a map type which gets translated into columns under the covers.
                 * The Nodes table is of the following form
                 *
                 * CREATE TABLE IF NOT EXISTS __DBNAME__.nodes (
                 *   id text PRIMARY KEY,
                 *   modified bigint,
                 *   deleted list<text>,
                 *   lastrev text,
                 *   modcount bigint,
                 *   children boolean,
                 *   revisions list<text>,  # value is the revision+key json encoded.
                 *   data map<text, text>);  # map of key and value where value is json for the revision, to update replace the whole value.
                 *   The key of the map is the name+revision of the property, hence everything is stored in 1 map.
                 *
                 * the lists can be appeded to, with each of the elements containing encoded data.
                 * Any field not in other fields is in map. If not versioned, the data is stored text encoded.
                 * If versioned the data is a json [] containing revisions.
                 * The map can be edited via CQL, but the values of the map have to be re-written, which requires the current
                 * version of the document.
                 *
                 * to update a map
                 * update nodes set data = data + ? ...
                 * to remove entries from a map
                 * update nodes set data = data - ?
                 * will remove the keys in the map ?
                 * I dont think add and remove can be performed in the same operation.
                 *
                 * To append to a list
                 * update nodes set revisions = revisions + ?
                 * To prepend to a list
                 * update nodes set revisions = ? + revisions
                 * To set a specific element
                 * update nodes set revisions[5] = ?
                 */

                // Perform setUpdates, MAX, Increment in 1 command,
                // update
                List<String> setTerms = new ArrayList<String>();
                List<String> insertTerms = new ArrayList<String>();
                List<String> insertPlaceholders =  new ArrayList<String>();
                List<Object> updateParams = new ArrayList<Object>();
                List<Object> insertParams = new ArrayList<Object>();
                Map<String, Object> dataMap = new HashMap<String, Object>();
                for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> e : setUpdates) {
                    String fld = client.mapField(collection, e.getKey().getName());
                    addRevision(updatedDocument, e.getKey(), e.getValue().value);
                    if ( fld != null) {
                        setTerms.add(fld + " = ? ");
                        insertTerms.add(fld);
                        insertPlaceholders.add("?");
                        updateParams.add(e.getValue().value);
                        insertParams.add(e.getValue().value);
                    } else {
                        dataMap.put(toDbKey(e.getKey()), toDBString(e.getValue().value));
                    }
                }
                for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> e : maxUpdates) {
                    String fld = client.mapField(collection, e.getKey().getName());
                    Object maxValue = fieldMax(currentDocument, e.getKey().getName(), e.getValue().value);
                    addRevision(updatedDocument, e.getKey(), maxValue);
                    if ( fld != null) {
                        setTerms.add(fld + " = ?");
                        insertTerms.add(fld);
                        insertPlaceholders.add("?");
                        updateParams.add(maxValue);
                        insertParams.add(maxValue);
                    } else {
                        dataMap.put(toDbKey(e.getKey()), toDBString(e.getValue().value));
                    }
                }
                for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> e : incUpdates) {
                    String fld = client.mapField(collection, e.getKey().getName());
                    Object incValue = fieldInc(currentDocument, e.getKey().getName(), e.getValue().value);
                    addRevision(updatedDocument, e.getKey(), incValue);
                    if ( fld != null) {
                        setTerms.add(fld + " =  ? ");
                        insertTerms.add(fld);
                        insertPlaceholders.add("?");
                        updateParams.add(incValue);
                        insertParams.add(incValue);
                    } else {
                        dataMap.put(toDbKey(e.getKey()), toDBString(e.getValue().value));
                    }
                }


                for (Map.Entry<UpdateOp.Key, UpdateOp.Operation> e : unsetUpdates) {
                    String fld = client.mapField(collection, e.getKey().getName());
                    addRevision(updatedDocument, e.getKey(), null);
                    if ( fld != null) {
                        setTerms.add(fld);
                        updateParams.add(null);
                    } else {
                        dataMap.put(toDbKey(e.getKey()), toDBString(null));
                    }
                }
                if ( dataMap.size() > 0) {
                    setTerms.add("data = ?");
                    insertTerms.add("data");
                    insertPlaceholders.add("?");
                    updateParams.add(dataMap);
                    insertParams.add(dataMap);
                }

                String idkey = client.mapField(collection, ID);
                if (idkey != null && !insertTerms.contains(idkey)) {
                    // if the ID key is missing from inset, add it
                    insertTerms.add(idkey);
                    insertPlaceholders.add("?");
                    insertParams.add(key);
                }
                String parentkey = client.mapField(collection, PARENTID);
                if (parentkey != null && !insertTerms.contains(parentkey)) {
                    // if the parent ID key is missing from insert, add it.
                    insertTerms.add(parentkey);
                    insertPlaceholders.add("?");
                    insertParams.add(getParentIdWithEdgeCases(key));
                }

                // neither parent Keys nor IDs get added to update statements, as they are immutable.
                // update will have the key already in the statement at the end
                updateParams.add(key);


                if (currentDocument != null) {
                    boolean perform = evaluateConditions(currentDocument, update.getConditions());
                    if (!perform) {
                        LOG.info("Conditions for update not satisfied, no update");
                    } else if ( setTerms.size() > 0) {
                        LOG.info("Performing update with {} {} ", setTerms, updateParams);

                        ResultSetFuture rsf = client.getSession().executeAsync(
                                client.getPreparedStatement(collection,
                                        "update",
                                        client.join(setTerms, ",")
                                ).bind(updateParams.toArray(new Object[updateParams.size()])));
                        // update the doc.

                        futures.add(new Object[]{updatedDocument, rsf});
                        // clear the local cache.
                        invalidateCache(collection, key);
                    } else {
                        LOG.warn("Not going to perform null update to document {} ", key);
                        nodeDocs.put(currentDocument.getId(), currentDocument);
                    }
                } else {
                    LOG.info("Performing insert with {} ",insertParams);
                    ResultSetFuture rsf = client.getSession().executeAsync(
                            client.getPreparedStatement(collection,
                                    "insert",
                                    client.join(insertTerms,","),
                                    client.join(insertPlaceholders,",")
                        ).bind(insertParams.toArray(new Object[insertParams.size()])));
                    // update the doc.
                    futures.add(new Object[]{updatedDocument, rsf});
                }

            }
            for (Object[] o : futures) {
                ResultSet rs = ((ResultSetFuture)o[1]).getUninterruptibly();
                if ( rs.wasApplied() && collection == Collection.NODES) {
                    if ( o[0] != null ) {
                        nodesCache.putIfAbsent((NodeDocument) o[0]);
                        if (nodeDocs != null) {
                            nodeDocs.put(((NodeDocument)o[0]).getId(), (T)o[0]);
                        }
                    }
                } else {
                    success = false;
                }
            }
            return success;
        } catch (DocumentStoreException ex) {
            LOG.error("Failed ", ex);
            return false;
        } finally {
            if ( ids.size() > 0) {
                stats.doneCreate(watch.elapsed(TimeUnit.NANOSECONDS), collection, ids, success);
            }
        }

    }

    private <T extends Document> boolean evaluateConditions(T currentDocument, Map<UpdateOp.Key, UpdateOp.Condition> conditions) {
        boolean perform = true;
        for (Map.Entry condition : conditions.entrySet()) {
            UpdateOp.Key k = (UpdateOp.Key) condition.getKey();
            UpdateOp.Condition c = (UpdateOp.Condition) condition.getValue();
            Object value = getValue(currentDocument, k.getName());
            switch (c.type) {
                case EQUALS:
                    if ( c.value == null ) {
                        perform = perform && c.value == value;
                    } else {
                        perform = perform && c.value.equals(value);
                    }
                    LOG.info("Checking Equals {}  {} {} ", new Object[]{k.getName(), c.value, value, perform});
                    break;
                case EXISTS:
                    perform = perform && value != null;
                    LOG.info("Checking Exists {} {} {} ", new Object[]{k.getName(), value, perform});
                    break;
                case NOTEQUALS:
                    if ( c.value == null ) {
                        perform = perform && c.value != value;
                    } else {
                        perform = perform && !c.value.equals(value);
                    }
                    LOG.info("Checking Not Equals {}  testvalue:{} docvalue:{}, {}  ", new Object[]{k.getName(), c.value, value, perform});
                    break;
            }
            LOG.info("Condition Key {} Value {} ",condition.getKey().getClass(),condition.getValue().getClass());
        }
        return perform;
    }

    private <T extends Document> Object getValue(T currentDocument, String name) {
        Object o = currentDocument.get(name);
        if ( o != null) {
            LOG.info("Value of {} is {} {} ", new Object[]{ name, o, o.getClass()});
        }
        if ( o instanceof NavigableMap) {
            return ((NavigableMap) o).firstEntry().getValue();
        }
        return o;
    }

    private String getParentIdWithEdgeCases(String key) {
        if (key.endsWith("/")) {
            // badly formed path with a non escaped trailing /, for the purposes of getting hte parent ID replace the / with a valid char and
            // get its parent ID.
            return Utils.getParentId(key.substring(0, key.length() - 1) + "_");
        }
        return Utils.getParentId(key);
    }

    private <T extends Document, V> V fieldInc(T currentDocument, String name, V value) {
        if (currentDocument == null) {
            return value;
        }
        V currentValue = (V) currentDocument.get(name);
        if (currentValue == null) {
            return value;
        }
        if (value instanceof Long && currentValue instanceof Long) {
            return (V)(Long)(((Long) value).longValue() + ((Long) currentValue).longValue());
        } else if (value instanceof Integer && currentValue instanceof Integer) {
            return (V)(Integer)(((Integer) value).intValue() + ((Integer) currentValue).intValue());
        } else if (value instanceof Double && currentValue instanceof Double ) {
            return (V)(Double)(((Double) value).doubleValue() + ((Double) currentValue).doubleValue());
        } else if (value instanceof Float && currentValue instanceof Float) {
            return (V)(Float)(((Float) value).floatValue() + ((Float) currentValue).floatValue());
        }
        throw new IllegalArgumentException("Cannot apply inc to value of types "+currentValue.getClass()+" "+value.getClass());
    }

    private <T extends Document, V>  V fieldMax(T currentDocument, String name, V value) {
        if (currentDocument == null) {
            return value;
        }
        V currentValue = (V) currentDocument.get(name);
        if (currentValue == null) {
            return value;
        }
        if (value instanceof Long && currentValue instanceof Long) {
            return (V)((Long) Math.max((Long)currentValue, (Long)value));
        } else if (value instanceof Integer && currentValue instanceof Integer) {
            return (V)((Integer) Math.max((Integer)currentValue, (Integer)value));
        } else if (value instanceof Double && currentValue instanceof Double ) {
            return (V)((Double) Math.max((Double)currentValue, (Double)value));
        } else if (value instanceof Float && currentValue instanceof Float) {
            return (V)((Float) Math.max((Float)currentValue, (Float)value));
        }
        throw new IllegalArgumentException("Cannot apply max to value of types "+currentValue.getClass()+" "+value.getClass());
    }


    @CheckForNull
    <T extends Document> T internalReadDocumentsUncached(Collection<T> collection, String key, NodeDocument cachedDoc) {
        LOG.info("internalReadDocumentsUncached collection:{} key:{} cachedDoc:{}  ",new Object[]{collection, key, cachedDoc });
        Map<String, T> d = internalReadDocumentsUncached(collection, ImmutableSet.of(key));
        return d.get(key);
    }

    <T extends Document> Map<String, T> internalReadDocumentsUncached(Collection<T> collection, Set<String> keys) {
        LOG.info("internalReadDocumentsUncached collection:{} keys:{}  ", new Object[]{collection, keys});
        Map<String, T> m = new HashMap<String, T>();
        ResultSet rs = client.getSession().execute(client.getBoundStatement(collection,"findbyids").bind(new ArrayList<String>(keys)));
        for (Row r : rs.all()) {
            T d = fromRow(collection, r);
            LOG.info("Loading {} {} ",d.getId(),d);
            m.put(d.getId(),d);
        }
        LOG.info("Found {} results ",m.size());
        return m;
    }

    private String toDbKey(UpdateOp.Key key) {
        if ( key.getRevision() == null ) {
            return key.getName();
        }
        return "/"+key.getName()+"/"+key.getRevision().toString();
    }

    private UpdateOp.Key fromDbKey(String dbKey) {
        if ( dbKey.charAt(0) == '/' ) {
            String[] parts = dbKey.substring(1).split("/", 2);
            return new UpdateOp.Key(parts[0], Revision.fromString(parts[1]));
        }
        return null;
    }


    public <T extends Document> T fromRow(Collection<T> collection, Row r) {
        LOG.info("Loading {} ", r);
        T doc = collection.newDocument(this);
        doc.put(ID, r.getString(client.mapField(collection, ID)));
        doc.put(MODIFIED, getLongFromRow(collection, MODIFIED, r, null));
        doc.put(MODCOUNT, getLongFromRow(collection, MODCOUNT, r, null));
        doc.put(CMODCOUNT, getLongFromRow(collection,  CMODCOUNT, r, null));
        if (getBoolFromRow(collection, HASBINARY, r) ) {
            doc.put(HASBINARY, NodeDocument.HAS_BINARY_VAL);
        }
        if (getBoolFromRow(collection, DELETEDONCE, r)) {
            doc.put(DELETEDONCE, Boolean.TRUE);
        }
        if ( r.getColumnDefinitions().contains("data")) {
            Map<String, String> data = r.getMap("data", String.class, String.class);
            // rebuild the document using Map of values keyed by name, containing an ordered map of values keyd by revision with
            // reverse order on the revision. The first entry in the treemap is the current value.
            for(Map.Entry<String, String> e : data.entrySet()) {
                UpdateOp.Key k = fromDbKey(e.getKey());
                if (k != null) {
                    addRevision(doc, k, fromDbString(e.getValue()));
                } else {
                    doc.put(e.getKey(), fromDbString(e.getValue()));
                }
            }
        }
        return doc;
    }

    private <T extends Document> void addRevision(T docdata, UpdateOp.Key k, Object o) {
        Object v = docdata.get(k.getName());
        if (k.getRevision() == null) {
            docdata.put(k.getName(), o);
        } else if ( v instanceof NavigableMap ) {
            ((NavigableMap)v).put(k.getRevision(), o);
        } else if ( v == null) {
            TreeMap<Revision, Object> m = new TreeMap<Revision, Object>(StableRevisionComparator.REVERSE);
            docdata.put(k.getName(), m);
            m.put(k.getRevision(), o);
        } else {
            // not versioned
            docdata.put(k.getName(), o);
        }
    }

    private Object toDBString(Object o) {
        try {
            if (o instanceof String) {
                return "s" + o;
            } else if (o instanceof Integer) {
                return "i" + o;
            } else if (o instanceof Long) {
                return "l" + o;
            } else if (o instanceof Boolean) {
                return "b" + o;
            } else if (o instanceof Double) {
                return "d" + o;
            } else if (o instanceof Float) {
                return "f" + o;
            } else if (o instanceof byte[]) {
                return "]" + Base64.encodeBase64String((byte[]) o);
            } else if (o instanceof String[]) {
                String[] sa = (String[]) o;
                StringBuilder sb = new StringBuilder();
                sb.append("S");
                if (sa.length > 0) {
                    sb.append(Base64.encodeBase64String(sa[0].getBytes("UTF-8")));
                }
                for (int i = 1; i < sa.length; i++) {
                    sb.append(",").append(Base64.encodeBase64String(sa[i].getBytes("UTF-8")));
                }
                return sb.toString();
            } else if (o instanceof int[]) {
                return "I" + Arrays.toString((int[]) o);
            } else if (o instanceof long[]) {
                return "L" + Arrays.toString((long[]) o);
            } else if (o instanceof float[]) {
                return "F" + Arrays.toString((float[]) o);
            } else if (o instanceof double[]) {
                return "D" + Arrays.toString((double[]) o);
            } else if (o instanceof boolean[]) {
                return "B" + Arrays.toString((boolean[]) o);
            } else {
                return JSON.serialize(o);
            }
        } catch ( UnsupportedEncodingException e) {
            LOG.error(e.getLocalizedMessage(),e);
        }
        throw new IllegalArgumentException("Unable to serialise "+o);
    }


    private Object fromDbString(String v) {
        if (v == null || v.length() == 0) {
            return null;
        }
        try {
            switch (v.charAt(0)) {
                case 's':
                    return v.substring(1);
                case 'i':
                    return Integer.parseInt(v.substring(1));
                case 'l':
                    return Long.parseLong(v.substring(1));
                case 'b':
                    return Boolean.parseBoolean(v.substring(1));
                case 'd':
                    return Double.parseDouble(v.substring(1));
                case 'f':
                    return Float.parseFloat(v.substring(1));
                case ']':
                    return Base64.decodeBase64(v.substring(1).getBytes("UTF-8"));
                case '{':
                    return JSON.parse(v);
                case 'S': {
                    String[] p = v.substring(1).split(",");
                    String[] va = new String[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Base64.encodeBase64String(p[i].getBytes("UTF-8"));
                    }
                    return va;
                }
                case 'F': {
                    String[] p = v.substring(1).split(",");
                    float[] va = new float[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Float.parseFloat(p[i]);
                    }
                    return va;
                }
                case 'I': {
                    String[] p = v.substring(1).split(",");
                    int[] va = new int[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Integer.parseInt(p[i]);
                    }
                    return va;
                }
                case 'L': {
                    String[] p = v.substring(1).split(",");
                    long[] va = new long[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Long.parseLong(p[i]);
                    }
                    return va;
                }
                case 'B': {
                    String[] p = v.substring(1).split(",");
                    boolean[] va = new boolean[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Boolean.parseBoolean(p[i]);
                    }
                    return va;
                }
                case 'D': {
                    String[] p = v.substring(1).split(",");
                    double[] va = new double[p.length];
                    for (int i = 0; i < p.length; i++) {
                        va[i] = Double.parseDouble(p[i]);
                    }
                    return va;
                }
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
        throw new IllegalArgumentException("Unable to deserialise from DB "+v);
    }

    private <T extends Document> boolean getBoolFromRow(Collection<T> collection, String name, Row r) {
        String k = client.mapField(collection, name);
        if (k != null && r.getColumnDefinitions().contains(k)) {
            if ( r.isNull(k)) {
                return false;
            }
            return r.getBool(k);
        }
        return false;
    }

    private <T extends Document> Object getLongFromRow(Collection<T> collection, String name, Row r, Object defaultValue) {
        String k = client.mapField(collection, name);
        if (k != null && r.getColumnDefinitions().contains(k)) {
            if ( r.isNull(k)) {
                return null;
            }
            return r.getLong(k);
        }
        return defaultValue;
    }

}
