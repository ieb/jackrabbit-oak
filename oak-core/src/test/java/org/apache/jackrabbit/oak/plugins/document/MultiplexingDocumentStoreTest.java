package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;

import com.google.common.collect.Maps;

public class MultiplexingDocumentStoreTest {
    
    private DocumentStore rootStore;
    private DocumentStore subStore;
    private MultiplexingDocumentStore store;

    /**
     * Prepares the local repository structure.
     * 
     * <p>A sub-repository is mounted at <tt>/1c</tt>, and 
     * the following nodes are already created: <tt>/1a, /1b, /1c, /1d, /1e</tt>.</p>
     * 
     * <p>All the nodes have a property named <em>prop</em> set with 
     * the value <em>val</em></p>
     *
     */
    @Before
    public void prepareMultiplexingStore() {
        rootStore = new MemoryDocumentStore();
        subStore = new MemoryDocumentStore();
        
        writeNode(rootStore, "0:/");
        writeNode(rootStore, "1:/1a");
        writeNode(rootStore, "1:/1b");
        writeNode(subStore, "1:/1c");
        writeNode(rootStore, "1:/1d");
        writeNode(rootStore, "1:/1e");
        
        store = new MultiplexingDocumentStore.Builder()
            .root(rootStore)
            .mount("/1c", "s1", subStore)
            .build();
    }
    
    @Test
    public void find_noResults() {
        
        assertNull(store.find(Collection.NODES, "1:/1z"));
    }
    
    @Test
    public void find_matching() {

        // match for root store
        assertNotNull(store.find(Collection.NODES, "1:/1a"));
        // match for sub store
        assertNotNull(store.find(Collection.NODES, "1:/1c"));
    }

    @Test
    public void find_wrongStore() {
        
        // insert a mismatched node in the sub store
        UpdateOp updateOp = new UpdateOp("1:/2a", true);
        updateOp.set("prop", "val");
        subStore.createOrUpdate(Collection.NODES, updateOp);
        
        assertNull(store.find(Collection.NODES, "1:/2a"));
    }
    
    @Test
    public void find_noResults_maxCacheParam() {
        
        assertNull(store.find(Collection.NODES, "1:/2a", Integer.MAX_VALUE));
    }
    
    @Test
    public void query_multipleStores() {
        
        List<NodeDocument> nodes = store.query(Collection.NODES, "1:/1a", "1:/1e", 10);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b", "1:/1c", "1:/1d"));
    }
    
    @Test
    public void query_multipleStores_obeysLimit() {
        
        List<NodeDocument> nodes = store.query(Collection.NODES, "1:/1a", "1:/1e", 1);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b"));
    }

    @Test
    public void query_atRoot() {
        
        List<NodeDocument> nodes = store.query(Collection.NODES, "0:/", "1:/1b", 1);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1a"));
    }
    
    @Test
    public void remove_matchingInRootStore() {
        
        store.remove(Collection.NODES, "1:/1a");
        
        assertThat(store.find(Collection.NODES, "1:/1a"), nullValue());
    }
    
    @Test
    public void remove_matchingInOtherStore() {
        
        store.remove(Collection.NODES, "1:/1c");
        
        assertThat(store.find(Collection.NODES, "1:/1c"), nullValue());
    }

    @Test
    public void remove_notMatching() {
        
        store.remove(Collection.NODES, "1:/1z");
    }

    @Test
    public void remove_multipleKeysInMultipleStores() {
        
        store.remove(Collection.NODES, Arrays.asList( "1:/1b", "1:/1c", "1:/1d" ));
        
        assertThat(store.query(Collection.NODES, "1:/1a", "1:/1e", 10).size(), equalTo(0));
    }
    
    @Test
    public void remove_withConditions() {

        UpdateOp.Key prop = new UpdateOp.Key("prop", null);
        UpdateOp.Condition equalsVal = Condition.newEqualsCondition("val");
        UpdateOp.Condition equalsOtherval = Condition.newEqualsCondition("otherVal");

        // /1b with prop = val ( -> REMOVED )
        // /1c with prop = val ( -> REMOVED )
        // /1d with prop = otherval ( -> NOT REMOVED )

        Map<String, Map<Key, Condition>> conditionedRemovals = Maps.newHashMap();
        conditionedRemovals.put("1:/1b", Collections.singletonMap(prop, equalsVal));
        conditionedRemovals.put("1:/1c", Collections.singletonMap(prop, equalsVal));
        conditionedRemovals.put("1:/1d", Collections.singletonMap(prop, equalsOtherval));
        
        assertThat(store.remove(Collection.NODES, conditionedRemovals), equalTo(2));
        
        List<NodeDocument> nodes = store.query(Collection.NODES, "1:/1a", "1:/1e", 10);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1d"));
    }
    
    @Test
    public void create() {
        
        UpdateOp rootOp = new UpdateOp("1:/1f", true);
        rootOp.set(Document.ID, "1:/1f");
        
        UpdateOp subOp = new UpdateOp("2:/1c/a", true);
        subOp.set(Document.ID, "2:/1c/a");
        
        boolean created = store.create(Collection.NODES, Arrays.asList(rootOp, subOp));

        assertTrue(created);
        
        assertNotNull(rootStore.find(Collection.NODES, "1:/1f"));
        assertNotNull(subStore.find(Collection.NODES, "2:/1c/a"));
    }

    @Test
    public void update() {
        
        UpdateOp update = new UpdateOp("", false);
        update.set("prop", "newVal");
        
        store.update(Collection.NODES, Arrays.asList("1:/1a", "1:/1c"), update);
        
        assertThat(store.find(Collection.NODES, "1:/1a").get("prop"), equalTo((Object) "newVal"));
        assertThat(store.find(Collection.NODES, "1:/1c").get("prop"), equalTo((Object) "newVal"));
        
    }
    
    @Test
    public void createOrUpdate() {
        
        UpdateOp update = new UpdateOp("1:/1e", false);
        update.set("prop", "newVal");
        
        UpdateOp create = new UpdateOp("1:/1f", true);
        create.set(Document.ID, "1:/1f");
        create.set("prop", "newVal");

        store.createOrUpdate(Collection.NODES, update);
        assertThat(store.find(Collection.NODES, "1:/1e").get("prop"), equalTo((Object) "newVal"));
        
        store.createOrUpdate(Collection.NODES, create);
        assertThat(store.find(Collection.NODES, "1:/1f").get("prop"), equalTo((Object) "newVal"));
    }
    
    @Test
    public void findAndUpdate_rootStore() {
        
        UpdateOp rootUpdate = new UpdateOp("1:/1a", false);
        rootUpdate.set("prop", "newVal");
        
        NodeDocument old = store.findAndUpdate(Collection.NODES, rootUpdate);
        assertThat(old.get("prop"), equalTo((Object) "val"));
        
        assertThat(store.find(Collection.NODES, "1:/1a").get("prop"), equalTo((Object) "newVal"));
    }

    @Test
    public void findAndUpdate_subStore() {
        
        UpdateOp rootUpdate = new UpdateOp("1:/1c", false);
        rootUpdate.set("prop", "newVal");
        
        NodeDocument old = store.findAndUpdate(Collection.NODES, rootUpdate);
        assertThat(old.get("prop"), equalTo((Object) "val"));
        
        assertThat(store.find(Collection.NODES, "1:/1c").get("prop"), equalTo((Object) "newVal"));
    }

    @Test
    public void testDoucumentStoreMountSortOrder() {
        List<MultiplexingDocumentStore.DocumentStoreMount> l = Lists.newArrayList();
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(rootStore, "/","", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/long","l", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/longer","l2", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/longerA","la", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/longerX","lx", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/longerStill","lS", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        l.add(new MultiplexingDocumentStore.DocumentStoreMount(subStore, "/longer/still", "ls", MultiplexingDocumentStore.Builder.MAPPED_PATHS));
        Collections.sort(l);
        assertEquals(l.get(0).getMountId(), "ls");
        assertEquals(l.get(1).getMountId(),"lS");
        assertEquals(l.get(2).getMountId(),"la");
        assertEquals(l.get(3).getMountId(),"lx");
        assertEquals(l.get(4).getMountId(),"l2");
        assertEquals(l.get(5).getMountId(),"l");
        assertEquals(l.get(6).getMountId(),"");
    }
    
    // TODO - mock/spy based tests forinvalidateCache, dispose, setReadWriteMode and getIfCached


    private void writeNode(DocumentStore root, String id) {
        UpdateOp updateOp = new UpdateOp(id, true);
        updateOp.set(Document.ID, id);
        updateOp.set("prop", "val");
        root.createOrUpdate(Collection.NODES, updateOp);
    }

    static class NodeListMatcher extends TypeSafeMatcher<List<NodeDocument>> {
        
        public static NodeListMatcher nodeListWithKeys(String first, String... others) {
            
            List<String> keys = new ArrayList<String>();
            keys.add(first);
            for ( String other: others) {
                keys.add(other);
            }
            
            return new NodeListMatcher(keys);
        }

        private final List<String> keys;
        
        private NodeListMatcher(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("A list of " + keys.size() + " node documents with the following keys: " + keys);
            
        }

        @Override
        public boolean matchesSafely(List<NodeDocument> toMatch) {
            if ( toMatch.size() != keys.size() ) {
                return false;
            }

            for ( int i = 0; i < toMatch.size() ; i++ ) {
                if ( !keys.get(i).equals(toMatch.get(i).getId())) {
                    return false;
                }
            }
            
            return true;
        }
        
    }


}
