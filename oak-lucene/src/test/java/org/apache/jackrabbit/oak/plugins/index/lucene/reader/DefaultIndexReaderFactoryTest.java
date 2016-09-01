/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.reader;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.NoopStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.MultiplexingIndexWriterTest.newDoc;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.*;

public class DefaultIndexReaderFactoryTest {
    private NodeState root = INITIAL_CONTENT;
    private NodeBuilder builder = EMPTY_NODE.builder();
    private IndexDefinition defn = new IndexDefinition(root, builder.getNodeState());
    private MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
            .mount("foo", "/libs", "/apps").build();

    @Test
    public void emptyDir() throws Exception{
        LuceneIndexReaderFactory factory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = factory.createReaders(defn, EMPTY_NODE,"/foo");
        assertTrue(readers.isEmpty());
    }

    @Test
    public void indexDir() throws Exception{
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, null, StatisticsProvider.NOOP);
        LuceneIndexWriter writer = factory.newInstance(defn, builder, true);

        writer.updateDocument("/content/en", newDoc("/content/en"));
        writer.close(0);

        LuceneIndexReaderFactory readerFactory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = readerFactory.createReaders(defn, builder.getNodeState(),"/foo");
        assertEquals(1, readers.size());

        LuceneIndexReader reader = readers.get(0);
        assertNotNull(reader.getReader());
        assertNull(reader.getSuggestDirectory());
        assertNull(reader.getLookup());

        assertEquals(1, reader.getReader().numDocs());

        final AtomicBoolean closed = new AtomicBoolean();
        reader.getReader().addReaderClosedListener(new IndexReader.ReaderClosedListener() {
            @Override
            public void onClose(IndexReader reader) {
                closed.set(true);
            }
        });

        reader.close();

        assertTrue(closed.get());
    }

    @Test
    public void suggesterDir() throws Exception{
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, null, StatisticsProvider.NOOP);
        enabledSuggestorForSomeProp();
        defn = new IndexDefinition(root, builder.getNodeState());
        LuceneIndexWriter writer = factory.newInstance(defn, builder, true);

        Document doc = newDoc("/content/en");
        doc.add(new StringField(FieldNames.SUGGEST, "test", null));
        writer.updateDocument("/content/en", doc);
        writer.close(0);

        LuceneIndexReaderFactory readerFactory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = readerFactory.createReaders(defn, builder.getNodeState(),"/foo");
        LuceneIndexReader reader = readers.get(0);
        assertNotNull(reader.getReader());
        assertNotNull(reader.getSuggestDirectory());
        assertNotNull(reader.getLookup());
    }

    @Test
    public void multipleReaders() throws Exception{
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, null, StatisticsProvider.NOOP);
        LuceneIndexWriter writer = factory.newInstance(defn, builder, true);

        writer.updateDocument("/content/en", newDoc("/content/en"));
        writer.updateDocument("/libs/config", newDoc("/libs/config"));
        writer.close(0);

        LuceneIndexReaderFactory readerFactory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = readerFactory.createReaders(defn, builder.getNodeState(),"/foo");
        assertEquals(2, readers.size());
    }

    @Test
    public void multipleReaders_SingleSuggester() throws Exception{
        LuceneIndexWriterFactory factory = new DefaultIndexWriterFactory(mip, null, StatisticsProvider.NOOP);
        enabledSuggestorForSomeProp();
        defn = new IndexDefinition(root, builder.getNodeState());
        LuceneIndexWriter writer = factory.newInstance(defn, builder, true);

        //Suggester field is only present for document in default mount
        Document doc = newDoc("/content/en");
        doc.add(new StringField(FieldNames.SUGGEST, "test", null));
        writer.updateDocument("/content/en", doc);

        writer.updateDocument("/libs/config", newDoc("/libs/config"));
        writer.close(0);

        LuceneIndexReaderFactory readerFactory = new DefaultIndexReaderFactory(mip, null);
        List<LuceneIndexReader> readers = readerFactory.createReaders(defn, builder.getNodeState(),"/foo");

        //Suggester should be present for all though it may be empty
        for (LuceneIndexReader reader : readers){
            assertNotNull(reader.getReader());
            assertNotNull(reader.getSuggestDirectory());
            assertNotNull(reader.getLookup());
        }
    }

    private void enabledSuggestorForSomeProp(){
        NodeBuilder prop = builder.child("indexRules").child("nt:base").child("properties").child("prop1");
        prop.setProperty("name", "foo");
        prop.setProperty(LuceneIndexConstants.PROP_USE_IN_SUGGEST, true);
    }
}