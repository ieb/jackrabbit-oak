/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for OAK-1768.
 */
public class LargeTreeOperationTest {

    private static final Tenant TEST_TENANT = new Tenant("testtenant");

    @Test
    public void removeLargeSubtree() throws CommitFailedException {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setUseSimpleRevision(true).getNodeStore();

        NodeBuilder builder = ns.getRoot(TEST_TENANT).builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < DocumentRootBuilder.UPDATE_LIMIT * 3; i++) {
            test.child("child-" + i);
        }
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        builder = ns.getRoot(TEST_TENANT).builder();
        Revision r1 = ns.newRevision(TEST_TENANT);
        // must trigger branch commit
        builder.getChildNode("test").remove();
        Revision r2 = ns.newRevision(TEST_TENANT);

        assertTrue("remove of large subtree must trigger branch commits",
                r2.getTimestamp() - r1.getTimestamp() > 1);

        ns.dispose();
    }

    @Test
    public void setLargeSubtreeOnRoot() throws CommitFailedException {
        setLargeSubtree(new String[0]);
    }

    @Test
    public void setLargeSubtree() throws CommitFailedException {
        setLargeSubtree("child");
    }

    private void setLargeSubtree(String... path) throws CommitFailedException {
        MemoryNodeStore memStore = new MemoryNodeStore();
        NodeBuilder builder = memStore.getRoot(TEST_TENANT).builder();
        NodeBuilder test = builder.child("test");
        for (int i = 0; i < DocumentRootBuilder.UPDATE_LIMIT * 3; i++) {
            test.child("child-" + i);
        }
        memStore.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        DocumentNodeStore ns = new DocumentMK.Builder()
                .setUseSimpleRevision(true).getNodeStore();

        builder = ns.getRoot(TEST_TENANT).builder();
        for (String name : path) {
            builder = builder.child(name);
        }
        Revision r1 = ns.newRevision(TEST_TENANT);
        // must trigger branch commit
        builder.setChildNode("test", memStore.getRoot(TEST_TENANT).getChildNode("test"));
        Revision r2 = ns.newRevision(TEST_TENANT);

        assertTrue("setting a large subtree must trigger branch commits",
                r2.getTimestamp() - r1.getTimestamp() > 1);

        ns.dispose();
    }
}
