/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The SF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.core.Tenant;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreInitialiser;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.state.TenantNodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

import com.google.common.collect.Maps;

public class MemoryNodeStoreProvider implements NodeStoreProvider {



    private NodeStore systemNodeStore;
    private NodeStoreInitialiser initialiser;
    private TenantNodeStore systemTenantNodeStore;
    private boolean systemStoreInitialised = false;
    private Map<Tenant, TenantNodeStore> nodeStores = Maps.newConcurrentMap();
    
    public MemoryNodeStoreProvider() {
        this(new MemoryNodeStore());
    }
    

    public MemoryNodeStoreProvider(NodeStore systemNodeStore) {
        this.systemNodeStore = systemNodeStore;
        this.systemTenantNodeStore = new MemoryTenantNodeStore(Tenant.SYSTEM_TENANT, systemNodeStore); 
    }


    @Override
    public TenantNodeStore getTenantNodeStore(Tenant tenant) {
        if (!nodeStores.containsKey(tenant)) {
            // fork the node state when the tenant is first accessed.
            TenantNodeStore tns = new MemoryTenantNodeStore(tenant, systemNodeStore.getRoot().builder());
            // this assumes that the new node store needs initialising.
            initialiser.initNodeStore(tns);
            nodeStores.put(tenant, tns);
        }
        return nodeStores.get(tenant);
    }

    @Override
    public TenantNodeStore getSystemNodeStore() {
        if (!systemStoreInitialised) {
            initialiser.initNodeStore(systemTenantNodeStore);
            systemStoreInitialised = true;
        }
        return systemTenantNodeStore;
    }
    

    @Override
    public void bindNodeStoreInitialiser(NodeStoreInitialiser initialiser) {
        this.initialiser = initialiser;
    }

    private static class MemoryTenantNodeStore implements TenantNodeStore {

        private NodeStore memoryNodeStore;
        private Tenant tenant;
        private List<Registration> regs = new ArrayList<Registration>();

        public MemoryTenantNodeStore(Tenant tenant, NodeBuilder builder) {
            this.tenant = tenant;
            NodeState base = ModifiedNodeState.squeeze(builder.getNodeState());
            memoryNodeStore = new MemoryNodeStore(base);
            System.err.println("Creating memory node store for tenant "+tenant+" "+memoryNodeStore);
        }

        public MemoryTenantNodeStore(Tenant tenant, NodeStore nodeStore) {
            this.tenant = tenant;
            memoryNodeStore = nodeStore;
        }

        @Override
        public NodeStore getNodeStore() {
            return memoryNodeStore;
        }

        @Override
        public Tenant getTenant() {
            return tenant;
        }

        @Override
        public void close() {
            new CompositeRegistration(regs).unregister();
        }

        @Override
        public void deregisterOnClose(List<Registration> regs) {
            this.regs.addAll(regs);

        }

    }

}
