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
package org.apache.jackrabbit.oak.core;

import java.util.Map;

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreInitialiser;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;

import com.google.common.collect.Maps;

public class DefaultNodeStoreProvider implements NodeStoreProvider {



    private NodeStore systemNodeStore;
    private NodeStoreInitialiser initialiser;
    private Map<Tenant, TenantNodeStore> nodeStores = Maps.newConcurrentMap();
    
    public DefaultNodeStoreProvider() {
        this(new MemoryNodeStore());
    }
    

    public DefaultNodeStoreProvider(NodeStore systemNodeStore) {
        this.systemNodeStore = systemNodeStore;
        nodeStores.put(Tenant.SYSTEM_TENANT, new TenantNodeStore(systemNodeStore, Tenant.SYSTEM_TENANT));
    }


    @Override
    public TenantNodeStore getTenantNodeStore(Tenant tenant) {
        if (!nodeStores.containsKey(tenant)) {
            System.err.println(this.getClass().getName()+" "+this+" Creating tenant node store for "+tenant+" from "+systemNodeStore);
            TenantNodeStore tns = new TenantNodeStore(systemNodeStore.cloneForTenant(tenant), tenant);
            nodeStores.put(tenant, tns);
        }
        return initialiser.initNodeStore(nodeStores.get(tenant));
    }

    @Override
    public TenantNodeStore getSystemNodeStore() {
        return getTenantNodeStore(Tenant.SYSTEM_TENANT);
    }
    

    @Override
    public void bindNodeStoreInitialiser(NodeStoreInitialiser initialiser) {
        this.initialiser = initialiser;
    }

}
