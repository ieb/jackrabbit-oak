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

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.core.Tenant;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreInitialiser;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

public class TenantNodeStore  {

    
    private NodeStore nodeStore;
    private Tenant tenant;
    private List<Registration> regs = new ArrayList<Registration>();
    private boolean initialised = false;
    
    public TenantNodeStore(NodeStore nodeStore, Tenant tenant) {
        this.nodeStore = nodeStore;
        this.tenant = tenant;
    }

    public NodeStore getNodeStore() {
        return nodeStore;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void close() {
        new CompositeRegistration(regs).unregister();
    }

    public void deregisterOnClose(List<Registration> regs) {
        this.regs.addAll(regs);

    }


    public boolean isInitialised() {
        return this.initialised;
    }

    public void markInitialised() {
        this.initialised = true;        
    }
}
