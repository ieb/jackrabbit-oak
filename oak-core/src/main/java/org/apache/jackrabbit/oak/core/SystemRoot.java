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
package org.apache.jackrabbit.oak.core;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authentication.LoginContext;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.TenantNodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;

/**
 *  Internal extension of the {@link MutableRoot} to be used
 *  when an usage of the system internal subject is needed.
 */
public class SystemRoot extends MutableRoot {


    private static final LoginContext LOGIN_CONTEXT = new LoginContext() {
        @Override
        public Subject getSubject() {
            return SystemSubject.INSTANCE;
        }
        @Override
        public void login() {
        }
        @Override
        public void logout() {
        }
    };

    private SystemRoot(@Nonnull NodeStore store, @Nonnull CommitHook hook,
                       @Nonnull String workspaceName, @Nonnull SecurityProvider securityProvider,
                       @Nonnull QueryEngineSettings queryEngineSettings,
                       @Nonnull QueryIndexProvider indexProvider,
                       @Nonnull ContentSessionImpl session) {
        super(store, hook, workspaceName, SystemSubject.INSTANCE,
                securityProvider, queryEngineSettings, indexProvider, session);
    }

    public SystemRoot(@Nonnull final NodeStore store, @Nonnull final CommitHook hook,
                      @Nonnull final String workspaceName, @Nonnull final SecurityProvider securityProvider,
                      @Nonnull final QueryEngineSettings queryEngineSettings,
                      @Nonnull final QueryIndexProvider indexProvider) {
        this(store, hook, workspaceName, securityProvider, queryEngineSettings, indexProvider,
                new ContentSessionImpl(
                        LOGIN_CONTEXT, securityProvider, workspaceName,
                        new SystemTenantNodeStore(store), hook, queryEngineSettings, indexProvider) {
                    @Nonnull
                    @Override
                    public Root getLatestRoot() {
                        return new SystemRoot(
                                store, hook, workspaceName, securityProvider,
                                queryEngineSettings,
                                indexProvider, this);
                    }
                });
    }
    
    public static class SystemTenantNodeStore implements TenantNodeStore {

        private NodeStore store;
        private List<Registration> regs = new ArrayList<Registration>();

        public SystemTenantNodeStore(NodeStore store) {
            this.store = store;
            System.err.println("Basing system node store on "+store);
        }

        @Override
        public NodeStore getNodeStore() {
            return store;
        }

        @Override
        public Tenant getTenant() {
            return Tenant.SYSTEM_TENANT;
        }

        @Override
        public void close() {
            new CompositeRegistration(regs).unregister();
        }

        @Override
        public void deregisterOnClose(List<Registration> regs) {
            System.err.println("System store registrations "+regs);
            this.regs.addAll(regs);
        }

    }

}
