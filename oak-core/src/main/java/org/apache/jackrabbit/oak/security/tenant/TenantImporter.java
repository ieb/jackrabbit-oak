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
package org.apache.jackrabbit.oak.security.tenant;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.slf4j.LoggerFactory;

public class TenantImporter implements ProtectedItemImporter {

    @Override
    public boolean init(Session session, Root root, NamePathMapper namePathMapper,
            boolean isWorkspaceImport, int uuidBehavior, ReferenceChangeTracker referenceTracker,
            SecurityProvider securityProvider) {
        LoggerFactory.getLogger(this.getClass()).info("Init");
        return true;
    }

    @Override
    public void processReferences() throws RepositoryException {
        LoggerFactory.getLogger(this.getClass()).info("processReferences");

    }

}
