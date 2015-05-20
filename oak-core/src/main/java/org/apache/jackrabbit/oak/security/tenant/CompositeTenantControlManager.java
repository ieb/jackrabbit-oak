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

import java.util.List;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.tenant.TenantControlManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeTenantControlManager implements TenantControlManager {

    private static final Logger log = LoggerFactory.getLogger(TenantControlManagerImpl.class);

    private List<TenantControlManager> mgrs;

    public CompositeTenantControlManager(List<TenantControlManager> mgrs) {
        this.mgrs = mgrs;
    }

    @Override
    public boolean canAccess(Tree tree) {
        for(TenantControlManager tcm : mgrs) {
            if (tcm.canAccess(tree)) {
                log.info("TCM {} granted ", tcm);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean canAccess(String absPath) {
        for(TenantControlManager tcm : mgrs) {
            if (tcm.canAccess(absPath)) {
                log.info("TCM {} granted ", tcm);
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean canAccessOak(String oakPath) {
        for(TenantControlManager tcm : mgrs) {
            if (tcm.canAccessOak(oakPath)) {
                log.info("TCM {} granted ", tcm);
                return true;
            }
        }
        return false;
    }


}
