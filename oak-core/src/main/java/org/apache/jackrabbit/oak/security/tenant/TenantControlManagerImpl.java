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

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.tenant.TenantControlManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TenantControlManagerImpl implements TenantControlManager {

    private static final String TENANT_BASE = "/tenant/";
    private static final String TENANT_BASE_OAK = "/tenant/";


    private static final Logger log = LoggerFactory.getLogger(TenantControlManagerImpl.class);


    private String tenantId;
    private String tenantPath;
    private String tenantPathOak;

    public TenantControlManagerImpl(String tenantId) {
        this.tenantId = tenantId;
        this.tenantPath = TENANT_BASE+tenantId;
        this.tenantPathOak = TENANT_BASE_OAK+tenantId;
    }

    @Override
    public boolean canAccess(Tree tree) {
        return (tree == null) || canAccess(tree.getPath());
    }

    @Override
    public boolean canAccess(String absPath) {
        return canAccess(absPath, TENANT_BASE, tenantPath);
    }
    
    @Override
    public boolean canAccessOak(String oakPath) {
        if  (canAccess(oakPath, TENANT_BASE_OAK, tenantPathOak)) {
            log.info("Granted tenant {} access to Oak: {} ",tenantId,oakPath);
            return true;
        }
        log.info("Denied tenant {} access to Oak: {} ",tenantId,oakPath);
        return false;
    }

    
    private boolean canAccess(String absPath, String tenantBase, String tenantTestPath) {
        // this code is only PoC. All tenant data appears under /tenant/<tenantid>
        // include the case where the /tenant/<tenantid>othertenant should be rejected.
        if (absPath != null && absPath.indexOf(tenantBase) == 0 ) {
            if (absPath.indexOf(tenantTestPath) == 0 && (absPath.length() == tenantTestPath.length() || absPath.charAt(tenantTestPath.length()) == '/')) {
                log.info("Granted tenant {} access to {} ",tenantId,absPath);
                return true;
            } else {
                log.warn("Denied tenant {} access to {} ",tenantId,absPath);
                return false;
            }
        }
        return true;
    }


}
