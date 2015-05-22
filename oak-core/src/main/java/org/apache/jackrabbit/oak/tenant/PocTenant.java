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
package org.apache.jackrabbit.oak.tenant;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PocTenant implements Tenant {

    private static final Logger log = LoggerFactory.getLogger(Tenant.class);
    private static final String TENANT_BASE = "/tenant/";
    private String tenantPath;
    private String tenantId;

    public PocTenant(String tenantId) {
        this.tenantId = tenantId;
        this.tenantPath = TENANT_BASE+tenantId;
    }

    @Override
    public boolean contains(String path) {
        if (path != null && path.indexOf(TENANT_BASE) == 0 ) {
            if (path.indexOf(tenantPath) == 0 && (path.length() == tenantPath.length() || path.charAt(tenantPath.length()) == '/')) {
                  log.info("Tenant {} contains {} ", tenantId, path);
                  return true;
             } else {
                  log.warn("Tenant {} does not contain {} ", tenantId, path);
                  return false;
             }
          }
          log.debug("Tenant {} Path is global {} ", tenantId, path);   
      return true;    
    }

    @Override
    public boolean containsChild(Tree tree, String name) {
        // getPath might be very slow, so it would be better if the tree knew more about itself.
        return contains(tree.getPath()+"/"+name);
    }

}
