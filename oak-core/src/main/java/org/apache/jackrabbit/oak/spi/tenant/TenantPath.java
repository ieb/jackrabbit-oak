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
package org.apache.jackrabbit.oak.spi.tenant;

import org.apache.jackrabbit.oak.commons.PathUtils;

public class TenantPath {

    public static final TenantPath SYSTEM_ROOT = new TenantPath(Tenant.SYSTEM_TENANT, "/");
    public static final TenantPath EMPTY_PATH = new TenantPath(new Tenant(""), "--");
    private Tenant tenant;
    private String path;

    public TenantPath(Tenant tenant, String path) {
        this.tenant = tenant;
        this.path = path;
    }

    public String getPath() {
        return path;
    }
    
    
    public Tenant getTenant() {
        return tenant;
    }

    public int getMemory() {
        return 40+path.length()*2+tenant.getMemory();
    }

    public String toPathString() {
        return null;
    }

    public static TenantPath fromString(String s) {
        Tenant t = Tenant.fromString(s);
        return new TenantPath(t, Tenant.getPathPart(s));
    }

    public int compareTo(TenantPath tenantPath) {
        int cmp = tenant.compareTo(tenant);
        if (cmp == 0) {
            return path.compareTo(tenantPath.path);
        }
        return cmp;
    }

    public TenantPath getChild(String name) {
        return new TenantPath(tenant, PathUtils.concat(path, name));
    }

}
