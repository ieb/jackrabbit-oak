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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.security.auth.Subject;

import org.apache.jackrabbit.oak.api.AuthInfo;

public class Tenant implements Comparable<Tenant> {

    public static final Tenant SYSTEM_TENANT = new Tenant("__system__");
    private String tenantId;

    
    public Tenant(@Nonnull Subject subject) {
        Iterator<AuthInfo> i = checkNotNull(subject).getPublicCredentials(AuthInfo.class).iterator();
        if ( i.hasNext() ) {
            this.tenantId = TenantUtil.getTenantId(i.next());
        }
        if (this.tenantId == null) {
            this.tenantId = "__default__";
        }
    }
    
    private Tenant(@Nonnull String tenantId) {
        this.tenantId = checkNotNull(tenantId);
    }

    @Override
    public int hashCode() {
        return tenantId.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Tenant) {
            return tenantId.equals(((Tenant) obj).tenantId);
        }
        return false;
    }

    @Override
    public int compareTo(Tenant o) {
        return tenantId.compareToIgnoreCase(o.tenantId);
    }
    
    @Override
    public String toString() {
        return tenantId;
    }

}
