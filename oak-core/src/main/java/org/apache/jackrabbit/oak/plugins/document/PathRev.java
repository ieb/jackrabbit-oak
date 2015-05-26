/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.apache.jackrabbit.oak.spi.tenant.TenantPath;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache key implementation, which is a combination of a path string and a
 * revision.
 */
public final class PathRev implements CacheValue {

    private final TenantPath tenantPath;

    private final Revision revision;

    public PathRev(@Nonnull TenantPath tenantPath, @Nonnull Revision revision) {
        this.tenantPath = checkNotNull(tenantPath);
        this.revision = checkNotNull(revision);
    }

    @Override
    public int getMemory() {
        return 24                           // shallow size
                + tenantPath.getMemory()    // path
                + 32;                      // revision
    }

    //----------------------------< Object >------------------------------------


    @Override
    public int hashCode() {
        return (tenantPath.hashCode() ^ revision.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof PathRev) {
            PathRev other = (PathRev) obj;
            return revision.equals(other.revision) && tenantPath.equals(other.tenantPath);
        }
        return false;
    }

    @Override
    public String toString() {
        return tenantPath.toPathString() + "@" + revision;
    }

    public String asString() {
        return toString();
    }

    public static PathRev fromString(String s) {
        TenantPath t = TenantPath.fromString(s);
        return new PathRev(t, Revision.fromString(Tenant.getRevisionPart(s)));
    }

    public int compareTo(PathRev b) {
        if (this == b) {
            return 0;
        }
        int compare = tenantPath.compareTo(b.tenantPath);
        if (compare == 0) {
              compare = revision.compareTo(b.revision);
        }
        return compare;
    }
    
}
