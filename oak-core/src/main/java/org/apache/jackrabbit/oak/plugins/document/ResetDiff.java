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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.apache.jackrabbit.oak.spi.tenant.TenantPath;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;

/**
 * Implementation of a node state diff, which translates a diff into reset
 * operations on a branch.
 */
class ResetDiff implements NodeStateDiff {

    private final Revision revision;
    private final TenantPath tenantPath;
    private final Map<TenantPath, UpdateOp> operations;
    private UpdateOp update;

    ResetDiff(@Nonnull Revision revision,
              @Nonnull Tenant tenant,
              @Nonnull Map<TenantPath, UpdateOp> operations2) {
        this(revision, new TenantPath(checkNotNull(tenant), "/"), operations2);
    }

    private ResetDiff(@Nonnull Revision revision,
                      @Nonnull TenantPath tenantPath,
                      @Nonnull Map<TenantPath, UpdateOp> operations) {
        this.revision = checkNotNull(revision);
        this.tenantPath = checkNotNull(tenantPath);
        this.operations = checkNotNull(operations);
    }

    @Override
    public boolean propertyAdded(PropertyState after) {
        getUpdateOp().removeMapEntry(after.getName(), revision);
        return true;
    }

    @Override
    public boolean propertyChanged(PropertyState before, PropertyState after) {
        getUpdateOp().removeMapEntry(after.getName(), revision);
        return true;
    }

    @Override
    public boolean propertyDeleted(PropertyState before) {
        getUpdateOp().removeMapEntry(before.getName(), revision);
        return true;
    }

    @Override
    public boolean childNodeAdded(String name, NodeState after) {
        TenantPath p = tenantPath.getChild(name);
        ResetDiff diff = new ResetDiff(revision, p, operations);
        UpdateOp op = diff.getUpdateOp();
        NodeDocument.removeDeleted(op, revision);
        return after.compareAgainstBaseState(EMPTY_NODE, diff);
    }

    @Override
    public boolean childNodeChanged(String name,
                                    NodeState before,
                                    NodeState after) {
        TenantPath p = tenantPath.getChild(name);
        return after.compareAgainstBaseState(before,
                new ResetDiff(revision, p, operations));
    }

    @Override
    public boolean childNodeDeleted(String name, NodeState before) {
        TenantPath p = tenantPath.getChild(name);
        ResetDiff diff = new ResetDiff(revision, p, operations);
        NodeDocument.removeDeleted(diff.getUpdateOp(), revision);
        return MISSING_NODE.compareAgainstBaseState(before, diff);
    }

    Map<TenantPath, UpdateOp> getOperations() {
        return operations;
    }

    private UpdateOp getUpdateOp() {
        if (update == null) {
            update = operations.get(tenantPath);
            if (update == null) {
                String id = Utils.getIdFromPath(tenantPath);
                update = new UpdateOp(id, false);
                operations.put(tenantPath, update);
            }
            NodeDocument.removeRevision(update, revision);
            NodeDocument.removeCommitRoot(update, revision);
        }
        return update;
    }
}
