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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.tree.impl.AbstractTree;
import org.apache.jackrabbit.oak.plugins.tree.impl.ImmutableTree;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.util.Text;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to evaluate permissions.
 */
public final class PermissionUtil implements PermissionConstants {

    private PermissionUtil() {}

    @CheckForNull
    public static String getParentPathOrNull(@Nonnull final String path) {
        if (path.length() <= 1) {
            return null;
        } else {
            int idx = path.lastIndexOf('/');
            if (idx == 0) {
                return "/";
            } else {
                return path.substring(0, idx);
            }
        }
    }

    /**
     * Get the name of the entry using the a tree object based on the absolute path the tree represents.
     * The tree is used to get a nodeBuilder which will use the store to determine the entry name. Where the
     * store is a multiplexed store, it will use entry names that identify the store in which the entry should be stored.
     * @param absolutePath absolute path the entry is for.
     * @param tree current tree object.
     * @return the path
     */
    @Nonnull
    public static String getEntryName(@Nullable String absolutePath, @Nonnull Tree tree) {
        String path = Strings.nullToEmpty(absolutePath);
        LoggerFactory.getLogger(PermissionUtil.class).info("ToMapPath {} ", absolutePath);
        if (tree instanceof AbstractTree) {
            return getEntryName (absolutePath, ((AbstractTree) tree).getNodeState().builder());
        }
        return String.valueOf(path.hashCode());
    }

    @Nonnull
    public static String getEntryName(@Nullable String absolutePath, @Nonnull NodeBuilder nodeBuilder) {
        String path = Strings.nullToEmpty(absolutePath);
        LoggerFactory.getLogger(PermissionUtil.class).info("ToMapPath {} ", absolutePath);

        return nodeBuilder.toMapPath(String.valueOf(path.hashCode()), absolutePath);
    }

    public static boolean checkACLPath(@Nonnull NodeBuilder node, @Nonnull String path) {
        PropertyState property = node.getProperty(REP_ACCESS_CONTROLLED_PATH);
        return property != null && path.equals(property.getValue(Type.STRING));
    }

    public static boolean checkACLPath(@Nonnull Tree node, @Nonnull String path) {
        PropertyState property = node.getProperty(REP_ACCESS_CONTROLLED_PATH);
        return property != null && path.equals(property.getValue(Type.STRING));
    }

    @Nonnull
    public static Tree getPermissionsRoot(@Nonnull Root root, @Nonnull String workspaceName) {
        return root.getTree(PERMISSIONS_STORE_PATH + '/' + workspaceName);
    }

    @Nonnull
    public static Tree getPrincipalRoot(@Nonnull Tree permissionsTree, @Nonnull String principalName) {
        return permissionsTree.getChild(Text.escapeIllegalJcrChars(principalName));
    }

    public static boolean isAdminOrSystem(@Nonnull Set<Principal> principals, @Nonnull ConfigurationParameters config) {
        if (principals.contains(SystemPrincipal.INSTANCE)) {
            return true;
        } else {
            Set<String> adminNames = config.getConfigValue(PARAM_ADMINISTRATIVE_PRINCIPALS, Collections.EMPTY_SET);
            for (Principal principal : principals) {
                if (principal instanceof AdminPrincipal || adminNames.contains(principal.getName())) {
                    return true;
                }
            }
            return false;
        }
    }

    @CheckForNull
    public static String getPath(@Nullable Tree parentBefore, @Nullable Tree parentAfter) {
        String path = null;
        if (parentBefore != null) {
            path = parentBefore.getPath();
        } else if (parentAfter != null) {
            path = parentAfter.getPath();
        }
        return path;
    }

    public static Tree getImmutableTree(@Nullable Tree tree, @Nonnull Root immutableRoot) {
        if (tree instanceof ImmutableTree) {
            return tree;
        } else {
            return (tree == null) ? null : immutableRoot.getTree(tree.getPath());
        }
    }
}