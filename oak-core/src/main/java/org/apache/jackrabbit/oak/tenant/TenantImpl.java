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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;

/**
 * Tenant Implemenmtation using file containing paths, loaded from the file system when the class is loaded by the classloader.
 * If the file cant be read, then all access to the repository will be denied except for the super user.
 */
public class TenantImpl implements Tenant {

    private static final Logger log = LoggerFactory.getLogger(TenantImpl.class);
    private static boolean loadedParentPaths;
    private static final Set<String> tenantPathParents = loadTenantParentPaths();
    private String tenantId;

    public static Set<String> loadTenantParentPaths() {
        loadedParentPaths = false;
        // FIXME: replace with a command line option.
        File f = new File("tenant-parent-paths.txt");
        ImmutableSet.Builder b = ImmutableSet.builder();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(f));
            String line = br.readLine();
            while(line!= null) {
                b.add(line.trim());
                line = br.readLine();
            }
            loadedParentPaths = true;
            return b.build();
        } catch (Exception e) {
            log.error("Unable to load tenant parent paths. All non Super User access to the " +
                    "repository will be denied until the file "+f.getAbsolutePath()+" is present and correct");
            // tempted to do System.exit() here ?
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    log.debug(e.getMessage(),e);
                }
            }
        }
        return ImmutableSet.of();
    }

    public TenantImpl(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public boolean contains(String path) {
        if (!loadedParentPaths) {
            // if no parent paths have been loaded, deny all access to everything in the repository.
            // this should help prevent misconfiguration.
            return false;
        }
        // this is not very efficient. A tree would be better if there are lots of tenant paths.
        if (path != null) {
            // subtree match on a parent, therefore some other tenant.
            for (String tp : tenantPathParents) {
                if (path.indexOf(tp) == 0) {
                    // match a parent, to be inside the tenant must also match with the tenantId, otherwise
                    // its outside the parent.
                    return path.indexOf(tp + tenantId) == 0;
                }
            }
        }
        // must be outside the tenant paths.
        return true;
    }

    @Override
    public boolean containsChild(Tree tree, String name) {
        // getPath might be very slow, so it would be better if the tree knew more about itself.
        return contains(tree.getPath()+"/"+name);
    }

    public static String getTenantId(String path) {
        if ( !loadedParentPaths) {
            throw new IllegalStateException("No Tenant Parent paths have been loaded. Repository is disabled to all non Super User sessions");
        }
        if ( path != null) {
            for (String tp : tenantPathParents) {
                if (path.indexOf(tp) == 0) {
                    String t = path.substring(tp.length());
                    int i = t.indexOf('/');
                    if (i > 0 ) {
                        t = t.substring(0,i);
                    }
                    return t;
                }
            }
        }
        return null;

    }

}
