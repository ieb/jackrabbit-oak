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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.apache.jackrabbit.oak.spi.security.Context;

/**
 * *If* the layout of tenant content was defined in the repository, this would implement more than it does.
 * For the moment, it will return false for most things, and the TenantControlManager will use patterns to define 
 * What content trees contain tenant specific data.
 */
public class TenantContext implements Context {
    
    
    private static final Context INSTANCE = new TenantContext();

    private TenantContext() {
    }

    static Context getInstance() {
        return INSTANCE;
    }

    
    @Override
    public boolean definesProperty(Tree parent, PropertyState property) {
        return false;
    }

    @Override
    public boolean definesContextRoot(Tree tree) {
        return false;
    }

    @Override
    public boolean definesTree(Tree tree) {
        return false;
    }

    @Override
    public boolean definesLocation(TreeLocation location) {
        return false;
    }

}
