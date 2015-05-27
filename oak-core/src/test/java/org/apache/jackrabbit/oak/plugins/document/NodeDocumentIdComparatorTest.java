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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.tenant.Tenant;
import org.apache.jackrabbit.oak.spi.tenant.TenantPath;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.apache.jackrabbit.oak.plugins.document.PathComparator}.
 */
public class NodeDocumentIdComparatorTest {

    private static final Tenant TEST_TENANT = new Tenant("testtenant");

    @Test
    public void sort() {
        List<String> paths = new ArrayList<String>();
        paths.add(Utils.getIdFromPath(new TenantPath(TEST_TENANT, "/foo")));
        paths.add(Utils.getIdFromPath(new TenantPath(TEST_TENANT, "/foo/bar")));
        paths.add(Utils.getIdFromPath(new TenantPath(TEST_TENANT, "/bar/qux")));
        paths.add(Utils.getIdFromPath(new TenantPath(TEST_TENANT, "/")));
        paths.add(Utils.getIdFromPath(new TenantPath(TEST_TENANT, "/bar")));

        Collections.sort(paths, NodeDocumentIdComparator.INSTANCE);

        List<String> expected = Lists.newArrayList(
                "2:/bar/qux", "2:/foo/bar", "1:/bar", "1:/foo", "0:/");

        assertEquals(expected, paths);
    }
}
