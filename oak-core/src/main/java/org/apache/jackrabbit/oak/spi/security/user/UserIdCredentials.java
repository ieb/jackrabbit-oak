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
package org.apache.jackrabbit.oak.spi.security.user;

import javax.annotation.Nonnull;
import javax.jcr.Credentials;

import org.apache.jackrabbit.oak.security.tenant.TenantCredentials;

/**
 * Credentials implementation that only contains a {@code userId} but no password.
 * It can be used for {@link org.apache.jackrabbit.api.security.user.User#getCredentials()},
 * where the corresponding user doesn't have a password set.
 */
public final class UserIdCredentials implements Credentials, TenantCredentials {

    private static final long serialVersionUID = -3568487785581562804L;
    private final String userId;
    private String tenantId;

    public UserIdCredentials(@Nonnull String userId, @Nonnull String tenantId) {
        this.userId = userId;
        this.tenantId = tenantId;
    }

    @Nonnull
    public String getUserId() {
        return userId;
    }

    @Override
    public String getTenantId() {
        return tenantId;
    }

}