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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.AuthInfo;
import org.apache.jackrabbit.oak.spi.tenant.TenantProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

/**
 * Tenant id is an attribute of SimpleCredentials which gets added to AuthInfo
 * on creation of the AuthInfo, either via TokenInfoImpl or direct from
 * SimpleCredentials. The code that logs into the repository must provide a
 * tenant.id attribute in SimpleCredentials for that attribute to appear here.
 * If not, the tenant is null.
 */
public class TenantUtil {

    private static final Logger logger = LoggerFactory.getLogger(TenantUtil.class);

    private static final String TENANT_ID = "tenant.id";
    private static TenantProvider tenantProvider = Oak.defaultTenantProvider();



    @Nullable
    public static String getTenantId(@Nonnull AuthInfo authInfo) {
        return (String) authInfo.getAttribute(TENANT_ID);
    }

    @Nonnull
    public static Map<String, Object> setTenantIdAttribute(@Nonnull Map<String, Object> attributes, @Nullable String tenantId) {
        attributes.put(TENANT_ID, tenantId);
        return attributes;
    }

    /**
     * Reports on the tenant binding of a credentials.
     * @param credentials
     */
    @Nullable
    public static String getTenantId(@Nullable Credentials credentials) {
        if (credentials instanceof SimpleCredentials) {
            return (String) ((SimpleCredentials) credentials).getAttribute(TENANT_ID);
        } else if (credentials != null) {
            try {
                Method m = credentials.getClass().getMethod("getTenantId");
                return (String)m.invoke(credentials);
            } catch (Exception e) {
                logger.debug("Unable to get tenantid from {}, {} ", credentials, e.getMessage());
            }
        }
        return null;
    }

    @Nullable
    public static String getTenantId(@Nullable String[] paths) {
        if (paths == null || paths.length == 0) {
            // no tenant.
            return null;
        }
        String tenantId = null;
        for (String path : paths) {

            String p = tenantProvider.getTenantId(path);
            if (p != null) {
                if (tenantId == null) {
                    tenantId = p;
                } else if (!tenantId.equals(p)) {
                    throw new IllegalArgumentException("The paths "+ Arrays.toString(paths)+" belong to more than one tenant, which is an illegal access pattern ");
                }
            }
        }
        return tenantId;
    }


    /**
     * Add tenant Id to the credentials if specified. If no tenantId is added or the tenantId is null, then
     * the credentials are not bound to a tenant and the session will not be able to see anything designated
     * as tenant data.
     * @param credentials the credentials, only SimpleCredentials will get bound to a tenant.
     * @param tenantId the tenantId.
     * @return credentials.
     */
    @Nullable
    public static Credentials addTenantId(@Nullable Credentials credentials, @Nullable String tenantId) {
        if ( tenantId != null ) {
            if (credentials instanceof SimpleCredentials && tenantId != null) {
                ((SimpleCredentials) credentials).setAttribute("tenant.id", tenantId);
                logger.debug("Credentials {} bound to tenant {}  ");
                return credentials;
            } else if ( credentials != null) {
                // Guest Credentials dont work here as its part of JCR API.
                // AdministrativeCredentials needed to be patched to add the setTenantId method.
                try {
                    Method m = credentials.getClass().getMethod("setTenantId", String.class);
                    m.invoke(credentials, tenantId);
                    logger.debug("Credentials {} bound to tenant {}  ");
                    return credentials;
                } catch (Exception e) {
                    logger.debug("Unable to set tenantid on {}, {} ", credentials, e.getMessage());
                }
            }
        }
        logger.warn("Credentials {} not bound to tenant {}  ");
        return credentials;
    }

    /**
     * Reports on the tenant binding of a credentials.
     * @param credentials
     */
    public static void reportOnTenant(@Nullable Credentials credentials) {
        String tenantId = null;
        if (credentials instanceof SimpleCredentials) {
            tenantId = (String) ((SimpleCredentials) credentials).getAttribute(TENANT_ID);
        } else if (credentials != null) {
            try {
                Method m = credentials.getClass().getMethod("getTenantId");
                tenantId = (String)m.invoke(credentials);
            } catch (Exception e) {
                logger.debug("Unable to get tenantid from {}, {} ", credentials, e.getMessage());
            }
        }
        if (tenantId != null) {
            logger.debug("Credentials {} bound to tenant {} ", credentials, tenantId);
        } else {
            logger.warn("Credentials {} not bound to any tenant ", credentials);
        }
    }



}
