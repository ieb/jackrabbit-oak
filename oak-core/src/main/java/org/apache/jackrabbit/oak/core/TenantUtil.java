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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.jcr.Credentials;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.security.tenant.TenantCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TenantUtil {

    private static final Logger log = LoggerFactory.getLogger(TenantUtil.class);
    public static final String SYSTEM_TENANT = "_system_";
    public static final String ATTR_TENANTID = "tenant.id";



    public static String getTenantId(Credentials credentials) {
        if (credentials instanceof TenantCredentials) {
            String tenantId = ((TenantCredentials) credentials).getTenantId();
            log.debug("TenantCredentials {} resolved tenant Id to {} ", credentials, tenantId);
            return tenantId;
        } else if (credentials != null) {
            try {
                Method m = credentials.getClass().getMethod("getAttribute", String.class);
                String tenantId = (String) m.invoke(credentials, ATTR_TENANTID);
                log.debug("Credentials {} resolved tenant Id to {} ", credentials, tenantId);
                return tenantId;
            } catch (IllegalArgumentException e) {
                log.debug(e.getMessage(),e);
            } catch (IllegalAccessException e) {
                log.debug(e.getMessage(),e);
            } catch (InvocationTargetException e) {
                log.debug(e.getMessage(),e);
            } catch (SecurityException e) {
                log.debug(e.getMessage(),e);
            } catch (NoSuchMethodException e) {
                log.debug(e.getMessage(),e);
            }
            try {
                Method m = credentials.getClass().getMethod("getTenantId");
                String tenantId = (String) m.invoke(credentials);
                log.debug("Credentials {} resolved tenant Id to {} ", credentials, tenantId);
                return tenantId;
            } catch (IllegalArgumentException e) {
                log.debug(e.getMessage(),e);
            } catch (IllegalAccessException e) {
                log.debug(e.getMessage(),e);
            } catch (InvocationTargetException e) {
                log.debug(e.getMessage(),e);
            } catch (SecurityException e) {
                log.debug(e.getMessage(),e);
            } catch (NoSuchMethodException e) {
                log.debug(e.getMessage(),e);
            }            
        } 
        log.debug("Credentials {} contained no tenant Id", credentials);
        return null;
    }

    public static String getTenantId(Root root) {
        ContentSession session = root.getContentSession();
        if (session instanceof ContentSessionImpl) {
            return ((ContentSessionImpl) session).getTenantId();
        }
        return null;
    }

}
