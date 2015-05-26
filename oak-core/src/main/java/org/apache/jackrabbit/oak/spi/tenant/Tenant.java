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
package org.apache.jackrabbit.oak.spi.tenant;

public class Tenant implements Comparable<Tenant>{
    public static final Tenant SYSTEM_TENANT = new Tenant("__system__");

    
    private String tenantId;
    

    // -------------- static -----------------------
    public static Tenant fromString(String tenantDescriptor) {
        if (tenantDescriptor.charAt(0) == '<') {
            int i = tenantDescriptor.indexOf('>');
            if (i > 0) {
                return new Tenant(tenantDescriptor.substring(1, i));
            }
        }
        throw new IllegalArgumentException("Not a valid tenant descriptor");
    }

    // -------------- public -----------------------
    public Tenant(String tenantId) {
        this.tenantId = tenantId;
        
    }
    
    public int compareTo(Tenant t) {
        return tenantId.compareTo(t.tenantId);
    }

    @Override
    public int hashCode() {
        return tenantId.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Tenant)) {
            return false;
        } else {
            return tenantId.equals(((Tenant)obj).tenantId);
        }
    }
    
    

    public int getMemory() {
        return tenantId.length();
        
    }


    public String toPathPrefix() {
       return "<"+tenantId+">";
    }


    public static String getPathPart(String s) {
        if ( s.charAt(0) == '<' ) {
            int i = s.indexOf('>');
            if ( i > 0 ) {
                int l = s.lastIndexOf('@');
                return s.substring(i+1,l);
            }
        }
        throw new IllegalArgumentException("Not a valid tenant descriptor");
    }

    public static String getRevisionPart(String s) {
        if ( s.charAt(0) == '<' ) {
            int i = s.indexOf('>');
            if ( i > 0 ) {
                int l = s.lastIndexOf('@');
                return s.substring(l+1);
            }
        }
        throw new IllegalArgumentException("Not a valid tenant descriptor");
    }

    public String getTenantId() {
        return tenantId;
    }





}
