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


package org.apache.jackrabbit.oak.spi.adapter;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

/**
 *
 */
public class AdapterManagerImplTest {

    private AdapterFactory adapterFactory1;
    private AdapterFactory adapterFactory2;

    @Before
    public void before() {
        adapterFactory1 = new AdapterFactory() {
            @Override
            public <T> T adaptTo(Object source, Class<T> targetClass) {
                if (source instanceof String) {
                    if (((String) source).startsWith("http") && URL.class.equals(targetClass)) {
                        try {
                            return (T) new URL((String) source);
                        } catch (MalformedURLException e) {
                            e.printStackTrace();
                        }
                    }
                    if ( URI.class.equals(targetClass)) {
                        try {
                            return (T) new URI((String) source);
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return null;
            }

            @Override
            public String[] getTargetClasses() {
                return new String[]{URI.class.getName(), URL.class.getName()};
            }

            @Override
            public int getPriority() {
                return 1;
            }
        };
        adapterFactory2 = new AdapterFactory() {
            @Override
            public <T> T adaptTo(Object source, Class<T> targetClass) {
                if ( source instanceof  String) {
                    if ( URI.class.equals(targetClass)) {
                        try {
                            return (T) new URI((String) source);
                        } catch (URISyntaxException e) {
                            e.printStackTrace();
                        }
                    }
                }
                return null;
            }

            @Override
            public String[] getTargetClasses() {
                return new String[]{ URI.class.getName()};
            }

            @Override
            public int getPriority() {
                return 2;
            }
        };

    }


    @Test
    public void testAdapterManagerImpl() {

        URI testing123_A0 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URI.class);
        URI testing123_B0 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URI.class);
        URL testing123_C0 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URL.class);
        URL testing123_D0 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A0);
        Assert.assertNull(testing123_B0);
        Assert.assertNull(testing123_C0);
        Assert.assertNull(testing123_D0);

        AdapterManager.getInstance().addAdapterFactory(adapterFactory1);
        AdapterManager.getInstance().addAdapterFactory(adapterFactory2);

        URI testing123_A = AdapterManager.getInstance().adaptTo("http://testing.com/123", URI.class);
        URI testing123_B = AdapterManager.getInstance().adaptTo("file://testing.com/123", URI.class);
        URL testing123_C = AdapterManager.getInstance().adaptTo("file://testing.com/123", URL.class);
        URL testing123_D = AdapterManager.getInstance().adaptTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A);
        Assert.assertEquals(testing123_A.getClass(),URI.class);
        Assert.assertNotNull(testing123_B);
        Assert.assertEquals(testing123_B.getClass(),URI.class);
        Assert.assertNull(testing123_C);
        Assert.assertNotNull(testing123_D);
        Assert.assertEquals(testing123_D.getClass(),URL.class);

        AdapterManager.getInstance().removeAdapterFactory(adapterFactory1);

        URI testing123_A1 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URI.class);
        URI testing123_B1 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URI.class);
        URL testing123_C1 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URL.class);
        URL testing123_D1 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URL.class);

        Assert.assertNotNull(testing123_A1);
        Assert.assertEquals(testing123_A1.getClass(),URI.class);
        Assert.assertNotNull(testing123_B1);
        Assert.assertEquals(testing123_B1.getClass(),URI.class);
        Assert.assertNull(testing123_C1);
        Assert.assertNull(testing123_D1);

        AdapterManager.getInstance().removeAdapterFactory(adapterFactory2);

        URI testing123_A2 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URI.class);
        URI testing123_B2 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URI.class);
        URL testing123_C2 = AdapterManager.getInstance().adaptTo("file://testing.com/123", URL.class);
        URL testing123_D2 = AdapterManager.getInstance().adaptTo("http://testing.com/123", URL.class);

        Assert.assertNull(testing123_A2);
        Assert.assertNull(testing123_B2);
        Assert.assertNull(testing123_C2);
        Assert.assertNull(testing123_D2);

    }
}
