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

package org.apache.jackrabbit.oak.conversion;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.conversion.customtypes.PrivateURI;
import org.apache.jackrabbit.oak.api.conversion.OakConversionService;
import org.apache.jackrabbit.oak.plugins.value.OakValue;
import org.apache.jackrabbit.oak.spi.blob.BlobWithPrivateURI;
import org.apache.jackrabbit.oak.spi.blob.BlobWithURI;

import javax.jcr.RepositoryException;
import java.net.URI;

/**
 * Exposed OSGi service implementing the OakConversionService.
 * This is the service that components outside Oak should
 */
@Component( immediate = true)
@Service(OakConversionService.class)
public class OakConversionServiceImpl implements OakConversionService {




    public OakConversionServiceImpl() {

    }

    @Override
    public <T> T convertTo(Object source, Class<T> targetClass) {
        try {
            if (source instanceof OakValue && URI.class.equals(targetClass)) {
                return (T) convertOakValueToURI((OakValue) source);
            } else if (source instanceof OakValue && PrivateURI.class.equals(targetClass)) {
                return (T) convertOakValueToPrivateURI((OakValue) source);
                // put other coversions that are supported here
                // restructure the if, then, else if this gets inefficient.
            } else {
                return null;
            }
        } catch (RepositoryException e) {
            // cant convert, possible due to permission denied.
            return null;
        }
    }

    private PrivateURI  convertOakValueToPrivateURI(OakValue source) throws RepositoryException {
        Blob blob = source.getBlob();
        if ( blob instanceof BlobWithPrivateURI) {
            return ((BlobWithPrivateURI)blob).getPrivateURI();

        }
        return null;
    }

    private URI convertOakValueToURI(OakValue source) throws RepositoryException {
        Blob blob = source.getBlob();
        if ( blob instanceof BlobWithURI) {
            return ((BlobWithURI)blob).getURI();
        }
        return null;
    }
}
