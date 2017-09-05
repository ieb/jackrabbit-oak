package org.apache.jackrabbit.oak.spi.blob;

import org.apache.jackrabbit.oak.api.conversion.customtypes.PrivateURI;

/**
 * Created by boston on 05/09/2017.
 */
public interface BlobWithPrivateURI {
    PrivateURI getPrivateURI();
}
