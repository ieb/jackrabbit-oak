package org.apache.jackrabbit.oak.spi.blob;


import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.conversion.customtypes.PrivateURI;

import java.net.URI;

/**
 * Created by boston on 05/09/2017.
 */
public interface WithURISupport {

    /**
     * Return a URI suitable to be used anywhere for the supplied Blob, or return
     * null if the DataStore doesnt support URIs or the Blob cant have a URI.
     * @param blob the blob.
     * @return a URI or null if no URI available in this context.
     */
    URI getURI(Blob blob);

    /**
     * Retrun a PrivateURI suitable for use by trusted components, but not suitable
     * for transmission outside of a trusted secure context. (ie not suitable for
     * public use).
     * @param blob the blob.
     * @return a URI or null if no PrivateURI is available in this context.
     */
    PrivateURI getPrivateURI(Blob blob);
}
