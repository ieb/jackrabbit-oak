package org.apache.jackrabbit.oak.plugins.document.cassandra;

/**
 * Created by ieb on 18/02/2016.
 */
public class Utils {

    public static String getParentUsingFrom(String fromKey) {
        int c = fromKey.indexOf(':');
        if ( c < 0 ) {
            throw new IllegalArgumentException("Key doesnt containa :, so isnt recognised as a valid key");
        }
        int pathCount = Integer.parseInt(fromKey.substring(0,c))-1;
        return String.valueOf(pathCount)+fromKey.substring(c, fromKey.length() - 1);
    }

    public static String getParentFromChildPath(String path) {
        int c = path.indexOf(':');
        if ( c < 0 ) {
            throw new IllegalArgumentException("Path doesnt containa :, so isnt recognised as a valid key");
        }
        int l = path.lastIndexOf('/');
        if ( l < 0 ) {
            throw new IllegalArgumentException("Key doesnt contain a /, so isnt recognised as a valid path with a child");
        }
        int pathCount = Integer.parseInt(path.substring(0,c))-1;
        return String.valueOf(pathCount)+path.substring(c, l - 1);
    }

}
