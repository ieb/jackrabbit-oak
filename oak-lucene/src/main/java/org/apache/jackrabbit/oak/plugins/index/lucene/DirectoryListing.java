package org.apache.jackrabbit.oak.plugins.index.lucene;

/**
 * Maintains an DirectoryListing of an OakDirectory.
 */
public interface DirectoryListing {
    /**
     * List all files in the directory.
     * @return
     */
    String[] listAll();

    /**
     * return true if the name is in the directory.
     * @param name
     * @return
     */
    boolean contains(String name);

    /**
     * remove the name from the directory listing, throw an exception if the name is not in the directory to let the caller
     * know of the problem. Save any unsaved state if required.
     * @param name
     */
    void remove(String name);

    /**
     * Add the name to the directory listing, saving any unsaved state.
     * @param name
     */
    void add(String name);

    /**
     * Close the directory listing, saving any unsaved state.
     */
    void close();

}
