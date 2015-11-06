/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.apache.commons.codec.binary.Hex;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.NoLockFactory;
import org.apache.lucene.util.WeakIdentityMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_LASTMODIFIED;
import static org.apache.jackrabbit.oak.api.Type.BINARIES;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.INDEX_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;

/**
 * Implementation of the Lucene {@link Directory} (a flat list of files)
 * based on an Oak {@link NodeBuilder}.
 */
class OakDirectory extends Directory {
    static final PerfLogger PERF_LOGGER = new PerfLogger(LoggerFactory.getLogger(OakDirectory.class.getName() + ".perf"));
    static final String PROP_DIR_LISTING = "dirListing";
    static final String PROP_BLOB_SIZE = "blobSize";
    static final String PROP_UNIQUE_KEY = "uniqueKey";
    static final int UNIQUE_KEY_SIZE = 16;
    
    private final static SecureRandom secureRandom = new SecureRandom();
    
    protected final NodeBuilder builder;
    protected final NodeBuilder directoryBuilder;
    private final IndexDefinition definition;
    private LockFactory lockFactory;
    private final boolean readOnly;
    private final DirectoryListing listing;
    private final boolean activeDeleteEnabled;

    public OakDirectory(NodeBuilder builder, IndexDefinition definition, boolean readOnly) {
        this.lockFactory = NoLockFactory.getNoLockFactory();
        this.builder = builder;
        this.directoryBuilder = builder.child(INDEX_DATA_CHILD_NAME);
        this.definition = definition;
        this.readOnly = readOnly;
        long start = PERF_LOGGER.start();
        this.listing = new GenerationalDirectoryListing(definition, builder, readOnly);
        PERF_LOGGER.end(start, 100, "Directory listing performed. Total {} files", listing.listAll().length);
        this.activeDeleteEnabled = definition.getActiveDeleteEnabled();
    }

    @Override
    public String[] listAll() throws IOException {
        return listing.listAll();
    }

    @Override
    public boolean fileExists(String name) throws IOException {
        return listing.contains(name);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        checkArgument(!readOnly, "Read only directory");
        String storageName = listing.getStorageName(name);
        if (listing.remove(name)) {
            // TODO: make remove control if the data is removed, trashed or left alone.
            NodeBuilder f = directoryBuilder.getChildNode(storageName);
            if (activeDeleteEnabled) {
                PropertyState property = f.getProperty(JCR_DATA);
                ArrayList<Blob> data;
                if (property != null && property.getType() == BINARIES) {
                    data = newArrayList(property.getValue(BINARIES));
                } else {
                    data = newArrayList();
                }
                NodeBuilder trash = builder.child(LuceneIndexConstants.TRASH_CHILD_NAME);
                long index;
                if (!trash.hasProperty("index")) {
                    index = 1;
                } else {
                    index = trash.getProperty("index").getValue(Type.LONG) + 1;
                }
                trash.setProperty("index", index);
                NodeBuilder trashEntry = trash.child("run_" + index);
                trashEntry.setProperty("time", System.currentTimeMillis());
                trashEntry.setProperty("name", storageName);
                trashEntry.setProperty(JCR_DATA, data, BINARIES);
            }
            f.remove();
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        String storageName = listing.getStorageName(name);

        NodeBuilder file = directoryBuilder.getChildNode(storageName);
        OakIndexInput input = new OakIndexInput(storageName, file);
        try {
            return input.length();
        } finally {
            input.close();
        }
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context)
            throws IOException {
        checkArgument(!readOnly, "Read only directory");
        String storageName = listing.getStorageName(name);
        NodeBuilder file;
        if (!directoryBuilder.hasChildNode(storageName)) {
            file = directoryBuilder.child(storageName);
            byte[] uniqueKey = new byte[UNIQUE_KEY_SIZE];
            secureRandom.nextBytes(uniqueKey);
            String key = StringUtils.convertBytesToHex(uniqueKey);
            file.setProperty(PROP_UNIQUE_KEY, key);
            file.setProperty(PROP_BLOB_SIZE, definition.getBlobSize());
        } else {
            file = directoryBuilder.child(storageName);
        }
        listing.add(name);
        return new OakIndexOutput(storageName, file);
    }


    @Override
    public IndexInput openInput(String name, IOContext context)
            throws IOException {
        String storageName = listing.getStorageName(name);
        NodeBuilder file = directoryBuilder.getChildNode(storageName);
        if (file.exists()) {
            return new OakIndexInput(storageName, file);
        } else {
            throw new FileNotFoundException(storageName);
        }
    }

    @Override
    public Lock makeLock(String name) {
        return lockFactory.makeLock(name);
    }

    @Override
    public void clearLock(String name) throws IOException {
        lockFactory.clearLock(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // ?
        /*
         * sync means that the names must be committed to stable storage when it is called.
         * That should mean a commit is performed and the listing saves the files to the stable storage.
         * Since stable storage in this case means the Oak session and all names are written to stable
         * storage already, the listing must be called to perform its implementation of sync.
         */
        if (!readOnly && definition.saveDirListing()) {
            listing.sync(names);
        }
    }

    @Override
    public void close() throws IOException {
        if (!readOnly && definition.saveDirListing()) {
            listing.close();
        }
    }

    @Override
    public void setLockFactory(LockFactory lockFactory) throws IOException {
        this.lockFactory = lockFactory;
    }

    @Override
    public LockFactory getLockFactory() {
        return lockFactory;
    }

    @Override
    public String toString() {
        return "Directory for " + definition.getIndexName();
    }


    /**
     * Size of the blob entries to which the Lucene files are split.
     * Set to higher than the 4kB inline limit for the BlobStore,
     */
    static final int DEFAULT_BLOB_SIZE = 32 * 1024;

    /**
     * A file, which might be split into multiple blobs.
     */
    private static class OakIndexFile {

        private final static Logger LOGGER = LoggerFactory.getLogger(OakIndexFile.class);

        /**
         * The file name.
         */
        private final String name;

        /**
         * The node that contains the data for this file.
         */
        private final NodeBuilder file;

        /**
         * The maximum size of each blob.
         */
        private final int blobSize;
        
        /**
         * The current position within the file (for positioned read and write
         * operations).
         */
        private long position = 0;

        /**
         * The length of the file.
         */
        private long length;

        /**
         * The list of blobs (might be empty).
         * The last blob has a size of 1 up to blobSize.
         * All other blobs have a size of blobSize.
         */
        private List<Blob> data;

        /**
         * Whether the data was modified since it was last flushed. If yes, on a
         * flush, the metadata, and the list of blobs need to be stored.
         */
        private boolean dataModified = false;

        /**
         * The index of the currently loaded blob.
         */
        private int index = -1;

        /**
         * The data of the currently loaded blob.
         */
        private byte[] blob;
        
        /**
         * The unique key that is used to make the content unique (to allow removing binaries from the blob store without risking to remove binaries that are still needed).
         */
        private final byte[] uniqueKey;

        /**
         * Whether the currently loaded blob was modified since the blob was
         * flushed.
         */
        private boolean blobModified = false;

        public OakIndexFile(String name, NodeBuilder file) {
            this.name = name;
            LOGGER.debug("Opening {} ", name);
            this.file = file;
            this.blobSize = determineBlobSize(file);
            this.uniqueKey = readUniqueKey(file);
            this.blob = new byte[blobSize];

            PropertyState property = file.getProperty(JCR_DATA);
            if (property != null && property.getType() == BINARIES) {
                this.data = newArrayList(property.getValue(BINARIES));
            } else {
                this.data = newArrayList();
            }

            this.length = (long)data.size() * blobSize;
            if (!data.isEmpty()) {
                Blob last = data.get(data.size() - 1);
                this.length -= blobSize - last.length();
                if (uniqueKey != null) {
                    this.length -= uniqueKey.length;
                }
            }
        }

        private OakIndexFile(OakIndexFile that) {
            this.name = that.name;
            this.file = that.file;
            this.blobSize = that.blobSize;
            this.uniqueKey = that.uniqueKey;
            this.blob = new byte[blobSize];

            this.position = that.position;
            this.length = that.length;
            this.data = newArrayList(that.data);
            this.dataModified = that.dataModified;
        }

        private void loadBlob(int i) throws IOException {
            checkElementIndex(i, data.size());
            if (index != i) {
                flushBlob();
                checkState(!blobModified);

                int n = (int) Math.min(blobSize, length - i * blobSize);
                InputStream stream = data.get(i).getNewStream();
                try {
                    ByteStreams.readFully(stream, blob, 0, n);
                } finally {
                    stream.close();
                }
                index = i;
            }
        }

        private void flushBlob() throws IOException {
            if (blobModified) {
                int n = (int) Math.min(blobSize, length - index * blobSize);
                InputStream in = new ByteArrayInputStream(blob, 0, n);
                if (uniqueKey != null) {
                    in = new SequenceInputStream(in, 
                            new ByteArrayInputStream(uniqueKey));
                }
                Blob b = file.createBlob(in);
                if (index < data.size()) {
                    data.set(index, b);
                } else {
                    checkState(index == data.size());
                    data.add(b);
                }
                dataModified = true;
                blobModified = false;
            }
        }

        public void seek(long pos) throws IOException {
            // seek() may be called with pos == length
            // see https://issues.apache.org/jira/browse/LUCENE-1196
            if (pos < 0 || pos > length) {
                throw new IOException("Invalid seek request");
            } else {
                position = pos;
            }
        }

        public void readBytes(byte[] b, int offset, int len)
                throws IOException {
            checkPositionIndexes(offset, offset + len, checkNotNull(b).length);

            if (len < 0 || position + len > length) {
                String msg = String.format("Invalid byte range request [%s] : position : %d, length : " +
                                "%d, len : %d", name, position, length, len);
                throw new IOException(msg);
            }

            int i = (int) (position / blobSize);
            int o = (int) (position % blobSize);
            while (len > 0) {
                loadBlob(i);

                int l = Math.min(len, blobSize - o);
                System.arraycopy(blob, o, b, offset, l);

                offset += l;
                len -= l;
                position += l;

                i++;
                o = 0;
            }
        }

        public void writeBytes(byte[] b, int offset, int len)
                throws IOException {
            int i = (int) (position / blobSize);
            int o = (int) (position % blobSize);
            while (len > 0) {
                int l = Math.min(len, blobSize - o);

                if (index != i) {
                    if (o > 0 || (l < blobSize && position + l < length)) {
                        loadBlob(i);
                    } else {
                        flushBlob();
                        index = i;
                    }
                }
                System.arraycopy(b, offset, blob, o, l);
                blobModified = true;

                offset += l;
                len -= l;
                position += l;
                length = Math.max(length, position);

                i++;
                o = 0;
            }
        }

        private static int determineBlobSize(NodeBuilder file){
            if (file.hasProperty(PROP_BLOB_SIZE)){
                return Ints.checkedCast(file.getProperty(PROP_BLOB_SIZE).getValue(Type.LONG));
            }
            return DEFAULT_BLOB_SIZE;
        }

        private static byte[] readUniqueKey(NodeBuilder file) {
            if (file.hasProperty(PROP_UNIQUE_KEY)) {
                String key = file.getString(PROP_UNIQUE_KEY);
                return StringUtils.convertHexToBytes(key);
            }
            return null;
        }

        public void flush() throws IOException {
            flushBlob();
            if (dataModified) {
                file.setProperty(JCR_LASTMODIFIED, System.currentTimeMillis());
                file.setProperty(JCR_DATA, data, BINARIES);
                dataModified = false;
            }
        }

        @Override
        public String toString() {
            return name;
        }

    }

    private static class OakIndexInput extends IndexInput {

        private final OakIndexFile file;
        private boolean isClone = false;
        private final WeakIdentityMap<OakIndexInput, Boolean> clones;

        public OakIndexInput(String name, NodeBuilder file) {
            super(name);
            this.file = new OakIndexFile(name, file);
            clones = WeakIdentityMap.newConcurrentHashMap();
        }

        private OakIndexInput(OakIndexInput that) {
            super(that.toString());
            this.file = new OakIndexFile(that.file);
            clones = null;
        }

        @Override
        public OakIndexInput clone() {
            // TODO : shouldn't we call super#clone ?
            OakIndexInput clonedIndexInput = new OakIndexInput(this);
            clonedIndexInput.isClone = true;
            if (clones != null) {
                clones.put(clonedIndexInput, Boolean.TRUE);
            }
            return clonedIndexInput;
        }

        @Override
        public void readBytes(byte[] b, int o, int n) throws IOException {
            checkNotClosed();
            file.readBytes(b, o, n);
        }

        @Override
        public byte readByte() throws IOException {
            checkNotClosed();
            byte[] b = new byte[1];
            readBytes(b, 0, 1);
            return b[0];
        }

        @Override
        public void seek(long pos) throws IOException {
            checkNotClosed();
            file.seek(pos);
        }

        @Override
        public long length() {
            checkNotClosed();
            return file.length;
        }

        @Override
        public long getFilePointer() {
            checkNotClosed();
            return file.position;
        }

        @Override
        public void close() {
            file.blob = null;
            file.data = null;

            if (clones != null) {
                for (Iterator<OakIndexInput> it = clones.keyIterator(); it.hasNext();) {
                    final OakIndexInput clone = it.next();
                    assert clone.isClone;
                    clone.close();
                }
            }
        }

        private void checkNotClosed() {
            if (file.blob == null && file.data == null) {
                throw new AlreadyClosedException("Already closed: " + this);
            }
        }

    }

    private final class OakIndexOutput extends IndexOutput {

        private final OakIndexFile file;

        public OakIndexOutput(String name, NodeBuilder file) throws IOException {
            this.file = new OakIndexFile(name, file);
        }

        @Override
        public long length() {
            return file.length;
        }

        @Override
        public long getFilePointer() {
            return file.position;
        }

        @Override
        public void seek(long pos) throws IOException {
            file.seek(pos);
        }

        @Override
        public void writeBytes(byte[] b, int offset, int length)
                throws IOException {
            file.writeBytes(b, offset, length);
        }

        @Override
        public void writeByte(byte b) throws IOException {
            writeBytes(new byte[]{b}, 0, 1);
        }

        @Override
        public void flush() throws IOException {
            file.flush();
        }

        @Override
        public void close() throws IOException {
            flush();
            file.blob = null;
            file.data = null;
        }

    }

    /**
     * Maintains an DirectoryListing of an OakDirectory.
     */
    public interface DirectoryListing {
        /**
         * List all files in the directory.
         * @return
         */
        @Nonnull
        String[] listAll();

        /**
         * return true if the name is in the directory.
         * @param name
         * @return
         */
        boolean contains(@Nullable String name);

        /**
         * remove the name from the directory listing, throw an exception if the name is not in the directory to let the caller
         * know of the problem. Save any unsaved state if required.
         * @param name
         */
        boolean remove(@Nonnull String name);

        /**
         * Add the name to the directory listing, saving any unsaved state.
         * @param name
         */
        void add(@Nonnull String name);

        @Nonnull
        String getStorageName(@Nonnull String name);

        /**
         * Close the directory listing, saving any unsaved state.
         */
        void close();

        void sync(Collection<String> names);
    }


    private static class SimpleDirectoryListing implements DirectoryListing {

        private Set<String> fileNames = Sets.newConcurrentHashSet();
        private NodeBuilder directoryBuilder;
        private IndexDefinition definition;

        private SimpleDirectoryListing(@Nonnull IndexDefinition definition, @Nonnull NodeBuilder builder) {
            this.definition = definition;
            this.directoryBuilder = builder.child(INDEX_DATA_CHILD_NAME);
            this.fileNames.addAll(getListing());
        }


        @Nonnull
        private Set<String> getListing(){
            Iterable<String> fileNames = null;
            if (this.definition.saveDirListing()) {
                PropertyState listing = this.directoryBuilder.getProperty(OakDirectory.PROP_DIR_LISTING);
                if (listing != null) {
                    fileNames = listing.getValue(Type.STRINGS);
                }
            }

            if (fileNames == null){
                fileNames = this.directoryBuilder.getChildNodeNames();
            }
            Set<String> result = ImmutableSet.copyOf(fileNames);
            return result;
        }

        @Override
        public String[] listAll() {
            return fileNames.toArray(new String[fileNames.size()]);
        }

        @Override
        public boolean contains(@Nonnull String name) {
            return fileNames.contains(name);
        }

        @Override
        public boolean remove(@Nonnull String name) {
            fileNames.remove(name);
            return true;
        }

        @Override
        public void add(@Nonnull String name) {
            fileNames.add(name);
        }

        @Override
        public void close() {
            save();
        }


        private void save() {
            this.directoryBuilder.setProperty(createProperty(OakDirectory.PROP_DIR_LISTING, fileNames, STRINGS));
        }


        @Override
        public void sync(Collection<String> names) {
            // this type of directory cant differentiate between files synced and files known about, so just
            // sync everything by performing a save operation.
            save();
        }

        @Nonnull
        @Override
        public String getStorageName(@Nonnull String name) {
            return name;
        }
    }



    /**
     * Implements a simple generational list of the lucene directory, loading the last valid
     * generation on creation, and saving new generations on each update.
     * Created by ieb on 23/10/15.
     */
    private static class GenerationalDirectoryListing implements DirectoryListing {


        private static final Logger LOGGER = LoggerFactory.getLogger(GenerationalDirectoryListing.class);
        public static final String DIRECTORY_LISTING_CONTAINER = ":dir";
        public static final String DIRECTORY_LISTING_PREFIX = "l_";
        public static final String LISTING_STATE_PROPERTY = "state";
        private final IndexDefinition definition;
        // the buidler containing lucene files.
        private final NodeBuilder directoryBuilder;
        // the parent builder
        private final NodeBuilder builder;
        private final NodeBuilder listingBuilder;
        private final boolean readOnly;
        private Map<String, IndexFileMetadata> listOfFiles = Maps.newConcurrentMap();
        private long generation;

        private GenerationalDirectoryListing(@Nonnull IndexDefinition definition, @Nonnull NodeBuilder builder, boolean readOnly) {
            this.generation = System.currentTimeMillis();
            this.readOnly = readOnly;
            this.definition = definition;
            this.builder = builder;
            this.directoryBuilder = builder.child(INDEX_DATA_CHILD_NAME);

            this.listingBuilder = builder.child(DIRECTORY_LISTING_CONTAINER);
            if(load()) {
                save();
            }
        }

        @Override
        @Nonnull
        public String[] listAll() {
            return listOfFiles.keySet().toArray(new String[listOfFiles.size()]);
        }

        @Override
        @Nonnull
        public boolean contains(@Nonnull String name) {
            return listOfFiles.containsKey(name);
        }

        @Override
        public boolean remove(@Nonnull String name) {
            if (listOfFiles.containsKey(name)) {
                listOfFiles.remove(name);
                LOGGER.debug("Removed " + name + " from list");
            } else {
                LOGGER.warn("Attempt to remove " + name + " from list denied, as name not in list");
            }
            return false;
        }

        /**
         *
         * @param name
         */
        @Override
        public void add(@Nonnull String name) {
            listOfFiles.put(name, getIndexFileMetaData(name, getStorageName(name)));
            LOGGER.debug("Added " + name + " to list");
        }


        /**
         * Convert the current state into a string representation.
         * @return
         */
        @Nonnull
        private String getCurrentState() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, IndexFileMetadata> n : listOfFiles.entrySet()) {
                // make certain state is current by inspecting the file as it is.
                n.setValue(getIndexFileMetaData(n.getKey(), n.getValue().storageName));
            }
            return IndexFileMetadata.stringify(listOfFiles);
        }

        /**
         * Extract the state from the node builder, return null of the state isnt present.
         * @param listing
         * @return
         */
        @Nullable
        private Map<String, IndexFileMetadata> parseState(@Nonnull NodeBuilder listing) {
            if (listing.hasProperty(LISTING_STATE_PROPERTY)) {
                return IndexFileMetadata.parse(listing.getProperty(LISTING_STATE_PROPERTY).getValue(STRING));
            }
            return null;
        }

        /**
         * Validate the listing checking that its contents are present and correct in the Oak repo.
         * @param listing
         * @return
         */
        @Nullable
        private Map<String, IndexFileMetadata> validateListing(@Nonnull NodeBuilder listing) {
            if (listing.exists()) {
                Map<String, IndexFileMetadata> state = parseState(listing);
                if (state != null) {
                    for (Map.Entry<String, IndexFileMetadata> n : state.entrySet()) {
                        IndexFileMetadata oakVersion = getIndexFileMetaData(n.getKey(), n.getValue().storageName);
                        IndexFileMetadata localVersion = n.getValue();
                        if (!localVersion.equals(oakVersion)) {
                            LOGGER.warn("Index File and Oak Version and not the same {}  {}", localVersion.toString(), localVersion.diff(oakVersion));
                            return null;
                        }
                    }
                    // all the files are present, so the listing validates.
                    return state;
                }
            }
            return null;

        }


        /**
         * Load the fist valid listing, returning true of the listing should be saved.
         * @return
         */
        private boolean load() {
            // try and find generational files.
            // If no generational files fallback to previous methods.
            List<String> childNodes = Lists.newArrayList(listingBuilder.getChildNodeNames());
            Collections.sort(childNodes);
            int checked = 0;
            boolean save = false;
            int loaded = -1;
            Set<String> toRemove = Sets.newHashSet();
            LOGGER.debug("Ordered Listing files (evaluated in reverse order) {} {} ", builder, childNodes);
            for (int i = childNodes.size()-1; i >= 0; i--) {
                String dirName = childNodes.get(i);
                if (dirName.startsWith(DIRECTORY_LISTING_PREFIX)) {
                    checked++;
                    if (loaded < 0) {
                        Map<String, IndexFileMetadata> validatedList = validateListing(listingBuilder.getChildNode(dirName));
                        if (validatedList != null) {
                            LOGGER.info("Accepted directory listing for {} using {} ", builder, dirName);
                            listOfFiles.clear();
                            listOfFiles.putAll(validatedList);
                            loaded = i;
                        } else {
                            LOGGER.warn("Rejected directory listing {} ", dirName);
                        }
                    } else if (!readOnly && checked >= 100 ) {
                        toRemove.add(dirName);
                    }
                }
            }
            // if more than 10 listing files can be removed, perform a GC.
            if (toRemove.size() > 10) {
                doGC(childNodes, toRemove);
            }
            if (loaded >= 0 ) {
                return (loaded != childNodes.size() - 1);
            }
            if ( checked > 0) {
                LOGGER.warn("Recovery of corrupted index failed, will try and load files that are present in the index, last ditch attempt to recover. ");
            }
            // if there were files checked and none valid the index has to be rebuilt from scratch.
            // if none where checked then this is directory has no directory listings so fallback to the
            // simple method used previously.
            if (checked == 0) {
                // no generational files, fallback to simple method
                OakDirectory.SimpleDirectoryListing simpleDirectoryListing = new OakDirectory.SimpleDirectoryListing(definition, directoryBuilder);
                for (String s : simpleDirectoryListing.listAll()) {
                    IndexFileMetadata ifm = getIndexFileMetaData(s, s);
                    if (ifm != null) {
                        listOfFiles.put(s, ifm);
                    } else {
                        LOGGER.warn("Based On Directory Listing the file name {} is invalid ",s);
                    }

                }
            }
            return true;
        }

        private void doGC(List<String> childNodes, Set<String> toRemove) {
            Set<String> keep = Sets.newHashSet();
            // build a set of index files to keep by scanning all listings not in the remove set.
            for(String c : childNodes) {
                if (!toRemove.contains(c)) {
                    // extract the list of index files.
                    Map<String, IndexFileMetadata> l = parseState(listingBuilder.getChildNode(c));
                    if (l != null) {
                        // add all index files references into the keep set.
                        for (Map.Entry<String, IndexFileMetadata> e : l.entrySet()) {
                            keep.add(e.getValue().storageName);
                        }
                    }
                }
            }
            // scan all remove listings.
            for(String r : toRemove) {
                // get all files referenced
                Map<String, IndexFileMetadata> l = parseState(listingBuilder.getChildNode(r));
                if ( l != null) {
                    for(Map.Entry<String, IndexFileMetadata> s : l.entrySet()) {
                        // if not in the keep set, remove the index file.
                        IndexFileMetadata ifm = s.getValue();
                        if (!keep.contains(ifm.storageName)) {
                            LOGGER.warn("Removing Index file {} ",s);
                            directoryBuilder.getChildNode(ifm.storageName).remove();

                        }
                    }
                }
                // remove the listing file.
                LOGGER.warn("Removing Listing file {} ",r);
                listingBuilder.getChildNode(r).remove();
            }
        }

        @Nullable
        private IndexFileMetadata getIndexFileMetaData(@Nonnull String name, @Nonnull String storageName) {
            if (directoryBuilder.hasChildNode(storageName)) {
                OakIndexFile inp = new OakIndexFile(name, directoryBuilder.getChildNode(storageName));
                // Looking at OakIndexFile it will be quite expensive to generate a checksum due to the block nature, so for the moment
                // use the unique key.
                try {
                    MessageDigest sha1 = MessageDigest.getInstance("SHA1");
                    byte[] b = new byte[4096];
                    int i = 0;
                    for (; i < (inp.length-b.length); i += b.length) {
                        inp.readBytes(b, 0, b.length);
                        sha1.update(b, 0, b.length);
                    }
                    i = (int)(inp.length-i);
                    if (i > 0) {
                        inp.readBytes(b, 0, i);
                        sha1.update(b, 0, i);
                    }
                    return new IndexFileMetadata(name, storageName, inp.length, new String(Hex.encodeHex(sha1.digest())));
                } catch (IOException e ) {
                    LOGGER.warn("IO Exception reading index file ",e);
                    return new IndexFileMetadata(name, storageName, inp.length, "Unable to generate checksum "+e.getMessage());
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("JDK Doesnt have SHA1 support");
                }
            }
            return null;
        }


        @Override
        public void close() {
            LOGGER.info("Saving due to close.");
            save();
        }

        private void save() {
            if (!readOnly) {
                String listingGeneration = DIRECTORY_LISTING_PREFIX + System.currentTimeMillis();
                String listingState = getCurrentState();
                LOGGER.info("Saving Listing state to {} as {} ", listingGeneration, listingState);
                NodeBuilder n = listingBuilder.setChildNode(listingGeneration);
                n.setProperty(LISTING_STATE_PROPERTY, listingState);
            }
        }

        @Override
        public void sync(Collection<String> names) {
            // only perform a save on sync when the checkpoint files are updated, otherwise do nothing.
            for (String name : names) {
                if (listOfFiles.containsKey(name) && listOfFiles.get(name).isCheckpointFile() ) {
                    LOGGER.info("Saving due to sync  {} .", names);
                    save();
                    return;
                }
            }
        }


        @Nonnull
        @Override
        public String getStorageName(@Nonnull String name) {
            String storageName = name;
            if (listOfFiles.containsKey(name)) {
                storageName = listOfFiles.get(name).storageName;
            } else if ("segments.gen".equals(name)) {
                storageName = "segments.gen_"+generation;
            }
            return storageName;
        }
    }

    /**
     * Store some basic metadata about the file. A name, length and some form of checksum.
     */
    private static class IndexFileMetadata {

        private static final Logger LOGGER = LoggerFactory.getLogger(IndexFileMetadata.class);
        private final String name;
        private final String storageName;
        private final long length;
        private final String checkSum;


        public IndexFileMetadata(@Nonnull String externalForm) {
            LOGGER.debug("Directory Entry {} ", externalForm);
            String[] f = externalForm.split(",");
            if (f.length == 4) {
                name = f[0];
                storageName = f[1];
                length = Long.parseLong(f[2]);
                checkSum = f[3];
            } else {
                throw new IllegalArgumentException("Bad external form ");
            }
        }

        private String toExternalForm() {
            return name+","+storageName+","+length+","+checkSum;
        }


        public IndexFileMetadata(@Nonnull String name, @Nonnull String storageName, long length, @Nonnull String checkSum) {
            this.name = name;
            this.storageName = storageName;
            this.length = length;
            this.checkSum = checkSum;
        }

        /**
         * Metadata is equal if the name, length and checksum match.
         * @param obj
         * @return
         */
        @Override
        public boolean equals(@Nullable Object obj) {
            if (obj instanceof IndexFileMetadata) {
                IndexFileMetadata ifm = (IndexFileMetadata) obj;
                // the metadata is equal if the name equals and the file is mutable or the checksum and length are equal.
                return name.equals(ifm.name) && (isMutable() || length == ifm.length && checkSum.equals(ifm.checkSum));
            }
            return false;
        }

        /**
         * Some files within a Lucene directory are mutable.
         * @return true if the file is mutable.
         */
        private boolean isMutable() {
            return name.endsWith(".del");
        }

        /**
         * Some files in a Lucene directory are checkpoint files, written to perform a checkpoint.
         * @return
         */
        public boolean isCheckpointFile() {
            return name.equals("segments.gen") || name.startsWith("segments_");
        }


        @Override
        public String toString() {
            return "Name: "+name+", length:"+length+", checksum:"+checkSum;
        }

        /**
         * report differences between metadata.
         * @param otherVersion
         * @return
         */
        @Nonnull
        public String diff(@Nullable IndexFileMetadata otherVersion) {
            if (otherVersion == null) {
                return " OakVersion doesnt exist. ";
            }
            StringBuilder sb = new StringBuilder();
            if (!name.equals(otherVersion.name)) {
                sb.append("Names: ").append(name).append(" != ").append(otherVersion.name).append(",");
            }
            if (!checkSum.equals(otherVersion.checkSum)) {
                sb.append("CheckSum: ").append(checkSum).append(" != ").append(otherVersion.checkSum).append(",");
            }
            if (length != otherVersion.length) {
                sb.append("Length: ").append(length).append(" != ").append(otherVersion.length).append(",");
            }
            return sb.toString();
        }

        public void refresh() {
        }




        public static String stringify(Map<String, IndexFileMetadata> listOfFiles) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, IndexFileMetadata> e : listOfFiles.entrySet()) {
             sb.append(e.getValue().toExternalForm()).append(";");
            }
            return sb.toString();

        }


        public static Map<String, IndexFileMetadata> parse(String state) {
            ImmutableMap.Builder<String, IndexFileMetadata> mb = ImmutableMap.builder();
            if (state != null && state.trim().length() > 0) {
                LOGGER.debug("Directory state {} ", state);
                for (String l : state.split(";")) {
                    try {
                        IndexFileMetadata ifm = new IndexFileMetadata(l);
                        mb.put(ifm.name, ifm);
                    } catch (IllegalArgumentException e) {
                        LOGGER.warn("Empty or invalid IndexFileMetadata [{}] ",l);
                    }
                }
            } else {
                LOGGER.warn("Empty Listing state for Oak Index Directory");
            }
            return mb.build();

        }
    }

}
