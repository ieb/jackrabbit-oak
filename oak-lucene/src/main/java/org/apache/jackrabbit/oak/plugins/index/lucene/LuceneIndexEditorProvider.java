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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.IndexEditor;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

/**
 * Service that provides Lucene based {@link IndexEditor}s
 * 
 * @see LuceneIndexEditor
 * @see IndexEditorProvider
 * 
 */
public class LuceneIndexEditorProvider implements IndexEditorProvider {
    private final IndexCopier indexCopier;
    private final StatisticsProvider statisticsProvider;
    private final ExtractedTextCache extractedTextCache;
    private final IndexAugmentorFactory augmentorFactory;
    private final LuceneIndexWriterFactory indexWriterFactory;

    public LuceneIndexEditorProvider() {
        this(null, StatisticsProvider.NOOP);
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier, StatisticsProvider statisticsProvider) {
        //Disable the cache by default in ExtractedTextCache
        this(indexCopier, statisticsProvider, new ExtractedTextCache(0, 0));
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     StatisticsProvider statisticsProvider,
                                     ExtractedTextCache extractedTextCache) {
        this(indexCopier, statisticsProvider, extractedTextCache, null, Mounts.defaultMountInfoProvider());
    }

    public LuceneIndexEditorProvider(@Nullable IndexCopier indexCopier,
                                     StatisticsProvider statisticsProvider,
                                     ExtractedTextCache extractedTextCache,
                                     @Nullable IndexAugmentorFactory augmentorFactory,
                                     MountInfoProvider mountInfoProvider) {
        this.indexCopier = indexCopier;
        this.statisticsProvider = statisticsProvider;
        this.extractedTextCache = checkNotNull(extractedTextCache);
        this.augmentorFactory = augmentorFactory;

        this.indexWriterFactory = new DefaultIndexWriterFactory(checkNotNull(mountInfoProvider), indexCopier, statisticsProvider);
    }

    @Override
    public Editor getIndexEditor(
            @Nonnull String type, @Nonnull NodeBuilder definition, @Nonnull NodeState root,
            @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        if (TYPE_LUCENE.equals(type)) {
            LuceneIndexEditorContext context = new LuceneIndexEditorContext(root, definition, callback,
                    indexWriterFactory, extractedTextCache, augmentorFactory, statisticsProvider);
            return new LuceneIndexEditor(context);
        }
        return null;
    }

    IndexCopier getIndexCopier() {
        return indexCopier;
    }

    ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }
}
