/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * A IndexDeletionPolicy that keeps a configurable number of previous commits. This should enable
 * recovery should the current comm
 */
public class DelayedIndexDeletionPolicy extends IndexDeletionPolicy {
    private static final Logger LOGGER = LoggerFactory.getLogger(DelayedIndexDeletionPolicy.class);
    private int maxGenerations;

    public DelayedIndexDeletionPolicy(int maxGenerations) {
        this.maxGenerations = maxGenerations;
    }

    @Override
    public void onInit(List<? extends IndexCommit> commits) throws IOException {
        applyPolicy(commits);
    }


    @Override
    public void onCommit(List<? extends IndexCommit> commits) throws IOException {
        applyPolicy(commits);
    }

    /**
     * @param commits an ordered list of commits, the 0th element is the oldest.
     */
    private void applyPolicy(List<? extends IndexCommit> commits) {
        if ( commits != null && commits.size() > maxGenerations ) {
            int id = commits.size()-maxGenerations;
            for ( int i = 0; i < id; i++) {
                IndexCommit c = commits.get(i);
                LOGGER.info("Deleting generation {} ", c);
                c.delete();
            }
            if ( LOGGER.isDebugEnabled()) {
                for (int i = id; i < commits.size(); i++) {
                    LOGGER.info("Keeping generation {} ", commits.get(i));
                }
            }
        } else {
            LOGGER.info("Keeping {} genrations.", commits.size());

        }
    }

}
