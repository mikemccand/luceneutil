package perf;

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

import static org.apache.lucene.tests.util.TestUtil.randomRealisticUnicodeString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.lucene.util.BytesRef;

/**
 * Consuming {@link perf.LineFileDocs.LineFileDoc}, group them and put the grouped docs into
 * a thread-safe queue.
 */
public abstract class DocGrouper {
    protected final int numDocs;
    protected final BlockingQueue<DocGroups> outputQueue = new ArrayBlockingQueue<>(1024);
    public static final DocGroups END = new DocGroups() {
        @Override
        public BytesRef getGroupId() {
            return null;
        }

        @Override
        public LineFileDocs.LineFileDoc getNextLFD() {
            return null;
        }

        @Override
        public int getNumOfDocsInGroup() {
            return 0;
        }

        @Override
        public int getRemainingNumGroups() {
            return 0;
        }
    };

    public static BytesRef[] group100;
    public static BytesRef[] group100K;
    public static BytesRef[] group10K;
    public static BytesRef[] group1M;

    DocGrouper(int numDocs) {
        this.numDocs = numDocs;
    }

    public DocGroups getNextDocGroups() throws InterruptedException {
        return outputQueue.take();
    }

    public static synchronized void initGroupIds(Random random) {
        if (group100 != null) {
            throw new IllegalStateException("Cannot init group ids twice");
        }
        group100 = randomStrings(100, random);
        group10K = randomStrings(10000, random);
        group100K = randomStrings(100000, random);
        group1M = randomStrings(1000000, random);
    }

    // returned array will not have dups
    private static BytesRef[] randomStrings(int count, Random random) {
        final BytesRef[] strings = new BytesRef[count];
        HashSet<String> idSet = new HashSet<>(count);
        int i = 0;
        while(i < count) {
            String s = randomRealisticUnicodeString(random);
            while (s.equals("") == false && idSet.contains(s)) {
                s = randomRealisticUnicodeString(random);
            }
            strings[i++] = new BytesRef(s);
            idSet.add(s);
        }

        return strings;
    }

    public abstract void add(LineFileDocs.LineFileDoc lfd) throws InterruptedException;

    /**
     * A simple impl when we do not need grouping
     */
    static final class NoGroupImpl extends DocGrouper {

        NoGroupImpl(int numDocs) {
            super(numDocs);
        }

        @Override
        public void add(LineFileDocs.LineFileDoc lfd) throws InterruptedException {
            if (lfd == LineFileDocs.END) {
                outputQueue.put(END);
            }
            outputQueue.put(new DocGroups.SingleLFD(lfd));
        }
    }

    static final class TextGrouper extends DocGrouper {

        private int groupCounter;
        private int docCounter;
        private int nextNumDocs;
        private LineFileDocs.LineFileDoc[] buffer;
        private final BytesRef[] groupIds;
        private final float docsPerGroupBlock;

        TextGrouper(int numDocs) {
            super(numDocs);
            assert group100 != null;
            if (numDocs >= 5000000) {
                groupIds = group1M;
            } else if (numDocs >= 500000) {
                groupIds = group100K;
            } else {
                groupIds = group10K;
            }
            docsPerGroupBlock = ((float) numDocs) / groupIds.length;
            reset();
        }

        @Override
        public void add(LineFileDocs.LineFileDoc lfd) throws InterruptedException {
            if (lfd == LineFileDocs.END) {
                assert docCounter == 0;
                outputQueue.put(END);
            }
            buffer[docCounter++] = lfd;
            if (docCounter == nextNumDocs) {
                outputQueue.put(new DocGroups.TextBased(groupIds[groupCounter], buffer));
                groupCounter++;
                reset();
            }
        }

        /* Called when we move to next group */
        private void reset() {
            nextNumDocs = calculateNextGroupDocCount();
            buffer = new LineFileDocs.LineFileDoc[nextNumDocs];
        }

        private int calculateNextGroupDocCount() {
            if (groupCounter == groupIds.length - 1) {
                // The last group, we make sure the sum matches the total doc count
                return numDocs - ((int) (groupCounter * docsPerGroupBlock));
            } else {
                // This will toggle between X and X+1 docs,
                // converging over time on average to the
                // floating point docsPerGroupBlock:
                return ((int) ((1 + groupCounter) * docsPerGroupBlock)) - ((int) (groupCounter * docsPerGroupBlock));
            }
        }
    }

    /**
     * The class represent one or more document groups
     * Note only when we're consuming binary LFD there'll be more than one groups in
     * the class
     */
    public static abstract class DocGroups {
        public abstract BytesRef getGroupId();
        public abstract LineFileDocs.LineFileDoc getNextLFD();
        public abstract int getNumOfDocsInGroup();
        public abstract int getRemainingNumGroups();

        /**
         * A wrapper for singleLFD, when we don't use group fields
         */
        static final class SingleLFD extends DocGroups {
            private final LineFileDocs.LineFileDoc lfd;

            SingleLFD(LineFileDocs.LineFileDoc lfd) {
                this.lfd = lfd;
            }

            @Override
            public BytesRef getGroupId() {
                throw new UnsupportedOperationException("We're not indexing groups");
            }

            @Override
            public LineFileDocs.LineFileDoc getNextLFD() {
                return lfd;
            }

            @Override
            public int getNumOfDocsInGroup() {
                return 1;
            }

            @Override
            public int getRemainingNumGroups() {
                return lfd.remainingDocs();
            }
        }

        static final class TextBased extends DocGroups {
            private final BytesRef groupId;
            private final LineFileDocs.LineFileDoc[] lfdArray;
            private int cursor;

            TextBased(BytesRef groupId, LineFileDocs.LineFileDoc[] lfdArray) {
                this.groupId = groupId;
                this.lfdArray = lfdArray;
            }

            @Override
            public BytesRef getGroupId() {
                return groupId;
            }

            @Override
            public LineFileDocs.LineFileDoc getNextLFD() {
                return lfdArray[cursor++];
            }

            @Override
            public int getNumOfDocsInGroup() {
                return lfdArray.length;
            }

            @Override
            public int getRemainingNumGroups() {
                if (cursor == lfdArray.length) {
                    return 0;
                }
                return 1;
            }
        }
    }
}
