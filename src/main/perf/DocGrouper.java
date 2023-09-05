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
    protected final int targetNumGroups;
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
    public static BytesRef[] group2M;

    DocGrouper(int numDocs, int targetNumGroups) {
        this.numDocs = numDocs;
        this.targetNumGroups = targetNumGroups;
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
        group2M = randomStrings(2000000, random);
    }

    static int getTargetNumGroups(int numDocs) {
        if (numDocs >= 5000000) {
            return 1_000_000;
        } else if (numDocs >= 500000) {
            return 100_000;
        } else {
            return 10_000;
        }
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
            super(numDocs, 0);
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
            super(numDocs, getTargetNumGroups(numDocs));
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
     * The binary LFD is naturally grouped by the binary blob, so that if we have two groups sharing the same
     * binary blob then the two groups cannot be indexed concurrently.
     * This grouper will produce {@link DocGroups.BinaryBased}, which will contain either:
     * 1. One group, consists of multiple LFD, or
     * 2. One LFD, split into multiple groups
     * Such that each {@link DocGroups.BinaryBased} can be indexed in parallel
     * <br>
     * The algorithm will first calculate average number of documents in the group, then init first budget as the avg
     * value, then tries to follow:
     * 1. If the coming LFD has number of documents within 0.5 * budget and 1.5 * budget, then the LFD sole will form
     *    a group, we will then adjust next budget as: newBudget = 2 * budget - LFD.docNum()
     * 2. If the coming LFD has number of documents less than 0.5 * budget, then we will try to accumulate the next LFDs
     *    until the total number of documents of the accumulated LFD reaches 0.5 * budget and form the group.
     *    The next budget will be adjusted as: newBudget = 2 * budget - total_number_doc_of_the_group
     * 3. If the coming LFD has number of documents larger than 1.5 * budget, then we will evenly divide this LFD into
     *    round(LFD.docNum() / budget) number of groups. Then adjust the next budget as:
     *    newBudget = (num_group + 1) * budget - LFD.docNum()
     *
     * In addition to above rule, we will also calibrate the budget to be within 1 ~ 1.4 range of the initial budget
     * such that we won't generate too big or small groups, and gives a theoretical upper bound number of groups of
     * 2 * target number of groups. (Reality will be much less)
     */
    static final class BinaryGrouper extends DocGrouper {

        private final List<LineFileDocs.LineFileDoc> buffer = new ArrayList<>();
        private final BytesRef[] groupIds;
        private final int avg;
        private int budget;
        private int accumDocNum;
        private int groupCounter;

        BinaryGrouper(int numDocs) {
            super(numDocs, getTargetNumGroups(numDocs));
            assert group100 != null;
            if (numDocs >= 5000000) {
                groupIds = group2M;
            } else if (numDocs >= 500000) {
                groupIds = group1M;
            } else {
                groupIds = group100K;
            }
            avg = numDocs / targetNumGroups;
            budget = avg;
        }

        @Override
        public void add(LineFileDocs.LineFileDoc lfd) throws InterruptedException {
            if (buffer.size() != 0) {
                // case 2
                // we previously have some smallish document block
                if (lfd != LineFileDocs.END) {
                    buffer.add(lfd);
                    accumDocNum += lfd.remainingDocs();
                }
                if (accumDocNum >= 0.5 * budget || lfd == LineFileDocs.END) {
                    outputQueue.put(new DocGroups.BinaryBased(
                            buffer.toArray(new LineFileDocs.LineFileDoc[0]),
                            new BytesRef[]{groupIds[groupCounter++]},
                            new int[] {accumDocNum}));
                    adjustBuffer(accumDocNum, 1);
                    reset();
                }
            } else {
                if (lfd == LineFileDocs.END) {
                    outputQueue.put(END);
                    return;
                }
                if (lfd.remainingDocs() >= 0.5 * budget && lfd.remainingDocs() <= 1.5 * budget) {
                    // case 1
                    outputQueue.put(new DocGroups.BinaryBased(
                            new LineFileDocs.LineFileDoc[]{lfd},
                            new BytesRef[]{groupIds[groupCounter++]},
                            new int[] {lfd.remainingDocs()}));
                    adjustBuffer(lfd.remainingDocs(), 1);
                } else if (lfd.remainingDocs() < 0.5 * budget) {
                    // case 2, accumulate but not form a group until we have enough documents
                    buffer.add(lfd);
                    accumDocNum += lfd.remainingDocs();
                } else {
                    // case 3
                    int numGroups = lfd.remainingDocs() / budget;
                    if (lfd.remainingDocs() % budget >= 0.5 * budget) {
                        numGroups++;
                    }
                    int remainder = lfd.remainingDocs() % numGroups;
                    int base = lfd.remainingDocs() / numGroups;
                    BytesRef[] nextGroupIds = new BytesRef[numGroups];
                    int[] numDocs = new int[numGroups];
                    for (int i = 0; i < numGroups; i++) {
                        nextGroupIds[i] = groupIds[groupCounter++];
                        numDocs[i] = base;
                        if (remainder > 0) {
                            numDocs[i]++;
                            remainder--;
                        }
                    }
                    outputQueue.put(new DocGroups.BinaryBased(
                            new LineFileDocs.LineFileDoc[]{lfd},
                            nextGroupIds,
                            numDocs
                    ));
                }
            }
        }

        private void adjustBuffer(int lastAccumDocNum, int groupNum) {
            budget = groupNum * budget - lastAccumDocNum;
            if (budget < avg) {
                budget = avg;
            } else if (budget > avg * 1.4) {
                budget = (int) (avg * 1.4);
            }
        }

        private void reset() {
            buffer.clear();
            accumDocNum = 0;
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

        static final class BinaryBased extends DocGroups {

            private final LineFileDocs.LineFileDoc[] lfdArray;
            private final BytesRef[] groupIds;
            private final int[] numDocsPerGroup;
            private int consumedDocNumInOneGroup;
            private int groupIdx;
            private int lfdIdx;

            BinaryBased(LineFileDocs.LineFileDoc[] lfdArray, BytesRef[] groupIds, int[] numDocsPerGroup) {
                assert lfdArray.length == 1 || groupIds.length == 1;
                assert groupIds.length == numDocsPerGroup.length;

                this.numDocsPerGroup = numDocsPerGroup;
                this.lfdArray = lfdArray;
                this.groupIds = groupIds;
            }

            @Override
            public BytesRef getGroupId() {
                return groupIds[groupIdx];
            }

            @Override
            public LineFileDocs.LineFileDoc getNextLFD() {
                if (lfdArray[lfdIdx].remainingDocs() <= 0) {
                    if (lfdIdx == lfdArray.length - 1) {
                        throw new IllegalStateException("The group has no more document!");
                    }
                    lfdIdx++;
                }
                consumeDoc();
                assert lfdArray[lfdIdx].remainingDocs() > 0;
                return lfdArray[lfdIdx];
            }

            private void consumeDoc() {
                consumedDocNumInOneGroup++;
                if (consumedDocNumInOneGroup > numDocsPerGroup[groupIdx]) {
                    consumedDocNumInOneGroup = 1;
                    groupIdx++;
                }
            }

            @Override
            public int getNumOfDocsInGroup() {
                return numDocsPerGroup[groupIdx];
            }

            @Override
            public int getRemainingNumGroups() {
                return groupIds.length - groupIdx;
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
