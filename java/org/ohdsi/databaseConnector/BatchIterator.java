package org.ohdsi.databaseConnector;

/**
 * This class helps to break the array into partitions. You can iterate using hasNext/getNext methods to get first/last element index inside each partition
 * This is a straightforward implementation of Iterator pattern, but it does not implement Iterator interface because it has too much functionality for such a simple case.
 */
public class BatchIterator {
    private final int batchSize;
    private final int totalSize;
    private final int batchAmount;
    private int currentPage;

    public BatchIterator(int batchSize, int count) {

        this.batchSize = batchSize;
        this.totalSize = count;
        this.batchAmount = getPageSize(count, batchSize);
    }

    public boolean hasNext() {

        return currentPage < batchAmount;
    }

    public BatchData getNext() {

        if (!hasNext()) {
            return null;
        }
        int firstElementIndex = currentPage * batchSize;
        currentPage = currentPage + 1;
        return new BatchData(
                firstElementIndex,
                getSize(firstElementIndex)
        );
    }

    private int getPageSize(int count, int batchSize) {

        if (count % batchSize == 0) {
            return count / batchSize;
        }
        return count / batchSize + 1;

    }

    private int getSize(int firstElement) {

        int sizeResult = totalSize - firstElement;
        if (sizeResult > batchSize) {
            return batchSize;
        }
        return sizeResult;
    }


    class BatchData {
        private int firstElementIndex;
        private int size;

        public BatchData(int firstElementIndex, int size) {

            this.firstElementIndex = firstElementIndex;
            this.size = size;
        }

        public int getFirstElementIndex() {

            return firstElementIndex;
        }

        public int getLastElementIndex() {

            return firstElementIndex + size - 1;
        }

        public int getSize() {

            return size;
        }
    }

}
