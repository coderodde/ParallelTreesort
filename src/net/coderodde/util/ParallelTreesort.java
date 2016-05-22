package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;

/**
 * This class implements a parallel tree sort. The algorithm splits the input 
 * range into <tt>P</tt> subarrays, where <tt>P</tt> is the amount of cores on 
 * the system, merges all the tree and performs an in-order walk copying the 
 * keys into the input array.
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (May 21, 2016)
 */
public class ParallelTreesort {

    private static final int MINIMUM_THREAD_WORKLOAD = 1 << 16;
    
    public static void sort1(final int[] array) {
        sort1(array, 0, array.length);
    }
    
    public static void sort1(final int[] array, 
                             final int fromIndex, 
                             final int toIndex) {
        final int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            // Trivially sorted.
            return;
        }
        
        final int machineParallelism = Runtime.getRuntime()
                                              .availableProcessors();
        final int commitThreads = Math.max(
                rangeLength / MINIMUM_THREAD_WORKLOAD, 1);
        
        final int numberOfThreads = Math.min(machineParallelism, commitThreads);
        
          /////////////////////////////
         //// Tree building stage ////
        /////////////////////////////
        final TreeBuilderThread[] treeBuilderThreads = 
          new TreeBuilderThread[numberOfThreads];
        
        int tmpFromIndex = fromIndex;
        final int chunkLength = rangeLength / numberOfThreads;
        
        // Spawn all but the last tree building threads.
        for (int i = 0; i < treeBuilderThreads.length - 1; ++i) {
            treeBuilderThreads[i] = 
                    new TreeBuilderThread(array,
                                          tmpFromIndex,
                                          tmpFromIndex += chunkLength);
            treeBuilderThreads[i].start();
        }
        
        // Run the last tree building thread in current thread while others are on
        // their way.
        treeBuilderThreads[treeBuilderThreads.length - 1] = 
                new TreeBuilderThread(array, tmpFromIndex, toIndex);
        
        treeBuilderThreads[treeBuilderThreads.length - 1].run();
        
        for (int i = 0; i < treeBuilderThreads.length - 1; ++i) {
            try {
                treeBuilderThreads[i].join();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(
                        "The " + treeBuilderThreads[i].getClass().getName() + 
                        " " + treeBuilderThreads[i].getName() +
                        " threw an " + ex.getClass().getSimpleName(), ex);
            }
        }
        
        final TreeNode[] rootArray = new TreeNode[treeBuilderThreads.length];
        
        for (int i = 0; i < rootArray.length; ++i) {
            rootArray[i] = treeBuilderThreads[i].getRoot();
        }
        
        new TreeMergerThread(rootArray).run();
        
        TreeNode node = rootArray[0].minimum();
        int index = fromIndex;
        
        while (node != null) {
            final int key   = node.key;
            final int count = node.count;
            
            for (int i = 0; i < count; ++i) {
                array[index++] = key;
            }
            
            node = node.successor();
        }
    }
    
    private static final class TreeBuilderThread extends Thread
    implements Comparable<TreeBuilderThread> {
        
        private final int[] array;
        private final int fromIndex;
        private final int toIndex;
        private TreeNode root;
        private final HashTableEntry[] table;
        private final int mask;
        private final int rangeLength;
        private final Random random = new Random();
        private int treeSize = 1; // Count the root in advance.
        
        TreeBuilderThread(final int[] array,
                          final int fromIndex,
                          final int toIndex) {
            this.array       = array;
            this.fromIndex   = fromIndex;
            this.toIndex     = toIndex;
            this.rangeLength = toIndex - fromIndex;
            
            final int tableCapacity = fixCapacity(rangeLength);
            
            this.mask = tableCapacity - 1;
            this.table = new HashTableEntry[tableCapacity];
        }
        
        @Override
        public void run() {
            // Shuffle the array in order to make sure that the result tree
            // has (in average) logarithmic height.
            for (int i = fromIndex; i < toIndex; ++i) {
                int randomIndex = fromIndex + random.nextInt(rangeLength);
                swap(array, i, randomIndex);
            }
            
            final int initialKey = array[fromIndex];
            root = new TreeNode(initialKey);
            table[getHashTableIndex(initialKey)] = 
                    new HashTableEntry(initialKey, root, null);
            
            for (int i = fromIndex + 1; i < toIndex; ++i) {
                final int currentArrayComponent = array[i];
                final int hashTableIndex = 
                        getHashTableIndex(currentArrayComponent);
                final HashTableEntry entry = findEntry(currentArrayComponent, 
                                                       table[hashTableIndex]);
                
                if (entry != null) {
                    // 'currentArrayComponent' already appears in the tree,
                    // just increment the counter.
                    entry.treeNode.count++;
                } else {
                    // We need to create a new tree node for 
                    // 'currentArrayComponent', and a hash table entry pointing
                    // to it.
                    final TreeNode newNode =
                            new TreeNode(currentArrayComponent);
                    
                    table[hashTableIndex] =
                            new HashTableEntry(currentArrayComponent,
                                               newNode,
                                               table[hashTableIndex]);
                    
                    // Insert 'newNode' into the current tree.
                    insertTreeNode(newNode);
                    treeSize++;
                }
            }
        }

        // The definition of this method implies that the thread with the
        // largest tree will end up at the beginning of the thread array.
        @Override
        public int compareTo(TreeBuilderThread o) {
            return Integer.compare(o.treeSize, this.treeSize);
        }
        
        private static void swap(final int[] array, 
                                 final int index1, 
                                 final int index2) {
            final int tmp = array[index1];
            array[index1] = array[index2];
            array[index2] = tmp;
        }
        
        private HashTableEntry 
            findEntry(final int key, final HashTableEntry collisionChainHead) {
            HashTableEntry currentEntry = collisionChainHead;
            
            while (currentEntry != null && currentEntry.key != key) {
                currentEntry = currentEntry.next;
            }
            
            return currentEntry;
        }
        
        private int getHashTableIndex(final int key) {
            return key & mask;
        }
        
        TreeNode getRoot() {
            return root;
        }
        
        private static int fixCapacity(final int rangeLength) {
            int ret = 1;
            
            while (ret < rangeLength) {
                ret <<= 1;
            }
            
            return ret;
        }
        
        private void insertTreeNode(final TreeNode node) {
            final int key = node.key;
            
            TreeNode current = root;
            TreeNode parentOfCurrent = null;
            
            while (current != null) {
                parentOfCurrent = current;
                
                if (key < current.key) {
                    current = current.left;
                } else {
                    // We don't check 'key > current.key' as there is not risk
                    // of duplicates in the tree.
                    current = current.right;
                }
            }
            
            if (key < parentOfCurrent.key) {
                parentOfCurrent.left = node;
            } else {
                parentOfCurrent.right = node;
            }
            
            node.parent = parentOfCurrent;
        }
    }
    
    private static final class TreeMergerThread extends Thread {
        
        private final TreeNode[] rootArray;
        private final int fromIndex;
        private final int toIndex;
        
        TreeMergerThread(final TreeNode[] rootArray,
                         final int fromIndex,
                         final int toIndex) {
            this.rootArray = rootArray;
            this.fromIndex = fromIndex;
            this.toIndex   = toIndex;
        }
        
        TreeMergerThread(final TreeNode[] rootArray) {
            this(rootArray, 0, rootArray.length);
        }
        
        @Override
        public void run() {
            final int rangeLength = toIndex - fromIndex;
            
            if (rangeLength == 1) {
                return;
            }
            
            if (rangeLength == 2) {
                mergeTrees(rootArray[fromIndex], rootArray[fromIndex + 1]);
                return;
            }
            
            final int middle = fromIndex + (rangeLength) / 2;
            
            TreeMergerThread[] threads = new TreeMergerThread[] {
                new TreeMergerThread(rootArray, fromIndex, middle),
                new TreeMergerThread(rootArray, middle, toIndex),
            };
            
            threads[0].start();
            threads[1].run();
            
            try {
                threads[0].join();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(
                        this.getClass().getSimpleName() + " threw an " +
                        "exception " + ex.getClass().getSimpleName() + ": " +
                        ex.getMessage());
            }
            
            mergeTrees(rootArray[fromIndex], rootArray[middle]);
        }
    }
    
    private static final class TreeNode {
        
        TreeNode left;
        TreeNode right;
        TreeNode parent;
        
        final int key;
        int count;
        
        TreeNode(final int key) {
            this.key = key;
            this.count = 1;
        }
        
        TreeNode minimum() {
            TreeNode minimumNode = this;
            
            while (minimumNode.left != null) {
                minimumNode = minimumNode.left;
            }
            
            return minimumNode;
        }
        
        TreeNode successor() {
            if (this.right != null) {
                return this.right.minimum();
            }
            
            TreeNode parentNode = this.parent;
            TreeNode currentNode = this;
            
            while (parentNode != null && parentNode.right == currentNode) {
                currentNode = parentNode;
                parentNode  = parentNode.parent;
            }
            
            return parentNode;
        }
        
        void insert(final TreeNode node) {
            TreeNode currentNode = this;
            TreeNode parentNode = null;
            final int key = node.key;
            
            while (currentNode != null) {
                final int currentNodeKey = currentNode.key;
                
                if (key < currentNodeKey) {
                    parentNode = currentNode;
                    currentNode = currentNode.left;
                } else if (currentNodeKey < key) {
                    parentNode = currentNode;
                    currentNode = currentNode.right;
                } else {
                    currentNode.count += node.count;
                    return;
                }
            }
            
            if (key < parentNode.key) {
                parentNode.left = node;
            } else {
                parentNode.right = node;
            }
            
            node.parent = parentNode;
        }
    }
    
    private static final class HashTableEntry {
        int key;
        TreeNode treeNode;
        HashTableEntry next;
        
        HashTableEntry(final int key,
                       final TreeNode treeNode,
                       final HashTableEntry next) {
            this.key = key;
            this.treeNode = treeNode;
            this.next = next;
        }
    }
    
    /**
     * Merges the tree starting from {@code root2} to the tree starting from
     * {@code root1}.
     * 
     * @param root1 the root of the tree to which we merge.
     * @param root2 the root of the tree that is being merged to the first tree.
     */
    private static void mergeTrees(final TreeNode root1, final TreeNode root2) {
        mergeTreesImpl(root1, root2);
    }
    
    private static void mergeTreesImpl(final TreeNode root1, 
                                       final TreeNode node) {
        if (node.left != null) {
            mergeTreesImpl(root1, node.left);
        }
        
        if (node.right != null) {
            mergeTreesImpl(root1, node.right);
        }
        
        root1.insert(node);
    }
    
    private static final class Warmup {
        static final int ITERATIONS = 100;
        static final int ARRAY_LENGTH = 500_000;
        static final int MINIMUM_VALUE = -100;
        static final int MAXIMUM_VALUE = 100;
    }
    
    private static final class Demo {
        static final int ARRAY_LENGTH = 2_000_000;
        static final int MINIMUM_VALUE = -1000;
        static final int MAXIMUM_VALUE = 10_000_000;
        static final int FROM_INDEX = 10;
        static final int TO_INDEX = ARRAY_LENGTH - 15;
    }
    
    public static void main(final String... args) {
        final long seed = System.nanoTime();
        final Random random = new Random(seed);
        final int[] array1 = random.ints(Demo.ARRAY_LENGTH, 
                                         Demo.MINIMUM_VALUE, 
                                         Demo.MAXIMUM_VALUE).toArray();
        final int[] array2 = array1.clone();
        
        System.out.println("[STATUS] Warming up...");
        warmup(random);
        System.out.println("[STATUS] Warming up done.");
        
        System.out.println("Seed = " + seed);
        
        long startTime = System.nanoTime();
        sort1(array1, Demo.FROM_INDEX, Demo.TO_INDEX);
        long endTime = System.nanoTime();
        
        System.out.printf("ParallelTreesort in %.0f milliseconds.\n",
                          (endTime - startTime) / 1e6);
        
        startTime = System.nanoTime();
        Arrays.parallelSort(array2, Demo.FROM_INDEX, Demo.TO_INDEX);
        endTime = System.nanoTime();
        
        System.out.printf("Arrays.parallelSort in %.0f milliseconds.\n",
                          (endTime - startTime) / 1e6);
        
        System.out.println("Algorithms agree: " + Arrays.equals(array1,
                                                                array2));
    }    
    
    private static void warmup(final Random random) {
        for (int i = 0; i < Warmup.ITERATIONS; ++i) {
            final int[] array1 = random.ints(Warmup.ARRAY_LENGTH,
                                             Warmup.MINIMUM_VALUE, 
                                             Warmup.MAXIMUM_VALUE).toArray();
            
            final int[] array2 = array1.clone();
            
            ParallelTreesort.sort1(array1);
            Arrays.parallelSort(array2);
        }
    }
}
