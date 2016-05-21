package net.coderodde.util;

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
    
    public static void sort2(final int[] array) {
        sort2(array, 0, array.length);
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
        
        final TreeBuilderThread[] treeBuilderThreads = 
          new TreeBuilderThread[numberOfThreads];
        
          /////////////////////////////
         //// Tree building stage ////
        /////////////////////////////
        int tmpFromIndex = fromIndex;
        final int chunkLength = rangeLength / numberOfThreads;
        
        // Spawn all but the last shuffle threads.
        for (int i = 0; i < treeBuilderThreads.length - 1; ++i) {
            treeBuilderThreads[i] = 
                    new TreeBuilderThread(array,
                                          tmpFromIndex,
                                          tmpFromIndex += chunkLength);
            treeBuilderThreads[i].start();
        }
        
        // Run the last shuffle thread in current thread while others are on
        // their way.
        new TreeBuilderThread(array, tmpFromIndex, toIndex).run();
        
        for (int i = 0; i < treeBuilderThreads.length - 1; ++i) {
            try {
                treeBuilderThreads[i].join();
            } catch (final InterruptedException ex) {
                throw new IllegalStateException(
                        "The ShuffleThread " + treeBuilderThreads[i].getName() +
                        " threw an " + ex.getClass().getSimpleName(), ex);
            }
        }
    }
    
    public static void sort2(final int[] array, 
                             final int fromIndex, 
                             final int toIndex) {
        
    }
    
    private static final class TreeBuilderThread extends Thread {
        
        private final int[] array;
        private final int fromIndex;
        private final int toIndex;
        private TreeNode root;
        private final HashTableEntry[] table;
        private final int mask;
        private final int rangeLength;
        private final Random random = new Random();
        
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
                }
            }
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
   
    public static void main(final String... args) {
        final int[] array = new int[] { 3, 1, 7, 4, 5, 9, -1, 0, 3 };
        final TreeBuilderThread t = new TreeBuilderThread(array,0, array.length);
        t.run();
        final TreeNode root = t.getRoot();
        
        System.out.println("hoefsd");
    }    
}
