import java.util.*;

public class QueueProcessor {
    // Function to add a RedisLock array as a single item to the queue
    public static Queue<RedisLock[]> addArrayAsSingleItemToQueue(RedisLock[] inputArray) {
        Queue<RedisLock[]> queue = new LinkedList<>();
        queue.add(inputArray); // Add the entire array as a single item
        return queue;
    }

    // Synchronized function to retrieve subsets and remove them from the queue
    public static synchronized List<RedisLock[]> getAndRemoveSubsets(Queue<RedisLock[]> queue) {
        if (queue.isEmpty()) {
            return Collections.emptyList(); // Return empty list if queue is empty
        }

        // Retrieve the first item
        RedisLock[] firstItem = queue.poll();
        if (firstItem == null) {
            return Collections.emptyList(); // Queue might have been emptied by another thread
        }

        // Prepare a result list for matching subsets
        List<RedisLock[]> matchingSubsets = new ArrayList<>();
        matchingSubsets.add(firstItem); // Include the first item itself

        // Use an iterator to safely remove items while iterating
        Iterator<RedisLock[]> iterator = queue.iterator();
        while (iterator.hasNext()) {
            RedisLock[] item = iterator.next();
            if (isSubset(firstItem, item)) { // Check if it's a subset of the first item
                matchingSubsets.add(item);
                iterator.remove(); // Remove from the queue
            }
        }

        return matchingSubsets;
    }

    // Helper function to check if one array is a subset of another
    private static boolean isSubset(RedisLock[] parent, RedisLock[] child) {
        Set<RedisLock> parentSet = new HashSet<>(Arrays.asList(parent));
        return parentSet.containsAll(Arrays.asList(child));
    }

    public static void main(String[] args) {
        // Example usage
        Queue<RedisLock[]> queue = new LinkedList<>();
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2")});
        queue.add(new RedisLock[]{new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1")});

        // Thread-safe retrieval and removal
        List<RedisLock[]> subsets = getAndRemoveSubsets(queue);

        // Print the subsets
        System.out.println("Matching subsets:");
        for (RedisLock[] subset : subsets) {
            System.out.println(Arrays.toString(subset));
        }

        // Print remaining queue
        System.out.println("Remaining queue:");
        for (RedisLock[] remainingItem : queue) {
            System.out.println(Arrays.toString(remainingItem));
        }
    }
}
