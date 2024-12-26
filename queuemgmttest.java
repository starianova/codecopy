import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

public class QueueProcessorTest {

    @Test
    public void testAddArrayAsSingleItemToQueue() {
        RedisLock[] inputArray = {
            new RedisLock("lock1"), new RedisLock("lock2"), new RedisLock("lock3")
        };
        Queue<RedisLock[]> queue = QueueProcessor.addArrayAsSingleItemToQueue(inputArray);

        assertNotNull(queue, "Queue should not be null");
        assertEquals(1, queue.size(), "Queue size should be 1");
        assertArrayEquals(inputArray, queue.peek(), "Queue should contain the input array as the first item");
    }

    @Test
    public void testGetAndRemoveSubsets() {
        // Setup queue with multiple arrays
        Queue<RedisLock[]> queue = new LinkedList<>();
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2")});
        queue.add(new RedisLock[]{new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1")});

        // Call method and validate results
        List<RedisLock[]> subsets = QueueProcessor.getAndRemoveSubsets(queue);

        // Expected results
        List<RedisLock[]> expectedSubsets = List.of(
            new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2"), new RedisLock("lock3")},
            new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2")},
            new RedisLock[]{new RedisLock("lock1")}
        );

        // Validate subsets
        assertNotNull(subsets, "Subsets should not be null");
        assertEquals(expectedSubsets.size(), subsets.size(), "Subsets size should match expected size");
        for (int i = 0; i < subsets.size(); i++) {
            assertArrayEquals(expectedSubsets.get(i), subsets.get(i), "Subset should match expected");
        }

        // Validate remaining queue
        assertEquals(1, queue.size(), "Remaining queue size should be 1");
        RedisLock[] remainingItem = queue.peek();
        RedisLock[] expectedRemaining = new RedisLock[]{new RedisLock("lock2"), new RedisLock("lock3")};
        assertArrayEquals(expectedRemaining, remainingItem, "Remaining item in the queue should match expected");
    }

    @Test
    public void testEmptyQueueGetAndRemoveSubsets() {
        Queue<RedisLock[]> queue = new LinkedList<>();

        List<RedisLock[]> subsets = QueueProcessor.getAndRemoveSubsets(queue);

        assertNotNull(subsets, "Subsets should not be null");
        assertTrue(subsets.isEmpty(), "Subsets should be empty for an empty queue");
    }

    @Test
    public void testQueueThreadSafety() throws InterruptedException {
        Queue<RedisLock[]> queue = new LinkedList<>();
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1"), new RedisLock("lock2")});
        queue.add(new RedisLock[]{new RedisLock("lock2"), new RedisLock("lock3")});
        queue.add(new RedisLock[]{new RedisLock("lock1")});

        // Thread-safe test with multiple threads
        Runnable task = () -> QueueProcessor.getAndRemoveSubsets(queue);

        Thread thread1 = new Thread(task);
        Thread thread2 = new Thread(task);

        thread1.start();
        thread2.start();

        thread1.join();
        thread2.join();

        // Validate the queue is empty or consistent after concurrent operations
        assertTrue(queue.isEmpty() || queue.size() <= 1, "Queue should be empty or consistent after thread-safe access");
    }
}
