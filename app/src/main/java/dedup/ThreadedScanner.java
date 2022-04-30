package dedup;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * This class drives the whole logic in this project
 */
public class ThreadedScanner implements DuplicateScanner {

    private Data data;

    private Queue<ThreadedFileWalker> workerPool = new ConcurrentLinkedQueue<>();

    public ThreadedScanner() {
        data = new Data();
        data.PENDING_FOLDERS = new LinkedBlockingQueue<>();
        data.CHECKSUM_ITEMS = new LinkedBlockingQueue<>();
        data.SIZE_MAP = new ConcurrentHashMap<>();
        data.HASH_MAP = new ConcurrentHashMap<>();
        data.KNOWN_SIZES = new HashSet<>();
    }

    @Override
    public Collection<Collection<Path>> scan(Collection<Path> dirs) {

        // Filter out non-directory arguments
        Objects.requireNonNull(dirs);
        for (var d : Set.copyOf(dirs)) {
            if (Files.isDirectory(d))
                data.PENDING_FOLDERS.add(d);
        }

        // Start threads to handle the traversal and checksum calc
        var threads = Executors.newFixedThreadPool(2);
        threads.execute(this::startTraversal);
        threads.execute(this::startChecksum);
        while (hasWork()) {
        }

        // Cleanup the worker threads
        try {
            threads.shutdown();
            if (!threads.awaitTermination(3, TimeUnit.SECONDS)) {
                threads.shutdownNow();
            }
        } catch (InterruptedException iex) {
        }

        // Return the results
        var vals = data.HASH_MAP.values();
        vals.removeIf(l -> l.size() == 1);
        return vals;
    }

    @Override
    public Collection<Path> getPotentialDuplicates(Collection<Path> dirs, Collection<Path> result) {
        Objects.requireNonNull(dirs);
        for (var d : Set.copyOf(dirs)) {
            if (Files.isDirectory(d))
                data.PENDING_FOLDERS.add(d);
        }

        // Start threads to handle the traversal
        var threads = Executors.newFixedThreadPool(1);
        threads.execute(this::startTraversal);
        while (data.PENDING_FOLDERS.size() > 0) {
        }

        // Cleanup the worker threads
        try {
            threads.shutdown();
            if (!threads.awaitTermination(3, TimeUnit.SECONDS)) {
                threads.shutdownNow();
            }
        } catch (InterruptedException iex) {
        }

        return data.CHECKSUM_ITEMS;
    }

    protected void startTraversal() {
        Consumer<Path> folderWalker = (dir) -> {
            var worker = getWorker(dir);
            try {
                worker.walk();
            } catch (IOException ioe) {
            } finally {
                addWorkerToPool(worker);
                data.PENDING_FOLDERS.remove(dir);
            }
        };

        while (hasFolderTasks()) {
            data.PENDING_FOLDERS.parallelStream().forEach(folderWalker);
        }
    }

    protected Collection<Path> getPendingFolderItems() {
        return data.PENDING_FOLDERS;
    }

    protected Collection<Path> getChecksumItems() {
        return data.CHECKSUM_ITEMS;
    }

    private ThreadedFileWalker getWorker(Path baseDir) {
        var worker = workerPool.poll();
        if (worker != null) {
            return worker.setDirectory(baseDir);
        }
        return new ThreadedFileWalker(data, baseDir);
    }

    private void addWorkerToPool(ThreadedFileWalker worker) {
        workerPool.add(worker);
    }

    private void startChecksum() {
        Consumer<Path> checksumCalc = (dir) -> {
            try {
                FileComparator.calculateChecksum(dir, data.HASH_MAP);
            } catch (IOException ex) {
            } finally {
                data.CHECKSUM_ITEMS.remove(dir);
            }
        };

        while (hasWork()) {
            data.CHECKSUM_ITEMS.parallelStream().forEach(checksumCalc);
        }
    }

    private boolean hasWork() {
        return data.PENDING_FOLDERS.size() > 0 || data.CHECKSUM_ITEMS.size() > 0;
    }

    private boolean hasFolderTasks() {
        return data.PENDING_FOLDERS.size() > 0;
    }

    static class Data {

        /**
         * A common object to be used for synchronization between threads on some
         * code sections that are thread critical.
         */
        static Object SYNC = new Object();

        /**
         * Thread safe map to gather the mapping between a file hash and name
         */
        Map<Long, Collection<Path>> HASH_MAP;

        /**
         * Thread safe Collection to hold folders that are yet to be parsed
         */
        Collection<Path> PENDING_FOLDERS;

        /**
         * Thread safe map to hold items waiting for their checksum to
         * be calculated
         */
        Collection<Path> CHECKSUM_ITEMS;

        /**
         * Thread safe map to map file sizes to a file name.
         * As soon as another entry is about to be inserted, both the existing
         * and new entries will be moved to the CHECKSUM_ITEMS queue and their
         * key moved to a simpler Set for future lookups.
         */
        Map<Long, Path> SIZE_MAP;

        /**
         * Thread safe Set to hold all file sizes that have been encountered
         * and safe memory by forwarding it straight to the CHECKSUM_ITEMS queue
         * ready for checksum calculation.
         */
        Set<Long> KNOWN_SIZES;

    }

}
