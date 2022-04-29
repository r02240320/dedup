package dedup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;

public class DuplicateFinder {

    public Collection<Collection<Path>> getDuplicates(Path dir, boolean threaded) throws IOException {

        var values = threaded ? parsePathThreaded(dir).values() : parsePath(dir).values();
        values.removeIf(list -> list.size() == 1);

        // For each set, calculate checksums and bucket them accordingly
        var result = createResultMap();
        var task = getFileListTask(result);
        if (threaded)
            values.parallelStream().forEach(task);
        else
            values.forEach(task);

        // Filter out buckets that have single files and return the rest
        values = result.values();
        values.removeIf(l -> l.size() == 1);
        return values;
    }

    protected Map<Long, Collection<Path>> parsePath(Path dir) throws IOException {
        var fileMap = createResultMap();
        new FileLister(fileMap, dir).walkDir();
        return fileMap;
    }

    protected Map<Long, Collection<Path>> parsePathThreaded(Path dir) throws IOException {
        var fileMap = createResultMap();
        var pendingDirs = new ConcurrentLinkedQueue<Path>();
        new FileListerThreaded(fileMap, dir, pendingDirs).walkDir();

        while (!pendingDirs.isEmpty()) {
            var mapper = getFileWalkTask(fileMap, pendingDirs);
            pendingDirs.stream().parallel().forEach(mapper);
        }

        return fileMap;
    }

    private Consumer<Path> getFileWalkTask(Map<Long, Collection<Path>> fileMap, Collection<Path> pendingDirs) {
        return (Path path) -> {
            try {
                pendingDirs.remove(path);
                new FileListerThreaded(fileMap, path, pendingDirs).walkDir();
            } catch (IOException ioe) {
                // For now just ignore any errors, alternatively log them
            }
        };
    }

    private Consumer<Collection<Path>> getFileListTask(Map<Long, Collection<Path>> result) {
        return (list) -> {
            var task = getFileChecksumTask(result);
            list.stream().parallel().forEach(task);
        };
    }

    private Consumer<Path> getFileChecksumTask(Map<Long, Collection<Path>> resultSet) {
        return (file) -> {
            try {
                long sum = FileChecksum.checksumFor(file);
                if (!resultSet.containsKey(sum)) {
                    resultSet.put(sum, createResultBucket());
                }
                resultSet.get(sum).add(file);
            } catch (IOException ioe) {
                // Ignore errors for now
            }
        };
    }

    private Map<Long, Collection<Path>> createResultMap() {
        return new ConcurrentHashMap<>();
    }

    private Collection<Path> createResultBucket() {
        return new ConcurrentLinkedQueue<>();
    }

}
