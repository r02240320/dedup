package dedup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class SimpleScanner implements DuplicateScanner {

    @Override
    public Collection<Collection<Path>> scan(Collection<Path> dirs) {

        var files = new LinkedList<Path>();
        getPotentialDuplicates(dirs, files);

        // For each bucket, calculate the checksums
        var result = createResultMap();
        var task = getFileChecksumTask(result);
        files.forEach(task);

        // Filter out buckets that have single files and return the rest
        var duplicates = result.values();
        duplicates.removeIf(l -> l.size() == 1);
        return duplicates;
    }

    @Override
    public Collection<Path> getPotentialDuplicates(Collection<Path> dirs, Collection<Path> result) {
        for (var d : Set.copyOf(Objects.requireNonNull(dirs))) {
            try {
                new SimpleFileWalker(result, d).walk();
            } catch (IOException ex) {
            }
        }
        return result;
    }

    private Consumer<Path> getFileChecksumTask(Map<Long, Collection<Path>> resultSet) {
        return (file) -> {
            try {
                long sum = FileComparator.checksumFor(file);
                if (!resultSet.containsKey(sum)) {
                    resultSet.put(sum, createResultBucket());
                }
                resultSet.get(sum).add(file);
            } catch (IOException ioe) {
            }
        };
    }

    private Map<Long, Collection<Path>> createResultMap() {
        return new HashMap<>();
    }

    private Collection<Path> createResultBucket() {
        return new LinkedList<>();
    }

}
