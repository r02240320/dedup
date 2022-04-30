package dedup;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

/**
 * This uses a simple directory walker for enumerating files but
 * uses multi-threading for checksum calculations
 */
public class HybridScanner extends ThreadedScanner {

    @Override
    public Collection<Path> getPotentialDuplicates(Collection<Path> dirs, Collection<Path> result) {
        return scanSameSizeFiles(dirs, result);
    }

    @Override
    protected void startTraversal() {
        var resultset = getChecksumItems();
        var folders = getPendingFolderItems();
        scanSameSizeFiles(folders, resultset);
        folders.clear();
    }

    private Collection<Path> scanSameSizeFiles(Collection<Path> dirs, Collection<Path> result) {
        for (var d : Set.copyOf(dirs)) {
            try {
                new SimpleFileWalker(result, d).walk();
            } catch (IOException ex) {
            }
        }
        return result;
    }

}
