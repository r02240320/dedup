package dedup;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/**
 * The same as the parent class but uses Threads to process the file entries.
 */
public class FileListerThreaded extends FileLister {

    private Collection<Path> pendingList;

    /**
     * 
     * @param fileMap     The map to save the results into
     * @param baseDir     The folder to start the scanning from
     * @param pendingList The collection to add directories that are to be traversed
     * @throws IOException
     */
    public FileListerThreaded(Map<Long, Collection<Path>> fileMap, Path baseDir, Collection<Path> pendingList)
            throws IOException {
        super(fileMap, baseDir);
        this.pendingList = Objects.requireNonNull(pendingList);
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (dir.equals(getBaseDir())) {
            return FileVisitResult.CONTINUE;
        }
        pendingList.add(dir);
        return FileVisitResult.SKIP_SUBTREE;
    }

}
