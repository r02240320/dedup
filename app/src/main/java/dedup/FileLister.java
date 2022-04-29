package dedup;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This class traverses a given directory and collects the files found
 * therein.
 */
public class FileLister implements FileVisitor<Path> {

    private Path baseDir;

    private Map<Long, Collection<Path>> fileMap;

    /**
     * 
     * @param fileMap The map to save the results into
     * @param baseDir The folder to start the scanning from
     * @throws IOException
     */
    public FileLister(Map<Long, Collection<Path>> fileMap, Path baseDir) throws IOException {
        this.fileMap = Objects.requireNonNull(fileMap);
        this.baseDir = Objects.requireNonNull(baseDir);
        if (!Files.isDirectory(baseDir))
            throw new IOException("Invalid argument, expected a directory parameter");
    }

    public void walkDir() throws IOException {
        Files.walkFileTree(baseDir, this);
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        if (size > 0) {
            if (!fileMap.containsKey(size)) {
                fileMap.put(size, new ConcurrentLinkedQueue<>());
            }
            fileMap.get(size).add(file);
        }

        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        // Just ignore file system errors for now
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    protected Path getBaseDir() {
        return baseDir;
    }

}
