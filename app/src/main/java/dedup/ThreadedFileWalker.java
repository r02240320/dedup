package dedup;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

public class ThreadedFileWalker implements FileVisitor<Path> {

    private Path baseDir;

    private boolean isDone;

    private ThreadedScanner.Data data;

    public ThreadedFileWalker(ThreadedScanner.Data data, Path baseDir) {
        isDone = false;
        this.data = Objects.requireNonNull(data);
        this.baseDir = Objects.requireNonNull(baseDir);
    }

    public ThreadedFileWalker setDirectory(Path baseDir) {
        if (isDone)
            this.baseDir = Objects.requireNonNull(baseDir);
        isDone = false;
        return this;
    }

    public ThreadedFileWalker walk() throws IOException {
        Files.walkFileTree(baseDir, this);
        return this;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        if (dir.equals(baseDir)) {
            return FileVisitResult.CONTINUE;
        }
        data.PENDING_FOLDERS.add(dir);
        return FileVisitResult.SKIP_SUBTREE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        if (size > 0) {
            addFile(file, size);
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        isDone = true;
        return FileVisitResult.CONTINUE;
    }

    private void addFile(Path entry, long size) {
        if (data.KNOWN_SIZES.contains(size)) {
            data.CHECKSUM_ITEMS.add(entry);
        } else {
            // Only one thread should be moving things about
            synchronized (ThreadedScanner.Data.SYNC) {
                var old = data.SIZE_MAP.putIfAbsent(size, entry);
                if (old != null) {
                    data.CHECKSUM_ITEMS.add(entry);
                    data.CHECKSUM_ITEMS.add(old);
                    data.KNOWN_SIZES.add(size);
                    data.SIZE_MAP.remove(size);
                }
            }
        }
    }

}
