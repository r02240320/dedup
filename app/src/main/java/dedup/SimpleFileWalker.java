package dedup;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * This class traverses a given directory and collects the files
 * that share file size with one or more other files
 */
public class SimpleFileWalker implements FileVisitor<Path> {

    /**
     * A structure to keep the mapping between the size of the file
     * and the first Path with that size.
     * When another entry comes along, we retrieve the first value, add it
     * to the checksum list together with the incoming entry.
     * To prevent multiple adding of the first entry into the checksum list,
     * if is marked as true i.e processed once added, see visitFile() method.
     */
    private Map<Long, Entry<Path, Boolean>> sizeMap;

    /**
     * A collection of files that qualify for checksum calculation
     */
    private Collection<Path> checksumItems;

    /**
     * The base directory to start the traversing from
     */
    private Path baseDir;

    /**
     * 
     * @param checksumItems A list to push items that qualify for checksum
     * @param baseDir       The folder to start the scanning from
     * @throws IOException
     */
    public SimpleFileWalker(Collection<Path> checksumItems, Path baseDir) throws IOException {
        this.checksumItems = Objects.requireNonNull(checksumItems);
        this.baseDir = Objects.requireNonNull(baseDir);
        this.sizeMap = new HashMap<>();
        if (!Files.isDirectory(baseDir))
            throw new IOException("Invalid argument, expected a directory parameter");
    }

    public Collection<Path> walk() throws IOException {
        Files.walkFileTree(baseDir, this);
        return checksumItems;
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        long size = attrs.size();
        if (size > 0) {
            if (!sizeMap.containsKey(size)) {
                var v = new SimpleEntry<Path, Boolean>(file, false);
                sizeMap.put(size, v);
            } else {
                var v = sizeMap.get(size);
                if (!v.getValue()) {
                    checksumItems.add(v.getKey());
                    v.setValue(true);
                }
                checksumItems.add(file);
            }
        }
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        return FileVisitResult.CONTINUE;
    }

}
