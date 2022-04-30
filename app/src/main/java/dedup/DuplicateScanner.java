package dedup;

import java.nio.file.Path;
import java.util.Collection;

public interface DuplicateScanner {

    /**
     * This method should traverse the FileSystem and return a Collection
     * of duplicates.
     * Each entry in the returned Collection should be a collection of Paths
     * that point to similar files.
     * 
     * @param dirs a collection of the directories to scan for duplicates
     * @return a collection of duplicates
     */
    public Collection<Collection<Path>> scan(Collection<Path> dirs);

    /**
     * Scans a given list of directories for possible duplicates and save the
     * entries in the given resultset.
     * 
     * @param dirs
     * @param resultset
     * @return
     */
    public Collection<Path> getPotentialDuplicates(Collection<Path> dirs, Collection<Path> resultset);

}
