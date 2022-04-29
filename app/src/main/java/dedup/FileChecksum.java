package dedup;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * This class is a utility class to calculate a file's checksum using CRC32.
 * 
 * It can also be used to compare files for equality
 */
public class FileChecksum {

    private static int BUFSIZE = 8192;

    public static long checksumFor(Path file) throws IOException {
        Objects.requireNonNull(file);
        var opts = StandardOpenOption.READ;
        var strm = new BufferedInputStream(Files.newInputStream(file, opts));
        var chckStrm = new CheckedInputStream(strm, new CRC32());
        try (strm; chckStrm) {
            var buf = new byte[BUFSIZE];
            while (chckStrm.read(buf, 0, BUFSIZE) > -1) {
            }

            return chckStrm.getChecksum().getValue();
        }
    }

    public static boolean isSameFile(Path path1, Path path2) throws IOException {
        if (path1 == path2)
            return true;

        Objects.requireNonNull(path1);
        Objects.requireNonNull(path2);
        if (Files.size(path1) != Files.size(path2))
            return false;

        var chk1 = checksumFor(path1);
        var chk2 = checksumFor(path2);
        return chk1 == chk2;
    }

}
