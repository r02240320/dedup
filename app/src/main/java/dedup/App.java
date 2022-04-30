package dedup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class App {

    public static void main(String[] args) throws IOException, InterruptedException {
        var dirs = getDirectory(args);

        // ========================== FILE WALKERS TEST ========================
        testWalker(dirs, new SimpleScanner(), "SIMPLE");
        testWalker(dirs, new HybridScanner(), "HYBRID");
        testWalker(dirs, new ThreadedScanner(), "THREADED");

        // ========================== FILE WALKERS TEST ========================
        testScanner(dirs, new SimpleScanner(), "SIMPLE");
        testScanner(dirs, new HybridScanner(), "HYBRID");
        testScanner(dirs, new ThreadedScanner(), "THREADED");

    }

    static Collection<Path> getDirectory(String[] args) {
        if (args == null || args.length == 0) {
            var roots = File.listRoots();
            var paths = new ArrayList<Path>(roots.length);
            for (var f : roots)
                paths.add(f.toPath());
            return paths;
        } else {
            var paths = new ArrayList<Path>(args.length);
            for (var f : args) {
                var p = Path.of(f);
                if (Files.exists(p) && Files.isDirectory(p))
                    paths.add(p);
            }
            return paths;
        }
    }

    static void testScanner(Collection<Path> dirs, DuplicateScanner drv, String name) {
        var start = System.nanoTime();
        var res = drv.scan(dirs);
        var dur = Duration.ofNanos(System.nanoTime() - start);
        printDuplicateResults(res, dur, name);
    }

    static void testWalker(Collection<Path> dirs, DuplicateScanner drv, String name) {
        var start = System.nanoTime();
        var res = drv.getPotentialDuplicates(dirs, new ConcurrentLinkedQueue<>());
        var dur = Duration.ofNanos(System.nanoTime() - start);
        printTraverseSummary(res, dur, name);
    }

    static void printTraverseSummary(Collection<Path> fileMap, Duration duration, String type) {
        var fmt = "The %s scanner run took: %s to go through %s files";
        System.out.println(fmt.formatted(type, duration, fileMap.size()));
    }

    static void printDuplicateResults(Collection<Collection<Path>> list, Duration duration, String type) {
        var fmt = "%s Duplicate finder took: %s and found: %d duplicates\n";
        System.out.println(String.format(fmt, type, duration, list.size()));

        if (list.size() <= 20) {
            int i = 0;
            for (var col : list) {
                ++i;
                System.out.print(String.format("List: %d has %d duplicates!", i, col.size()));
                var items = col.stream().map(Path::toString).toArray(String[]::new);
                var str = String.join("\n\t", items);
                System.out.print(String.format("\n\t%s", str));
                System.out.println();
            }
        }
    }

}
