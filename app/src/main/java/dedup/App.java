package dedup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

public class App {

    public static void main(String[] args) throws IOException, InterruptedException {
        var dir = getDirectory(args);
        // compareWalkTimes(dir);
        // findDuplicates(dir, true);

        // =============================== SIMPLE ===============================
        DuplicateScanner drv = new SimpleScanner();
        var start = System.nanoTime();
        var res = drv.scan(dir);
        // var res = drv.getPotentialDuplicates(dir, new LinkedList<>());
        var dur = Duration.ofNanos(System.nanoTime() - start);
        // printRunSummary(res, dur, "SIMPLE");
        printDuplicateResults(res, dur, "SIMPLE");

        // =============================== HYBRID ===============================
        drv = new HybridScanner();
        start = System.nanoTime();
        res = drv.scan(dir);
        // res = drv.getPotentialDuplicates(dir, new ConcurrentLinkedQueue<>());
        dur = Duration.ofNanos(System.nanoTime() - start);
        printDuplicateResults(res, dur, "HYBRID");
        // printRunSummary(res, dur, "HYBRID");

        // =============================== THREADED==============================
        drv = new ThreadedScanner();
        start = System.nanoTime();
        res = drv.scan(dir);
        // res = drv.getPotentialDuplicates(dir, new ConcurrentLinkedQueue<>());
        dur = Duration.ofNanos(System.nanoTime() - start);
        printDuplicateResults(res, dur, "THREADED");
        // printRunSummary(res, dur, "THREADED");

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

    static void compareWalkTimes(Path dir) throws IOException {
        // var dpf = new DuplicateFinder();
        // var startTs = System.nanoTime();

        // var map1 = dpf.parsePath(dir);
        // var duration = Duration.ofNanos(System.nanoTime() - startTs);
        // printRunSummary(map1, duration, false);

        // var map2 = dpf.parsePathThreaded(dir);
        // duration = Duration.ofNanos(System.nanoTime() - startTs);
        // printRunSummary(map2, duration, true);

        // compareMaps(map1, map2);
    }

    static void findDuplicates(Path dir, boolean useTreads) throws IOException, InterruptedException {

        // var dpf = new DuplicateFinder();
        // long startTs = System.nanoTime();
        // var dups = dpf.getDuplicates(dir, useTreads);
        // var duration = Duration.ofNanos(System.nanoTime() - startTs);

        // printDuplicateResults(dups, duration, false);
    }

    static void compareMaps(Map<Long, Collection<Path>> map1, Map<Long, Collection<Path>> map2) {
        boolean same = map1.size() == map2.size();
        if (same) {
            for (var kv : map1.entrySet()) {
                if (!same)
                    break;
                var k = kv.getKey();
                var v1 = kv.getValue();
                var v2 = map2.get(k);
                if (v2 == null || v1.size() != v2.size())
                    same = false;
                else
                    same = v2.containsAll(v1);
            }
        }

        System.out.println(String.format("The maps are: %s", same ? "Equal" : "Different"));
    }

    static void printDetails(Map<Long, Collection<Path>> fileMap) {
        for (var kv : fileMap.entrySet()) {
            var cnt = kv.getValue().size();
            if (cnt > 1) {
                System.out.println("The are %s files with size: %d".formatted(cnt, kv.getKey()));
            }
        }
    }

    static void printRunSummary(Collection<Path> fileMap, Duration duration, String type) {
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
