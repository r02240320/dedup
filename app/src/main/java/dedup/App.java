package dedup;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;

public class App {

    public static void main(String[] args) throws IOException, InterruptedException {
        var dir = getDirectory(args);
        if (dir != null) {
            // compareWalkTimes(dir);
            findDuplicates(dir, false);
        }
    }

    static Path getDirectory(String[] args) {
        if (args == null || args.length == 0) {
            var val = System.getProperty("scanDir");
            return val == null ? null : Path.of(val);
        }
        return Path.of(args[0]);
    }

    static void compareWalkTimes(Path dir) throws IOException {
        var dpf = new DuplicateFinder();
        var startTs = System.nanoTime();

        var map1 = dpf.parsePath(dir);
        var duration = Duration.ofNanos(System.nanoTime() - startTs);
        printRunSummary(map1, duration, false);

        var map2 = dpf.parsePathThreaded(dir);
        duration = Duration.ofNanos(System.nanoTime() - startTs);
        printRunSummary(map2, duration, true);

        compareMaps(map1, map2);
    }

    static void findDuplicates(Path dir, boolean useTreads) throws IOException, InterruptedException {

        var dpf = new DuplicateFinder();
        long startTs = System.nanoTime();
        var dups = dpf.getDuplicates(dir, useTreads);
        var duration = Duration.ofNanos(System.nanoTime() - startTs);

        printDuplicateResults(dups, duration, false);
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

    static void printRunSummary(Map<Long, Collection<Path>> fileMap, Duration duration, boolean threaded) {

        fileMap.values().stream().map(q -> q.size()).reduce((a, b) -> a + b).ifPresent(cnt -> {
            System.out.println(
                    "The %sThreaded run took: %s to go through: %s files".formatted(threaded ? "" : "Non-", duration,
                            cnt));
        });

    }

    static void printDuplicateResults(Collection<Collection<Path>> list, Duration duration, boolean summary) {
        var fmt = "Duplicate finder took: %s and found: %d duplicates\n";
        System.out.println(String.format(fmt, duration, list.size()));

        if (list.size() < 100) {
            int i = 0;
            for (var col : list) {
                ++i;
                System.out.print(String.format("List: %d has %d duplicates!", i, col.size()));
                if (!summary) {
                    var str = String.join("\n\t", col.stream().map(p -> p.toString()).toList());
                    System.out.print(String.format("\n\t%s", str));
                }
                System.out.println();
            }
        }
    }
}
