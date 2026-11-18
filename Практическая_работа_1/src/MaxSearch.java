import java.util.Random;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MaxSearch {
    static final int WORKERS = Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors()));

    static int range(int[] a, int start, int end) throws InterruptedException {
        if (a == null || start < 0 || end > a.length || start >= end) {
            throw new IllegalArgumentException("Некорректный массив или диапазон поиска");
        }
        int max = Integer.MIN_VALUE;
        for (int i = start; i < end; i++) {
            if (a[i] > max) max = a[i];
            Thread.sleep(1);
        }
        return max;
    }

    static int threaded(int[] a) throws Exception {
        checkArray(a);
        int workers = Math.min(WORKERS, a.length);
        ExecutorService pool = Executors.newFixedThreadPool(workers);
        try {
            List<Future<Integer>> results = new ArrayList<>();
            for (int i = 0; i < workers; i++) {
                int start = i * a.length / workers;
                int end = (i + 1) * a.length / workers;
                results.add(pool.submit(() -> range(a, start, end)));
            }
            int max = Integer.MIN_VALUE;
            for (Future<Integer> result : results) {
                max = Math.max(max, result.get());
                Thread.sleep(1);
            }
            return max;
        } finally {
            pool.shutdownNow();
        }
    }

    static class MaxTask extends RecursiveTask<Integer> {
        final int[] a;
        final int start;
        final int end;

        MaxTask(int[] a, int start, int end) {
            this.a = a;
            this.start = start;
            this.end = end;
        }

        protected Integer compute() {
            if (end - start <= 625) {
                try {
                    return range(a, start, end);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            }
            int middle = (start + end) / 2;
            MaxTask left = new MaxTask(a, start, middle);
            MaxTask right = new MaxTask(a, middle, end);
            left.fork();
            int rightMax = right.compute();
            int leftMax = left.join();
            int max = Math.max(leftMax, rightMax);
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return max;
        }
    }

    static int forkJoin(int[] a) {
        checkArray(a);
        ForkJoinPool pool = new ForkJoinPool(WORKERS);
        try {
            return pool.invoke(new MaxTask(a, 0, a.length));
        } finally {
            pool.shutdown();
        }
    }

    static void checkArray(int[] a) {
        if (a == null || a.length == 0) {
            throw new IllegalArgumentException("Массив не должен быть пустым или отсутствовать");
        }
    }

    interface Search { int run() throws Exception; }

    static int measure(String name, Search search) throws Exception {
        System.out.println("Начат способ: "+name+". Ожидайте окончания вычислений.");
        Runtime runtime = Runtime.getRuntime();
        long baseline = runtime.totalMemory() - runtime.freeMemory();
        AtomicLong peak = new AtomicLong(baseline);
        AtomicBoolean running = new AtomicBoolean(true);
        Thread sampler = new Thread(() -> {
            while (running.get()) {
                long used = runtime.totalMemory() - runtime.freeMemory();
                peak.accumulateAndGet(used, Math::max);
                try { Thread.sleep(5); } catch (InterruptedException e) { return; }
            }
        });
        sampler.setDaemon(true);
        sampler.start();
        long start = System.nanoTime();
        int result;
        try { result = search.run(); } finally { running.set(false); sampler.join(); }
        double ms = (System.nanoTime() - start) / 1_000_000.0;
        System.out.printf("%s: максимум = %d, время = %.2f мс, прирост пика кучи = %.2f МиБ%n",
                name, result, ms, (peak.get() - baseline) / 1048576.0);
        return result;
    }

    public static void main(String[] args) {
        System.out.println("=== Практическая работа №1. Сравнение способов поиска максимума ===");
        if (args.length != 0) {
            System.err.println("Использование: java -cp out MaxSearch");
            return;
        }
        try {
            int[] a = new Random(42).ints(10000, -100000, 100001).toArray();
            System.out.println("Элементов: " + a.length + ", потоков: " + WORKERS);
            int first = measure("Последовательно", () -> range(a, 0, a.length));
            int second = measure("ExecutorService", () -> threaded(a));
            int third = measure("ForkJoin", () -> forkJoin(a));
            if (first != second || first != third) {
                System.err.println("Ошибка: результаты поиска различаются.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Поиск прерван.");
        } catch (Exception e) {
            System.err.println("Не удалось выполнить поиск: " + e.getMessage());
        }
    }
}

