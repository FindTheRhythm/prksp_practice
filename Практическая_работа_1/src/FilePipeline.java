import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FilePipeline {
    enum Type { XML, JSON, XLS }
    record FileItem(int id, Type type, int size) {
        FileItem {
            if (id < 1 || type == null || size < 10 || size > 100) {
                throw new IllegalArgumentException("Некорректные данные файла");
            }
        }
    }

    static class FileQueue {
        private final List<FileItem> files = new ArrayList<>();
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition changed = lock.newCondition();
        private boolean closed;

        void put(FileItem file) throws InterruptedException {
            if (file == null) throw new IllegalArgumentException("Файл не задан");
            lock.lockInterruptibly();
            try {
                while (files.size() == 5 && !closed) changed.await();
                if (closed) throw new IllegalStateException("Очередь закрыта");
                files.add(file);
                System.out.printf("Создан №%d %s размер %d; в очереди %d%n", file.id(), file.type(), file.size(), files.size());
                changed.signalAll();
            } finally { lock.unlock(); }
        }

        FileItem take(Type type) throws InterruptedException {
            if (type == null) throw new IllegalArgumentException("Тип файла не задан");
            lock.lockInterruptibly();
            try {
                while (true) {
                    for (int i = 0; i < files.size(); i++) {
                        if (files.get(i).type() == type) {
                            FileItem file = files.remove(i);
                            changed.signalAll();
                            return file;
                        }
                    }
                    if (closed) return null;
                    changed.await();
                }
            } finally { lock.unlock(); }
        }

        void close() {
            lock.lock();
            try { closed = true; changed.signalAll(); } finally { lock.unlock(); }
        }
    }

    public static void main(String[] args) {
        System.out.println("=== Практическая работа №1. Многопоточная обработка файлов ===");
        int count;
        try {
            if (args.length > 1) throw new IllegalArgumentException();
            count = args.length == 0 ? 20 : Integer.parseInt(args[0]);
            if (count < 1 || count > 10000) throw new IllegalArgumentException();
        } catch (IllegalArgumentException e) {
            System.err.println("Укажите одно целое количество файлов от 1 до 10000: java -cp out FilePipeline [количество]");
            return;
        }
        FileQueue queue = new FileQueue();
        List<Thread> processors = new ArrayList<>();
        for (Type type : Type.values()) {
            Thread processor = new Thread(() -> {
                try {
                    FileItem file;
                    while ((file = queue.take(type)) != null) {
                        int delay = file.size() * 7;
                        Thread.sleep(delay);
                        System.out.printf("Обработан №%d %s размер %d за %d мс%n", file.id(), type, file.size(), delay);
                    }
                } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }, "processor-" + type);
            processors.add(processor);
            processor.start();
        }
        Thread generator = new Thread(() -> {
            try {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                for (int i = 1; i <= count; i++) {
                    Thread.sleep(random.nextInt(100, 1001));
                    Type type = Type.values()[random.nextInt(Type.values().length)];
                    queue.put(new FileItem(i, type, random.nextInt(10, 101)));
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            finally { queue.close(); }
        }, "generator");
        generator.start();
        try {
            generator.join();
            for (Thread processor : processors) processor.join();
            System.out.println("Все файлы обработаны.");
        } catch (InterruptedException e) {
            generator.interrupt();
            queue.close();
            for (Thread processor : processors) processor.interrupt();
            Thread.currentThread().interrupt();
            System.err.println("Работа прервана; обработка оставшихся файлов отменена.");
        }
    }
}
