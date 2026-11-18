import java.math.BigInteger;
import java.util.Scanner;
import java.util.concurrent.*;

public class AsyncSquares {
    public static void main(String[] args) {
        System.out.println("=== Практическая работа №1. Асинхронное вычисление квадратов чисел ===");
        if (args.length != 0) {
            System.err.println("Использование: java -cp out AsyncSquares");
            return;
        }
        ExecutorService workers = Executors.newFixedThreadPool(5);
        ExecutorService reporter = Executors.newSingleThreadExecutor();
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Введите целое число или exit для завершения:");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (line.equalsIgnoreCase("exit")) break;
                if (line.length() > 1001) {
                    System.out.println("Слишком длинное число: допустимо не более 1000 цифр.");
                    continue;
                }
                try {
                    if (!line.matches("[+-]?[0-9]{1,1000}")) throw new NumberFormatException();
                    BigInteger number = new BigInteger(line);
                    Future<BigInteger> result = workers.submit(() -> {
                        Thread.sleep(ThreadLocalRandom.current().nextInt(1, 6) * 1000L);
                        return number.multiply(number);
                    });
                    reporter.submit(() -> {
                        try {
                            System.out.println(number + "² = " + result.get());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (ExecutionException e) {
                            System.err.println("Ошибка запроса: " + e.getCause());
                        }
                    });
                    System.out.println("Запрос принят. Можно ввести следующее число.");
                } catch (NumberFormatException e) {
                    System.out.println("Введите корректное целое число.");
                }
            }
            if (scanner.ioException() != null) {
                System.err.println("Ошибка чтения ввода: " + scanner.ioException().getMessage());
            }
        } finally {
            workers.shutdown();
            reporter.shutdown();
            try {
                workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                reporter.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                workers.shutdownNow();
                reporter.shutdownNow();
                Thread.currentThread().interrupt();
                System.err.println("Ожидание результатов прервано; незавершённые запросы отменены.");
            }
        }
    }
}
