import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class FileExchange {
    private static final String HEADER = "supplierId;supplierName;product;price;quantity";
    public static void main(String[] args) throws Exception {
        Path root = Path.of(args.length > 0 ? args[0] : "data").toAbsolutePath().normalize();
        int seconds = args.length > 1 ? Integer.parseInt(args[1]) : 0;
        Path incoming = Files.createDirectories(root.resolve("incoming"));
        Path processed = Files.createDirectories(root.resolve("processed"));
        System.out.println("=== Практическая работа №2. Обмен файлами поставщиков ===");
        System.out.println("Входящий каталог готов: " + incoming);
        System.out.println("Обработанные файлы: " + processed);
        try (WatchService watcher = incoming.getFileSystem().newWatchService()) {
            incoming.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
            try (var files = Files.list(incoming)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    System.out.printf("%nНайден файл: %s | Размер: %d байт | Изменён: %s%n", file.getFileName(), Files.size(file), Files.getLastModifiedTime(file).toInstant().atZone(java.time.ZoneId.systemDefault()).format(java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")));
                    process(file, processed);
                }
            }
            if (seconds <= 0) { System.out.println("\nОбработка входящих файлов завершена."); return; }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            System.out.println("\nНаблюдение WatchService включено на " + seconds + " с. Добавьте CSV в каталог incoming.");
            while (System.nanoTime() < deadline) {
                WatchKey key = watcher.poll(200, TimeUnit.MILLISECONDS);
                if (key == null) continue;
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.OVERFLOW) {
                        try (var files = Files.list(incoming)) {
                            for (Path file : files.filter(Files::isRegularFile).toList()) process(file, processed);
                        }
                    } else {
                        Path file = incoming.resolve((Path) event.context());
                        if (Files.isRegularFile(file)) {
                            System.out.println("WatchService: " + (event.kind() == StandardWatchEventKinds.ENTRY_CREATE ? "появился новый файл " : "изменён файл ") + file.getFileName());
                            for (int attempt = 0; attempt < 5 && Files.exists(file); attempt++) {
                                Thread.sleep(150);
                                if (process(file, processed)) break;
                            }
                        }
                    }
                }
                if (!key.reset()) throw new IllegalStateException("Входящий каталог больше недоступен");
            }
            System.out.println("\nВремя наблюдения истекло. Работа программы завершена.");
        }
    }
    private static boolean process(Path file, Path processed) {
        if (!file.getFileName().toString().endsWith(".csv")) return true;
        try {
            String text;
            if (file.getFileName().toString().contains("01")) {
                try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ); ByteArrayOutputStream output = new ByteArrayOutputStream()) {
                    ByteBuffer buffer = ByteBuffer.allocate(4096);
                    while (channel.read(buffer) != -1) {
                        buffer.flip();
                        output.write(buffer.array(), 0, buffer.remaining());
                        buffer.clear();
                    }
                    text = output.toString(StandardCharsets.UTF_8);
                }
                System.out.println("Способ чтения: FileChannel + ByteBuffer");
            } else text = Files.readString(file, StandardCharsets.UTF_8);
            List<String> lines = text.lines().filter(s -> !s.isBlank()).toList();
            if (lines.size() < 2 || !lines.get(0).equals(HEADER)) throw new IllegalArgumentException("Неверный заголовок CSV или файл не содержит записей");
            BigDecimal total = BigDecimal.ZERO;
            for (String line : lines.subList(1, lines.size())) {
                String[] fields = line.split(";", -1);
                if (fields.length != 5 || Integer.parseInt(fields[0]) <= 0 || fields[1].isBlank() || fields[2].isBlank()) throw new IllegalArgumentException("Некорректная запись: проверьте id, поставщика, товар и количество полей");
                BigDecimal price = new BigDecimal(fields[3]);
                int quantity = Integer.parseInt(fields[4]);
                if (price.signum() < 0 || quantity < 0) throw new IllegalArgumentException("Цена и количество не могут быть отрицательными");
                total = total.add(price.multiply(BigDecimal.valueOf(quantity)));
            }
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(file)) {
                byte[] chunk = new byte[8192];
                int count;
                while ((count = input.read(chunk)) != -1) digest.update(chunk, 0, count);
            }
            String sha = HexFormat.of().formatHex(digest.digest());
            long size = Files.size(file);
            Path destination = processed.resolve(file.getFileName());
            if (Files.exists(destination)) destination = processed.resolve(java.util.UUID.randomUUID() + "-" + file.getFileName());
            Files.move(file, destination);
            System.out.printf("Обработан файл: %s%n  Размер: %d байт; записей: %d%n  Общая стоимость: %s руб.%n  SHA-256: %s%n  Результат: файл перемещён в processed%n", destination.getFileName(), size, lines.size() - 1, total.toPlainString().replace('.',','), sha);
            return true;
        } catch (Exception error) {
            System.out.println("Файл отклонён: " + file.getFileName() + ". Причина: " + (error instanceof NumberFormatException ? "цена, количество или id имеют неверный числовой формат" : error.getMessage()));
            return false;
        }
    }
}