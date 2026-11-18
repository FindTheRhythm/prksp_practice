package practice;
import io.rsocket.*;
import io.rsocket.core.*;
import io.rsocket.transport.netty.server.*;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.*;
import org.reactivestreams.Publisher;
import java.time.*;
import java.util.Map;

public class RSocketDemo {
    static final Map<Integer,String> PRODUCTS=loadCatalog();
    static Map<Integer,String> loadCatalog() {
        try(var input=RSocketDemo.class.getResourceAsStream("/catalog.csv")) {
            if(input==null) throw new IllegalStateException("Не найден файл catalog.csv");
            Map<Integer,String> catalog=new java.util.TreeMap<>();
            new String(input.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8).lines().skip(1).filter(line->!line.isBlank()).forEach(line->{
                String[] fields=line.split(";",-1);
                if(fields.length!=3 || new java.math.BigDecimal(fields[2]).signum()<0) throw new IllegalArgumentException("Некорректная строка каталога");
                int id=Integer.parseInt(fields[0]);
                if(id<=0 || catalog.putIfAbsent(id,line)!=null) throw new IllegalArgumentException("Некорректный или повторный id товара");
            });
            return java.util.Collections.unmodifiableMap(catalog);
        } catch(java.io.IOException error) { throw new IllegalStateException("Не удалось прочитать каталог",error); }
    }
    static String read(Payload payload) { try { return payload.getDataUtf8(); } finally { payload.release(); } }
    static Mono<Payload> product(String id) {
        return Mono.fromCallable(() -> {
            String result=PRODUCTS.get(Integer.parseInt(id));
            if(result==null) throw new IllegalArgumentException("Товар не найден: " + id);
            return (Payload)DefaultPayload.create(result);
        }).doOnError(error -> System.out.println("СЕРВЕР — обработка ошибки: " + error.getMessage()));
    }
    static CloseableChannel server(int port) {
        return RSocketServer.create(SocketAcceptor.with(new RSocket() {
            public Mono<Payload> requestResponse(Payload payload) { return product(read(payload)); }
            public Flux<Payload> requestStream(Payload payload) {
                read(payload);
                return Flux.fromIterable(PRODUCTS.keySet()).delayElements(Duration.ofMillis(250)).concatMap(id -> product(id.toString()));
            }
            public Mono<Void> fireAndForget(Payload payload) { System.out.println("СЕРВЕР — получено событие без ожидания ответа (Fire-and-Forget): " + read(payload)); return Mono.empty(); }
            public Flux<Payload> requestChannel(Publisher<Payload> input) {
                return Flux.from(input).map(RSocketDemo::read).concatMap(RSocketDemo::product).delayElements(Duration.ofMillis(100));
            }
        })).bind(TcpServerTransport.create("127.0.0.1",port)).block(Duration.ofSeconds(10));
    }
    static void show(String mode,Payload payload) { printProduct(mode,read(payload)); }
    static void printProduct(String mode,String data) {
        String[] fields=data.split(";");
        System.out.printf("%s | %s | Товар №%s: «%s», цена %s руб.%n",java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),mode,fields[0],fields[1],fields[2].replace('.',','));
    }
    static void client(int port) {
        RSocket connection=RSocketConnector.connectWith(TcpClientTransport.create("127.0.0.1",port)).block(Duration.ofSeconds(10));
        try {
            System.out.println("КЛИЕНТ подключён. Последовательно выполняются четыре режима и запрос отсутствующего товара.\n");
            connection.requestResponse(DefaultPayload.create("1")).doOnNext(p -> show("Один объект (Request-Response)",p)).then().block();
            connection.requestStream(DefaultPayload.create("catalog")).doOnNext(p -> show("Поступил элемент потока (Request-Stream)",p)).then().block();
            connection.fireAndForget(DefaultPayload.create("Пользователь просмотрел каталог")).block();
            connection.requestChannel(Flux.fromIterable(PRODUCTS.keySet()).map(String::valueOf).delayElements(Duration.ofMillis(120)).map(DefaultPayload::create)).map(p -> read(p)).filter(value -> value.contains(";")).map(String::toUpperCase).doOnNext(value -> printProduct("Двусторонний обмен (Channel)",value)).then().block();
            connection.requestResponse(DefaultPayload.create("999")).doOnNext(p -> { p.release(); throw new IllegalStateException("Ожидалась ошибка: отсутствующий товар не должен возвращаться"); }).onErrorResume(error -> {if(!error.getMessage().contains("Товар не найден")) return Mono.error(error); System.out.println("КЛИЕНТ — ошибка обработана, соединение не прервано: "+error.getMessage());return Mono.empty();}).then().block();
            System.out.println("\nПроверка RSocket завершена: все четыре режима выполнены, ошибка обработана.");
        } finally { connection.dispose(); }
    }
    public static void main(String[] args) {
        System.out.println("=== Практическая работа №4. Четыре режима RSocket ===");
        String mode=args.length>0?args[0]:"demo";
        int port=args.length>1?Integer.parseInt(args[1]):7004;
        if(mode.equals("client")) { client(port); return; }
        CloseableChannel channel=server(port);
        System.out.println("СЕРВЕР готов принимать подключения: " + channel.address());
        if(mode.equals("server")) channel.onClose().block();
        else try { client(port); } finally { channel.dispose(); channel.onClose().block(); }
    }
}
