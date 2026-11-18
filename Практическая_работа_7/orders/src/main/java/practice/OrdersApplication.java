package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

@SpringBootApplication
@RestController
@RequestMapping(produces="application/json;charset=UTF-8")
public class OrdersApplication {
    public record Order(long id,String product,int quantity) {}
    final java.util.Map<Long,Order> orders;
    public OrdersApplication(com.fasterxml.jackson.databind.ObjectMapper json) throws java.io.IOException {
        try(var input=OrdersApplication.class.getResourceAsStream("/orders.json")) {
            if(input==null) throw new IllegalStateException("Не найден файл orders.json");
            orders=java.util.Arrays.stream(json.readValue(input,Order[].class)).collect(java.util.stream.Collectors.toUnmodifiableMap(Order::id,java.util.function.Function.identity()));
        }
    }
    @GetMapping("/api/orders")
    public reactor.core.publisher.Flux<Order> list() { return reactor.core.publisher.Flux.fromIterable(new java.util.TreeMap<>(orders).values()); }
    public static void main(String[] args) { var context=SpringApplication.run(OrdersApplication.class,args); System.out.println("СЕРВИС «Заказы» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port")); }
    @GetMapping("/api/orders/{id}")
    public Mono<Order> get(@PathVariable long id) {
        return Mono.justOrEmpty(orders.get(id)).switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,"Заказ не найден")));
    }
}