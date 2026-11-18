package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.scheduling.annotation.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.net.http.HttpClient;
import java.util.*;
@SpringBootApplication
@RestController
@RequestMapping("/api/orders")
@EnableScheduling
public class OrdersApplication {
    public record Product(long id,String name,BigDecimal price,int available) {}
    public record Input(long productId,int quantity,String address) {}
    public record Order(long id,long productId,int quantity,BigDecimal total,String address,String status) {}
    public record Created(long orderId,String address,String requestId) {}
    final JdbcTemplate db;
    final TransactionTemplate tx;
    final RabbitTemplate rabbit;
    final ObjectMapper json;
    final RestClient supplier;
    final CircuitBreaker breaker=CircuitBreaker.of("supplier",CircuitBreakerConfig.custom().slidingWindowSize(4).minimumNumberOfCalls(2).failureRateThreshold(50).waitDurationInOpenState(Duration.ofSeconds(5)).ignoreExceptions(HttpClientErrorException.class).build());
    public OrdersApplication(JdbcTemplate db,TransactionTemplate tx,RabbitTemplate rabbit,ObjectMapper json,@Value("${supplier.url}") String url) {
        this.db=db;this.tx=tx;this.rabbit=rabbit;this.json=json;
        var factory=new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofMillis(800)).build());
        factory.setReadTimeout(Duration.ofSeconds(1));
        supplier=RestClient.builder().baseUrl(url).requestFactory(factory).build();
    }
    public static void main(String[] args) { var context=SpringApplication.run(OrdersApplication.class,args); System.out.println("СЕРВИС «Заказы» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port")); }
    Product product(long id,String trace) {
        try {
            return breaker.executeSupplier(()->{
                for(int attempt=1;;attempt++) {
                    try { return supplier.get().uri("/api/suppliers/products/{id}",id).header("X-Request-ID",trace).retrieve().body(Product.class); }
                    catch(ResourceAccessException|HttpServerErrorException error) { if(attempt==2) throw error; System.out.println("[requestId="+trace+"] ЗАКАЗЫ | Ошибка получения товара. Повтор безопасного GET-запроса"); }
                }
            });
        } catch(HttpClientErrorException.NotFound error) { throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Указанный товар не существует"); }
        catch(RuntimeException error) { System.out.println("[requestId="+trace+"] ЗАКАЗЫ | Поставщик недоступен. Защита Circuit Breaker: "+breaker.getState());throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Сервис поставщиков временно недоступен. Повторите запрос позже"); }
    }
    @PostMapping @ResponseStatus(HttpStatus.CREATED)
    public Order create(@RequestBody Input input,@RequestHeader("X-Request-ID") String trace) {
        if(input.productId()<=0||input.quantity()<=0||input.address()==null||input.address().isBlank())throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Укажите товар, положительное количество и адрес");
        Product product=product(input.productId(),trace);
        if(product.available()<input.quantity())throw new ResponseStatusException(HttpStatus.CONFLICT,"Недостаточно товара в наличии");
        return tx.execute(status->{
            BigDecimal total=product.price().multiply(BigDecimal.valueOf(input.quantity()));
            Long id=db.queryForObject("INSERT INTO customer_order(product_id,quantity,total,address) VALUES(?,?,?,?) RETURNING id",Long.class,input.productId(),input.quantity(),total,input.address().trim());
            try { db.update("INSERT INTO outbox(payload) VALUES(?)",json.writeValueAsString(new Created(id,input.address().trim(),trace))); }
            catch(Exception error) { throw new IllegalStateException(error); }
            System.out.println("[requestId="+trace+"] ЗАКАЗЫ | Создан заказ №"+id+"; сумма: "+total+" руб.; событие сохранено в outbox");
            return new Order(id,input.productId(),input.quantity(),total,input.address().trim(),"CREATED");
        });
    }
    @GetMapping public List<Order> list() { return db.query("SELECT * FROM customer_order ORDER BY id",(r,n)->new Order(r.getLong("id"),r.getLong("product_id"),r.getInt("quantity"),r.getBigDecimal("total"),r.getString("address"),r.getString("status"))); }
    @GetMapping("/{id}") public Order get(@PathVariable long id) {return list().stream().filter(o->o.id()==id).findFirst().orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Заказ не найден"));}
    @Scheduled(fixedDelay=1000) public void publish() {
        for(var row:db.queryForList("SELECT * FROM outbox WHERE sent=FALSE ORDER BY id LIMIT 20")) {
            try {
                rabbit.invoke(operations->{ operations.convertAndSend("","delivery.created",row.get("payload"));operations.waitForConfirmsOrDie(3000);return null; });
                db.update("UPDATE outbox SET sent=TRUE WHERE id=?",row.get("id"));
                System.out.println("ЗАКАЗЫ | Событие outbox №"+row.get("id")+" отправлено в RabbitMQ; брокер подтвердил получение. "+row.get("payload"));
            } catch(RuntimeException error) { System.out.println("ЗАКАЗЫ | Событие outbox №"+row.get("id")+" ожидает повторной отправки. Причина: "+error.getClass().getSimpleName());break; }
        }
    }
}
