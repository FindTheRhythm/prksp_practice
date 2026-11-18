package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.*;
import org.springframework.web.server.ResponseStatusException;
import java.time.Duration;
import java.net.http.HttpClient;
@SpringBootApplication
@RestController
@RequestMapping("/api/deliveries")
public class DeliveryApplication {
    public record Order(long id,String product,int quantity,String address) {}
    public record Delivery(long orderId,String address,String status,String product,int quantity,String instance) {}
    final RestClient orders;
    final String instance;
    public DeliveryApplication(@Value("${orders.url}") String url,@Value("${instance.id}") String instance) {
        var factory=new JdkClientHttpRequestFactory(HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build());factory.setReadTimeout(Duration.ofSeconds(2));
        orders=RestClient.builder().baseUrl(url).requestFactory(factory).build();this.instance=instance;
    }
    public static void main(String[] args) {var context=SpringApplication.run(DeliveryApplication.class,args); System.out.println("СЕРВИС «Доставка» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port"));}
    @GetMapping("/{orderId}") public ResponseEntity<Delivery> get(@PathVariable long orderId) {
        try {
            Order order=orders.get().uri("/api/orders/{id}",orderId).retrieve().body(Order.class);
            System.out.println("ДОСТАВКА | Экземпляр: "+instance+" | Заказ №"+orderId+" | Адрес: "+order.address());
            return ResponseEntity.ok().header("X-Instance-ID",instance).body(new Delivery(order.id(),order.address(),"READY_TO_ASSIGN",order.product(),order.quantity(),instance));
        } catch(HttpClientErrorException.NotFound error) {throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Заказ не найден");}
        catch(RestClientException error) {throw new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Сервис заказов временно недоступен");}
    }
}
