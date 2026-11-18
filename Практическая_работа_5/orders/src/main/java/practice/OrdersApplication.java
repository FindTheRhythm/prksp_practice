package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.util.*;
@SpringBootApplication
@RestController
@RequestMapping("/api/orders")
public class OrdersApplication {
    public record Order(long id,String product,int quantity,String address) {}
    public record Input(String product,int quantity,String address) {}
    final JdbcTemplate db;
    public OrdersApplication(JdbcTemplate db) {this.db=db;}
    public static void main(String[] args) {var context=SpringApplication.run(OrdersApplication.class,args); System.out.println("СЕРВИС «Заказы» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port"));}
    @GetMapping public List<Order> list() {return db.query("SELECT * FROM customer_order ORDER BY id",(r,n)->new Order(r.getLong("id"),r.getString("product"),r.getInt("quantity"),r.getString("address")));}
    @GetMapping("/{id}") public Order get(@PathVariable long id) {return list().stream().filter(o->o.id()==id).findFirst().orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Заказ не найден"));}
    @PostMapping @ResponseStatus(HttpStatus.CREATED) public Order create(@RequestBody Input input) {
        if(input.product()==null||input.product().isBlank()||input.address()==null||input.address().isBlank()||input.quantity()<=0)throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Укажите товар, адрес и положительное количество");
        Long id=db.queryForObject("INSERT INTO customer_order(product,quantity,address) VALUES(?,?,?) RETURNING id",Long.class,input.product(),input.quantity(),input.address());
        return new Order(id,input.product(),input.quantity(),input.address());
    }
}
