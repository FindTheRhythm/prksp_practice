package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import java.math.BigDecimal;
import java.util.*;
@SpringBootApplication
@RestController
@RequestMapping("/api/suppliers/products")
public class SupplierApplication {
    public record Product(long id,String name,BigDecimal price,int available) {}
    final JdbcTemplate db;
    public SupplierApplication(JdbcTemplate db) { this.db=db; }
    public static void main(String[] args) { var context=SpringApplication.run(SupplierApplication.class,args); System.out.println("СЕРВИС «Поставщики» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port")); }
    @GetMapping public List<Product> list() { return db.query("SELECT * FROM product ORDER BY id",(r,n)->new Product(r.getLong("id"),r.getString("name"),r.getBigDecimal("price"),r.getInt("available"))); }
    @GetMapping("/{id}") public Product get(@PathVariable long id,@RequestHeader("X-Request-ID") String trace) {
        System.out.println("[requestId="+trace+"] ПОСТАВЩИК | Запрошен товар №"+id);
        return db.query("SELECT * FROM product WHERE id=?",(r,n)->new Product(r.getLong("id"),r.getString("name"),r.getBigDecimal("price"),r.getInt("available")),id).stream().findFirst().orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Товар не найден"));
    }
}
