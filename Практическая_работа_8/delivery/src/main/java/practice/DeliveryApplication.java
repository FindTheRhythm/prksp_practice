package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.http.*;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.*;
import reactor.core.scheduler.Schedulers;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.*;
@SpringBootApplication
@RestController
@RequestMapping("/api/deliveries")
public class DeliveryApplication {
    public record Created(long orderId,String address,String requestId) {}
    public record Delivery(long id,long orderId,String address,String status,String requestId) {}
    public record Update(String status) {}
    public record Event(long id,long deliveryId,String status,String time) {}
    final JdbcTemplate db;
    final TransactionTemplate tx;
    final ObjectMapper json;
    public DeliveryApplication(JdbcTemplate db,TransactionTemplate tx,ObjectMapper json) { this.db=db;this.tx=tx;this.json=json; }
    public static void main(String[] args) { var context=SpringApplication.run(DeliveryApplication.class,args); System.out.println("СЕРВИС «Доставка» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port")); }
    @RabbitListener(queues="delivery.created") public void consume(String payload) throws Exception {
        Created event=json.readValue(payload,Created.class);
        if(event.orderId()<=0||event.address()==null||event.requestId()==null)throw new IllegalArgumentException("Событие содержит некорректные данные");
        tx.executeWithoutResult(status->{
            var ids=db.queryForList("INSERT INTO delivery(order_id,address,request_id) VALUES(?,?,?) ON CONFLICT(order_id) DO NOTHING RETURNING id",Long.class,event.orderId(),event.address(),event.requestId());
            if(!ids.isEmpty()) db.update("INSERT INTO delivery_event(delivery_id,status) VALUES(?,'CREATED')",ids.get(0));
            System.out.println("[requestId="+event.requestId()+"] ДОСТАВКА | Получено событие заказа №"+event.orderId()+" | "+(ids.isEmpty()?"Повтор события: новая доставка не создаётся":"Доставка создана, статус: Создана (CREATED)"));
        });
    }
    List<Delivery> readAll() {return db.query("SELECT * FROM delivery ORDER BY id",(r,n)->new Delivery(r.getLong("id"),r.getLong("order_id"),r.getString("address"),r.getString("status"),r.getString("request_id")));}
    Delivery read(long id) {return readAll().stream().filter(d->d.id()==id).findFirst().orElseThrow(()->new ResponseStatusException(HttpStatus.NOT_FOUND,"Доставка не найдена"));}
    @GetMapping public Flux<Delivery> list() {return Mono.fromCallable(this::readAll).subscribeOn(Schedulers.boundedElastic()).flatMapMany(Flux::fromIterable);}
    @GetMapping("/{id}") public Mono<Delivery> get(@PathVariable long id) {return Mono.fromCallable(()->read(id)).subscribeOn(Schedulers.boundedElastic());}
    @PutMapping("/{id}") public Mono<Delivery> update(@PathVariable long id,@RequestBody Update input,@RequestHeader("X-Request-ID") String trace) {
        return Mono.fromCallable(()->tx.execute(transaction->{
            var rows=db.queryForList("SELECT status FROM delivery WHERE id=? FOR UPDATE",String.class,id);
            if(rows.isEmpty())throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Доставка не найдена");
            String current=rows.get(0),next=input.status();
            boolean allowed=switch(current) {case "CREATED"->Set.of("ACCEPTED","CANCELLED").contains(next==null?"":next);case "ACCEPTED"->Set.of("IN_TRANSIT","CANCELLED").contains(next==null?"":next);case "IN_TRANSIT"->"DELIVERED".equals(next);default->false;};
            if(!allowed)throw new ResponseStatusException(HttpStatus.CONFLICT,"Недопустимый переход статуса доставки");
            db.update("UPDATE delivery SET status=? WHERE id=?",next,id);
            db.update("INSERT INTO delivery_event(delivery_id,status) VALUES(?,?)",id,next);
            System.out.println("[requestId="+trace+"] ДОСТАВКА №"+id+" | Новый статус: "+switch(next) {case "ACCEPTED" -> "Принята";case "IN_TRANSIT" -> "В пути";case "DELIVERED" -> "Доставлена";case "CANCELLED" -> "Отменена";default -> next;}+" ("+next+")");
            return read(id);
        })).subscribeOn(Schedulers.boundedElastic());
    }
    @GetMapping(value="/{id}/events",produces=MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Event>> events(@PathVariable long id,@RequestHeader(value="Last-Event-ID",defaultValue="0") long after) {
        var cursor=new java.util.concurrent.atomic.AtomicLong(after);
        Flux<ServerSentEvent<Event>> changes=get(id).flatMapMany(delivery->Flux.interval(Duration.ZERO,Duration.ofMillis(400)).concatMap(tick->Mono.fromCallable(()->db.query("SELECT * FROM delivery_event WHERE delivery_id=? AND id>? ORDER BY id",(r,n)->new Event(r.getLong("id"),r.getLong("delivery_id"),r.getString("status"),r.getTimestamp("created_at").toInstant().toString()),id,cursor.get())).subscribeOn(Schedulers.boundedElastic())).flatMapIterable(events->events).doOnNext(event->cursor.set(event.id())).map(event->ServerSentEvent.builder(event).id(Long.toString(event.id())).event(event.status()).build()).takeUntil(event->Set.of("DELIVERED","CANCELLED").contains(event.data().status())));
        return changes.mergeWith(Flux.interval(Duration.ofSeconds(2)).map(tick->ServerSentEvent.<Event>builder().comment("keepalive").build())).takeUntil(event->event.data()!=null&&Set.of("DELIVERED","CANCELLED").contains(event.data().status()));
    }
}
