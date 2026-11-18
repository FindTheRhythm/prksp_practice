package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.reactive.function.client.*;
import reactor.core.publisher.*;
import java.time.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

@SpringBootApplication
@RestController
@RequestMapping(value="/api/deliveries",produces="application/json;charset=UTF-8")
public class DeliveriesApplication {
    enum Status { CREATED, ACCEPTED, IN_TRANSIT, DELIVERED, CANCELLED }
    public record Delivery(long id,long orderId,String address,Status status) {}
    public record Input(long orderId,String address,Status status) {}
    public record Order(long id,String product,int quantity) {}
    public record Event(long id,Status status,String operation,Instant time) {}
    static class Slot {
        volatile Delivery delivery;
        final Sinks.Many<Event> events=Sinks.many().replay().limit(16);
        Slot(Delivery delivery) { this.delivery=delivery; }
        void emit(String operation) { System.out.println("ДОСТАВКА №"+delivery.id()+" | "+switch(operation) { case "CREATED" -> "Создана"; case "DELETED" -> "Удалена"; default -> "Обновлена"; }+" | Статус: "+switch(delivery.status()) {case CREATED -> "Создана";case ACCEPTED -> "Принята";case IN_TRANSIT -> "В пути";case DELIVERED -> "Доставлена";case CANCELLED -> "Отменена";}+" ("+delivery.status()+")"); events.emitNext(new Event(delivery.id(),delivery.status(),operation,Instant.now()),Sinks.EmitFailureHandler.FAIL_FAST); }
    }
    final ConcurrentHashMap<Long,Slot> slots=new ConcurrentHashMap<>();
    final AtomicLong ids=new AtomicLong();
    final WebClient orders;
    public DeliveriesApplication(@Value("${orders.url}") String url) { orders=WebClient.builder().baseUrl(url).build(); }
    public static void main(String[] args) { var context=SpringApplication.run(DeliveriesApplication.class,args); System.out.println("СЕРВИС «Доставка» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port")); }
    Mono<Slot> find(long id) { return Mono.defer(() -> Mono.justOrEmpty(slots.get(id))).switchIfEmpty(Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND,"Доставка не найдена"))); }
    void validate(Input input) {
        if(input.orderId()<=0 || input.address()==null || input.address().isBlank()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Укажите существующий заказ и непустой адрес");
    }
    Mono<Order> order(long id) {
        return orders.get().uri("/api/orders/{id}",id).retrieve().bodyToMono(Order.class).timeout(Duration.ofSeconds(2))
            .onErrorResume(WebClientResponseException.NotFound.class,error -> Mono.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,"Указанный заказ не существует")))
            .onErrorMap(error -> error instanceof ResponseStatusException ? error : new ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE,"Сервис заказов временно недоступен",error));
    }
    @GetMapping
    public Flux<Delivery> list(@RequestParam(required=false) Status status) { return Flux.defer(() -> Flux.fromIterable(slots.values())).map(slot -> slot.delivery).filter(value -> status==null || value.status()==status); }
    @GetMapping("/{id}")
    public Mono<Delivery> get(@PathVariable long id) { return find(id).map(slot -> slot.delivery); }
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Delivery> create(@RequestBody Input input) {
        return Mono.defer(() -> {
            validate(input);
            if(input.status()!=null && input.status()!=Status.CREATED) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Начальный статус доставки должен быть CREATED (Создана)");
            return order(input.orderId()).map(value -> {
                Delivery delivery=new Delivery(ids.incrementAndGet(),input.orderId(),input.address().trim(),Status.CREATED);
                Slot slot=new Slot(delivery);
                slots.put(delivery.id(),slot);
                slot.emit("CREATED");
                return delivery;
            });
        });
    }
    @PutMapping("/{id}")
    public Mono<Delivery> update(@PathVariable long id,@RequestBody Input input) {
        return find(id).flatMap(slot -> Mono.fromSupplier(() -> {
            validate(input);
            if(input.status()==null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Укажите статус доставки");
            synchronized(slot) {
                Delivery current=slot.delivery;
                if(slots.get(id)!=slot) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Доставка уже удалена");
                if(input.orderId()!=current.orderId()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST,"Нельзя изменить заказ существующей доставки");
                Status next=input.status();
                boolean allowed=next==current.status() || switch(current.status()) {
                    case CREATED -> Set.of(Status.ACCEPTED,Status.CANCELLED).contains(next);
                    case ACCEPTED -> Set.of(Status.IN_TRANSIT,Status.CANCELLED).contains(next);
                    case IN_TRANSIT -> next==Status.DELIVERED;
                    default -> false;
                };
                if(!allowed) throw new ResponseStatusException(HttpStatus.CONFLICT,"Недопустимый переход статуса доставки");
                slot.delivery=new Delivery(id,current.orderId(),input.address().trim(),next);
                slot.emit("UPDATED");
                return slot.delivery;
            }
        }));
    }
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> delete(@PathVariable long id) {
        return find(id).flatMap(slot -> Mono.fromRunnable(() -> {
            synchronized(slot) {
                if(!slots.remove(id,slot)) throw new ResponseStatusException(HttpStatus.NOT_FOUND,"Доставка уже удалена");
                slot.emit("DELETED");
                slot.events.emitComplete(Sinks.EmitFailureHandler.FAIL_FAST);
            }
        }));
    }
    @GetMapping(value="/{id}/stream",produces=MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Event>> stream(@PathVariable long id) { return find(id).flatMapMany(slot -> slot.events.asFlux()).map(event -> ServerSentEvent.builder(event).event(event.operation()).id(event.time().toString()).build()); }
    @GetMapping("/{id}/order")
    public Mono<Order> externalOrder(@PathVariable long id) { return get(id).flatMap(delivery -> order(delivery.orderId())); }
}
