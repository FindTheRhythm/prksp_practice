package practice;
import org.springframework.boot.*;
import org.springframework.boot.autoconfigure.*;
import org.springframework.context.annotation.Bean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.server.*;
import org.springframework.web.reactive.function.client.*;
import org.springframework.http.*;
import reactor.core.publisher.Mono;
import java.util.*;
import java.time.Duration;
@SpringBootApplication
public class GatewayApplication {
    public static void main(String[] args) {var context=SpringApplication.run(GatewayApplication.class,args); System.out.println("СЕРВИС «API-шлюз» готов. HTTP-порт: "+context.getEnvironment().getProperty("local.server.port"));}
    @Bean WebFilter proxy(@Value("${supplier.url}") String supplier,@Value("${orders.url}") String orders,@Value("${delivery.url}") String delivery) {
        WebClient client=WebClient.builder().clientConnector(new org.springframework.http.client.reactive.ReactorClientHttpConnector(reactor.netty.http.client.HttpClient.create().option(io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS,1000).responseTimeout(Duration.ofSeconds(6)))).build();
        return (exchange,chain)->{
            String path=exchange.getRequest().getURI().getRawPath();
            if(!path.startsWith("/api/"))return chain.filter(exchange);
            String target=path.startsWith("/api/suppliers")?supplier:path.startsWith("/api/orders")?orders:path.startsWith("/api/deliveries")?delivery:null;
            if(target==null){exchange.getResponse().setStatusCode(HttpStatus.NOT_FOUND);return exchange.getResponse().setComplete();}
            String received=exchange.getRequest().getHeaders().getFirst("X-Request-ID");
            String trace=received!=null&&received.matches("[A-Za-z0-9._-]{1,100}")?received:UUID.randomUUID().toString();
            exchange.getResponse().getHeaders().set("X-Request-ID",trace);
            System.out.println("[requestId="+trace+"] ШЛЮЗ | "+exchange.getRequest().getMethod()+" "+path);
            String query=exchange.getRequest().getURI().getRawQuery();
            return client.method(exchange.getRequest().getMethod()).uri(target+path+(query==null?"":"?"+query)).headers(headers->{headers.addAll(exchange.getRequest().getHeaders());headers.remove(HttpHeaders.HOST);headers.remove(HttpHeaders.CONTENT_LENGTH);headers.set("X-Request-ID",trace);}).body(exchange.getRequest().getBody(),org.springframework.core.io.buffer.DataBuffer.class).exchangeToMono(response->{
                exchange.getResponse().setStatusCode(response.statusCode());
                response.headers().asHttpHeaders().forEach((key,values)->{if(!Set.of("transfer-encoding","connection","content-length").contains(key.toLowerCase()))exchange.getResponse().getHeaders().put(key,values);});
                if(response.headers().contentType().orElse(MediaType.APPLICATION_JSON).isCompatibleWith(MediaType.TEXT_EVENT_STREAM)) return exchange.getResponse().writeAndFlushWith(response.bodyToFlux(org.springframework.core.io.buffer.DataBuffer.class).map(Mono::just));
                return exchange.getResponse().writeWith(response.bodyToFlux(org.springframework.core.io.buffer.DataBuffer.class));
            }).onErrorResume(error->{
                System.out.println("[requestId="+trace+"] ШЛЮЗ | Внутренний сервис недоступен. Причина: "+error.getClass().getSimpleName());
                if(exchange.getResponse().isCommitted())return Mono.error(error);
                exchange.getResponse().setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);
                return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap("{\"error\":\"Внутренний сервис временно недоступен\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8))));
            });
        };
    }
}
