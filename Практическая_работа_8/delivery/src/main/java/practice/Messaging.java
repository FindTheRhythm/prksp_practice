package practice;
import org.springframework.context.annotation.*;
import org.springframework.amqp.core.*;
@Configuration
public class Messaging {
    @Bean Queue deliveryQueue() { return QueueBuilder.durable("delivery.created").withArgument("x-dead-letter-exchange","delivery.dead").build(); }
    @Bean DirectExchange deadExchange() { return new DirectExchange("delivery.dead"); }
    @Bean Queue deadQueue() { return QueueBuilder.durable("delivery.failed").build(); }
    @Bean Binding deadBinding() { return BindingBuilder.bind(deadQueue()).to(deadExchange()).with("delivery.created"); }
}
