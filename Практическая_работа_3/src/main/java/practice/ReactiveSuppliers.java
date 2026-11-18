package practice;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReactiveSuppliers {
    record Product(int supplierId, String name, BigDecimal price, int quantity) {
        public String toString() { return "поставщик №"+supplierId+", товар «"+name+"», цена "+price.toPlainString().replace('.',',')+" руб., количество "+quantity; }
    }
    record ValuedProduct(int supplierId, String name, BigDecimal value) {
        public String toString() { return "поставщик №"+supplierId+", товар «"+name+"», стоимость "+value.toPlainString().replace('.',',')+" руб."; }
    }
    static void log(String message) { System.out.println(java.time.LocalTime.now().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS"))+" | Поток: "+Thread.currentThread().getName()+" | "+message); }
    public static void main(String[] args) throws Exception {
        System.out.println("=== Практическая работа №3. Реактивная обработка товаров ===");
        System.out.println("Новые записи поступают каждые 150 мс. Стоимость = цена × количество.\n");
        List<Product> products;
        try (var input=ReactiveSuppliers.class.getResourceAsStream("/products.csv")) {
            if(input==null) throw new IllegalStateException("Не найден файл products.csv");
            products=new String(input.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8).lines().skip(1).filter(line->!line.isBlank()).map(line->{
                String[] fields=line.split(";",-1);
                if(fields.length!=4) throw new IllegalArgumentException("В CSV ожидаются четыре поля");
                return new Product(Integer.parseInt(fields[0]),fields[1],new BigDecimal(fields[2]),Integer.parseInt(fields[3]));
            }).toList();
        }
        System.out.println("Записей во входном файле: "+products.size());
        Observable.intervalRange(0,products.size(),0,150,TimeUnit.MILLISECONDS,Schedulers.computation())
            .map(index -> products.get(index.intValue()))
            .doOnNext(product -> log("Поступила запись: " + product))
            .observeOn(Schedulers.io())
            .flatMap(product -> Observable.fromCallable(() -> {
                if (product.price().signum()<0 || product.quantity()<0) throw new IllegalArgumentException("Некорректная цена или количество у товара: " + product.name());
                return product;
            }).onErrorResumeNext(error -> { log("Ошибка обработана, запись пропущена: " + error.getMessage()); return Observable.empty(); }))
            .doOnNext(product -> { if(product.quantity()==0) log("Фильтр: товар «"+product.name()+"» пропущен, нет в наличии"); })
            .filter(product -> product.quantity()>0)
            .map(product -> new ValuedProduct(product.supplierId(),product.name(),product.price().multiply(BigDecimal.valueOf(product.quantity()))))
            .doOnNext(product -> log("Результат преобразования: " + product))
            .groupBy(ValuedProduct::supplierId)
            .flatMapSingle(group -> group.map(ValuedProduct::value).reduce(BigDecimal.ZERO,BigDecimal::add).map(total -> "ИТОГО: поставщик №" + group.getKey() + ", общая стоимость = " + total.toPlainString().replace('.',',') + " руб."))
            .blockingSubscribe(ReactiveSuppliers::log,error -> { throw new IllegalStateException("Не удалось завершить обработку потока",error); },() -> log("Обработка завершена. Ошибочная запись не остановила поток."));
        Schedulers.shutdown();
    }
}