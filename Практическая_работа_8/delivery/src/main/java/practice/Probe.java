package practice;
public class Probe {
    public static void main(String[] args) throws Exception {
        var client=java.net.http.HttpClient.newBuilder().connectTimeout(java.time.Duration.ofSeconds(2)).build();
        var request=java.net.http.HttpRequest.newBuilder(java.net.URI.create("http://localhost:8080/actuator/health")).timeout(java.time.Duration.ofSeconds(3)).GET().build();
        System.exit(client.send(request,java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode()==200 ? 0 : 1);
    }
}
