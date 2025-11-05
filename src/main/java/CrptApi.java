import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class CrptApi {
    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final Deque<Instant> requestTimestamps = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    private final ObjectMapper objectMapper = new ObjectMapper()
            .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    public void createDocument(Document document, String signature, String omsId) {
        validateRateLimit();

        try {
            String requestBody = objectMapper.writeValueAsString(document);

            String url = String.format("https://ismp.crpt.ru/api/v2/lp/rollout?omsId=%s", omsId);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("Content-Type", "application/json")
                    .header("Signature", signature)
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() >= 400) {
                throw new ApiException("API request failed with status: " + response.statusCode());
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing document to JSON", e);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException("Error sending request to API", e);
        }
    }

    private void validateRateLimit() {
        lock.lock();
        try {
            Instant now = Instant.now();
            Instant windowStart = now.minus(getDurationFromTimeUnit());
            while (!requestTimestamps.isEmpty() && requestTimestamps.peek().isBefore(windowStart)) {
                requestTimestamps.poll();
            }
            if (requestTimestamps.size() >= requestLimit) {
                Instant oldest = requestTimestamps.peek();
                long waitMillis = Duration.between(windowStart, oldest).toMillis();

                if (waitMillis > 0) {
                    try {
                        Thread.sleep(waitMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for rate limit", e);
                    }
                    now = Instant.now();
                    windowStart = now.minus(getDurationFromTimeUnit());
                    while (!requestTimestamps.isEmpty() && requestTimestamps.peek().isBefore(windowStart)) {
                        requestTimestamps.poll();
                    }
                }
            }
            requestTimestamps.add(now);
        } finally {
            lock.unlock();
        }
    }

    private Duration getDurationFromTimeUnit() {
        return switch (timeUnit) {
            case MINUTES -> Duration.ofMinutes(1);
            case HOURS -> Duration.ofHours(1);
            case DAYS -> Duration.ofDays(1);
            default -> Duration.ofSeconds(1);
        };
    }

    @Getter
    @Setter
    public static class Document {
        private Description description;
        private String doc_id;
        private String doc_status;
        private String doc_type;
        private boolean importRequest;
        private String owner_inn;
        private String participant_inn;
        private String producer_inn;
        private String production_date;
        private String production_type;
        private Product[] products;
        private String reg_date;
        private String reg_number;
    }

    @Getter
    @Setter
    public static class Description {
        private String participantInn;
    }

    @Getter
    @Setter
    public static class Product {
        private String certificate_document;
        private String certificate_document_date;
        private String certificate_document_number;
        private String owner_inn;
        private String producer_inn;
        private String production_date;
        private String tnved_code;
        private String uit_code;
        private String uitu_code;
    }

    public static class ApiException extends RuntimeException {
        public ApiException(String message) {
            super(message);
        }

        public ApiException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}