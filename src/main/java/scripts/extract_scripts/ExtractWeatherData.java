package scripts.extract_scripts;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import utils.LoadConfig;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ExtractWeatherData {

    /**
     * ============================================================
     * Step 5: Extract weather data và ghi vào file CSV
     * Trả về số lượng records đã extract thành công
     * ============================================================
     */
    public static int extractWeatherToFile(
            LoadConfig config,
            String executionId,
            String dbSourceUrl,
            String dbOutputPath
    ) {
        System.out.println("[Step 5] Extracting weather data...");
        System.out.println("  - Execution ID : " + executionId);
        System.out.println("  - Source URL   : " + dbSourceUrl);

        // Tạo tên file theo ngày: weatherapi_yyyymmdd.csv
        String dailyFileName = generateDailyFileName(dbOutputPath);

        System.out.println("  - Output Path  : " + dailyFileName);

        int successCount = 0;

        try {
            // Lấy danh sách locations từ XML
            List<Location> locations = getLocationsFromConfig(config);
            System.out.println("  - Total locations: " + locations.size());

            // Lấy API config
            Element api = LoadConfig.getElement(config.getXmlDoc(), "api");
            Element weather = LoadConfig.getChildElement(api, "weather");
            String apiKey = LoadConfig.getValue(weather, "apiKey");
            String baseUrl = LoadConfig.getValue(weather, "baseUrl");
            int timeout = Integer.parseInt(LoadConfig.getValue(weather, "timeout"));

            // Tạo HTTP Client
            HttpClient client = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofMillis(timeout))
                    .build();

            // Tạo thư mục cha nếu chưa tồn tại
            java.nio.file.Path filePath = java.nio.file.Paths.get(dailyFileName);
            if (filePath.getParent() != null) {
                java.nio.file.Files.createDirectories(filePath.getParent());
            }

            // Tạo file CSV và ghi header
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dailyFileName))) {
                // Ghi header
                writer.write(getCsvHeader());
                writer.newLine();

                int failCount = 0;
                List<String> failedLocations = new ArrayList<>();

                // Extract data cho từng location
                for (Location location : locations) {
                    try {
                        System.out.println("  Processing: " + location.getName() + " (" + location.getCode() + ")");

                        // Build URL với apiName (có thể là tên hoặc tọa độ)
                        String url = buildApiUrl(baseUrl, apiKey, location.getApiName());

                        // Gọi API
                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofMillis(timeout))
                                .GET()
                                .build();

                        HttpResponse<String> response = client.send(request,
                                HttpResponse.BodyHandlers.ofString());

                        if (response.statusCode() == 200) {
                            // Parse JSON và ghi vào CSV
                            String csvRow = parseWeatherResponse(response.body(), location, executionId);
                            writer.write(csvRow);
                            writer.newLine();
                            successCount++;
                            System.out.println("  ✓ Success");
                        } else {
                            System.err.println("  ✗ Failed: HTTP " + response.statusCode());
                            failedLocations.add(location.getName() + " (" + location.getCode() +
                                    ") - HTTP " + response.statusCode());
                            failCount++;
                        }

                        // Delay để tránh rate limit
                        Thread.sleep(100);

                    } catch (Exception e) {
                        System.err.println("  ✗ Error: " + e.getMessage());
                        failedLocations.add(location.getName() + " (" + location.getCode() +
                                ") - " + e.getMessage());
                        failCount++;
                    }
                }

                System.out.println("\n[Step 5] Extract completed:");
                System.out.println("  - Success: " + successCount);
                System.out.println("  - Failed : " + failCount);
                System.out.println("  - File   : " + dailyFileName);

                // Gửi email báo cáo kết quả
                sendExtractReport(executionId, successCount, failCount, failedLocations, dailyFileName);
            }

        } catch (Exception e) {
            System.err.println("[Step 5] FAILED: Extract error");
            e.printStackTrace();
            throw new RuntimeException("Extract failed", e);
        }

        return successCount;
    }

    /**
     * Tạo tên file theo ngày: weatherapi_yyyymmdd.csv
     * Nếu dbOutputPath là thư mục -> tạo file mới trong thư mục đó
     * Nếu dbOutputPath là file path -> thay thế tên file
     * Chuẩn hóa path separator thành /
     */
    private static String generateDailyFileName(String dbOutputPath) {
        // Chuẩn hóa path separator thành /
        dbOutputPath = dbOutputPath.replace("\\", "/");

        // Format ngày: yyyyMMdd (VD: 20251118)
        String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String fileName = "weatherapi_" + dateStr + ".csv";

        // Kiểm tra nếu dbOutputPath là thư mục hay file path
        if (dbOutputPath.endsWith("/")) {
            // Là thư mục -> append tên file
            return dbOutputPath + fileName;
        } else {
            // Là file path -> lấy thư mục cha và thay tên file
            int lastSeparator = dbOutputPath.lastIndexOf('/');

            if (lastSeparator != -1) {
                String directory = dbOutputPath.substring(0, lastSeparator + 1);
                return directory + fileName;
            } else {
                // Không có thư mục -> tạo file ở thư mục hiện tại
                return fileName;
            }
        }
    }

    /**
     * Lấy danh sách locations từ XML config
     */
    private static List<Location> getLocationsFromConfig(LoadConfig config) {
        List<Location> locations = new ArrayList<>();
        Element locationsElement = LoadConfig.getElement(config.getXmlDoc(), "locations");

        if (locationsElement != null) {
            NodeList locationNodes = locationsElement.getElementsByTagName("location");
            for (int i = 0; i < locationNodes.getLength(); i++) {
                Element locationElement = (Element) locationNodes.item(i);
                String name = LoadConfig.getValue(locationElement, "name");
                String apiName = LoadConfig.getValue(locationElement, "apiName");
                String code = LoadConfig.getValue(locationElement, "code");
                String region = LoadConfig.getValue(locationElement, "region");
                locations.add(new Location(name, apiName, code, region));
            }
        }
        return locations;
    }

    /**
     * Build API URL với location name hoặc tọa độ
     */
    private static String buildApiUrl(String baseUrl, String apiKey, String locationQuery) {
        // Extract base path từ baseUrl
        String[] parts = baseUrl.split("\\?");
        String basePath = parts[0];

        // Build URL mới với location (có thể là tên hoặc lat,lon)
        return String.format("%s?key=%s&q=%s&aqi=yes",
                basePath, apiKey, locationQuery.replace(" ", "%20"));
    }

    /**
     * CSV Header
     */
    private static String getCsvHeader() {
        return "execution_id,location_name,location_code,region," +
                "temp_c,temp_f,feels_like_c,feels_like_f," +
                "humidity,wind_kph,wind_mph,wind_degree,wind_dir," +
                "pressure_mb,pressure_in,precip_mm,precip_in," +
                "cloud,uv,vis_km,vis_miles," +
                "condition_text,condition_code," +
                "aqi_us,aqi_gb,pm2_5,pm10,co,no2,o3,so2," +
                "last_updated,extract_time";
    }

    /**
     * Parse JSON response và tạo CSV row
     */
    private static String parseWeatherResponse(String jsonResponse, Location location, String executionId) {
        JsonObject root = JsonParser.parseString(jsonResponse).getAsJsonObject();
        JsonObject current = root.getAsJsonObject("current");
        JsonObject condition = current.getAsJsonObject("condition");
        JsonObject airQuality = current.getAsJsonObject("air_quality");

        String extractTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        return String.format("%s,%s,%s,%s," +                    // execution_id, location info
                        "%.1f,%.1f,%.1f,%.1f," +                 // temp, feels_like
                        "%d,%.1f,%.1f,%d,%s," +                  // humidity, wind
                        "%.1f,%.2f,%.1f,%.2f," +                 // pressure, precip
                        "%d,%.1f,%.1f,%.1f," +                   // cloud, uv, vis
                        "\"%s\",%d," +                           // condition
                        "%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f," + // air quality
                        "%s,%s",                                  // timestamps
                executionId,
                escapeComma(location.getName()),
                location.getCode(),
                location.getRegion(),
                current.get("temp_c").getAsDouble(),
                current.get("temp_f").getAsDouble(),
                current.get("feelslike_c").getAsDouble(),
                current.get("feelslike_f").getAsDouble(),
                current.get("humidity").getAsInt(),
                current.get("wind_kph").getAsDouble(),
                current.get("wind_mph").getAsDouble(),
                current.get("wind_degree").getAsInt(),
                current.get("wind_dir").getAsString(),
                current.get("pressure_mb").getAsDouble(),
                current.get("pressure_in").getAsDouble(),
                current.get("precip_mm").getAsDouble(),
                current.get("precip_in").getAsDouble(),
                current.get("cloud").getAsInt(),
                current.get("uv").getAsDouble(),
                current.get("vis_km").getAsDouble(),
                current.get("vis_miles").getAsDouble(),
                condition.get("text").getAsString(),
                condition.get("code").getAsInt(),
                airQuality.get("us-epa-index").getAsInt(),
                airQuality.get("gb-defra-index").getAsInt(),
                airQuality.get("pm2_5").getAsDouble(),
                airQuality.get("pm10").getAsDouble(),
                airQuality.get("co").getAsDouble(),
                airQuality.get("no2").getAsDouble(),
                airQuality.get("o3").getAsDouble(),
                airQuality.get("so2").getAsDouble(),
                current.get("last_updated").getAsString(),
                extractTime
        );
    }

    /**
     * Escape comma trong CSV
     */
    private static String escapeComma(String value) {
        if (value.contains(",")) {
            return "\"" + value + "\"";
        }
        return value;
    }

    /**
     * Gửi email báo cáo kết quả extract
     */
    private static void sendExtractReport(String executionId, int successCount, int failCount,
                                          List<String> failedLocations, String outputFile) {
        try {
            String subject = String.format("Weather ETL - Extract Report [%s]", executionId);
            StringBuilder body = new StringBuilder();
            body.append("=== EXTRACT REPORT ===\n\n");
            body.append("Execution ID: ").append(executionId).append("\n");
            body.append("Extract Time: ").append(LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
            body.append("Output File: ").append(outputFile).append("\n\n");

            body.append("--- SUMMARY ---\n");
            body.append("Total Locations: ").append(successCount + failCount).append("\n");
            body.append("Success: ").append(successCount).append("\n");
            body.append("Failed: ").append(failCount).append("\n\n");

            if (failCount > 0) {
                body.append("--- FAILED LOCATIONS ---\n");
                for (int i = 0; i < failedLocations.size(); i++) {
                    body.append(String.format("%d. %s\n", i + 1, failedLocations.get(i)));
                }
                body.append("\nVui lòng kiểm tra:\n");
                body.append("- API key có còn hợp lệ?\n");
                body.append("- Tên tỉnh thành có đúng format?\n");
                body.append("- Network connection?\n");
                body.append("- Rate limit của API?\n");
            } else {
                body.append("✓ All locations extracted successfully!\n");
            }

            // Import EmailSender từ utils
            utils.EmailSender.sendEmail(subject, body.toString());
            System.out.println("[Step 5] Extract report email sent successfully");

        } catch (Exception e) {
            System.err.println("[Step 5] Failed to send extract report email: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Inner class để lưu extract info
     */
    public static class ExtractInfo {
        private final String executionId;
        private final String sourceUrl;
        private final String outputPath;

        public ExtractInfo(String executionId, String sourceUrl, String outputPath) {
            this.executionId = executionId;
            this.sourceUrl = sourceUrl;
            this.outputPath = outputPath;
        }

        public String getExecutionId() {
            return executionId;
        }

        public String getSourceUrl() {
            return sourceUrl;
        }

        public String getOutputPath() {
            return outputPath;
        }
    }

    /**
     * Inner class để lưu location info
     */
    static class Location {
        private final String name;      // Tên hiển thị (có dấu)
        private final String apiName;   // Tên/Tọa độ dùng cho API
        private final String code;
        private final String region;

        public Location(String name, String apiName, String code, String region) {
            this.name = name;
            this.apiName = apiName;
            this.code = code;
            this.region = region;
        }

        public String getName() {
            return name;
        }

        public String getApiName() {
            return apiName;
        }

        public String getCode() {
            return code;
        }

        public String getRegion() {
            return region;
        }
    }
}