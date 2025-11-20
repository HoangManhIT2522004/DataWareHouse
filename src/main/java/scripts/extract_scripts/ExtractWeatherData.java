package scripts.extract_scripts;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import utils.DBConn;
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
     * Extract weather data - TỰ XỬ LÝ TẤT CẢ
     * ============================================================
     */
    public static void extractWeatherToFile(
            LoadConfig config,
            String executionId,
            String dbSourceUrl,
            String dbOutputPath,
            DBConn controlDB
    ) {
        System.out.println("[Step 5] Extracting weather data...");
        System.out.println("  - Execution ID : " + executionId);
        System.out.println("  - Source URL   : " + dbSourceUrl);
        System.out.println("  - Output Path  : " + dbOutputPath);

        int successCount = 0;
        int failCount = 0;
        List<String> failedLocations = new ArrayList<>();

        try {
            // Lấy danh sách locations từ XML
            List<Location> locations = getLocationsFromConfig(config);
            System.out.println("  - Total locations: " + locations.size());

            if (locations.isEmpty()) {
                throw new RuntimeException("No locations found in config");
            }

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
            java.nio.file.Path filePath = java.nio.file.Paths.get(dbOutputPath);
            if (filePath.getParent() != null) {
                java.nio.file.Files.createDirectories(filePath.getParent());
            }

            // Tạo file CSV và ghi header
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(dbOutputPath))) {
                writer.write(getCsvHeader());
                writer.newLine();

                // Extract data cho từng location
                for (Location location : locations) {
                    try {
                        System.out.println("  Processing: " + location.getName() + " (" + location.getCode() + ")");

                        String url = buildApiUrl(baseUrl, apiKey, location.getApiName());

                        HttpRequest request = HttpRequest.newBuilder()
                                .uri(URI.create(url))
                                .timeout(Duration.ofMillis(timeout))
                                .GET()
                                .build();

                        HttpResponse<String> response = client.send(request,
                                HttpResponse.BodyHandlers.ofString());

                        if (response.statusCode() == 200) {
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

                        Thread.sleep(100);

                    } catch (Exception e) {
                        System.err.println("  ✗ Error: " + e.getMessage());
                        failedLocations.add(location.getName() + " (" + location.getCode() +
                                ") - " + e.getMessage());
                        failCount++;
                    }
                }
            }

            System.out.println("\n[Step 5] Extract completed:");
            System.out.println("  - Success: " + successCount);
            System.out.println("  - Failed : " + failCount);
            System.out.println("  - File   : " + dbOutputPath);

            // ✅ KIỂM TRA KẾT QUẢ
            if (successCount == 0) {
                // TẤT CẢ ĐỀU FAIL
                String errorMsg = String.format(
                        "Extract failed: 0/%d locations successful. All API calls failed.",
                        locations.size()
                );

                // Update log = FAILED
                updateLogFailed(controlDB, executionId, errorMsg);

                // Gửi email ERROR
                sendErrorEmail(executionId, errorMsg, failedLocations, dbOutputPath);

                // Throw exception để caller biết
                throw new RuntimeException(errorMsg);

            } else if (failCount > 0) {
                // MỘT SỐ FAIL, MỘT SỐ SUCCESS

                // Update log = SUCCESS (nhưng có warning)
                updateLogSuccess(controlDB, executionId, successCount);

                // Gửi email REPORT (có cảnh báo)
                sendExtractReport(executionId, successCount, failCount,
                        failedLocations, dbOutputPath);

            } else {
                // TẤT CẢ THÀNH CÔNG

                // Update log = SUCCESS
                updateLogSuccess(controlDB, executionId, successCount);

                // Gửi email SUCCESS
                sendSuccessEmail(executionId, successCount, dbOutputPath);
            }

        } catch (Exception e) {
            // ✅ BẮT LỖI BẤT NGỜ (không phải lỗi API)
            System.err.println("[Step 5] FAILED: Extract error");
            e.printStackTrace();

            // Update log = FAILED
            updateLogFailed(controlDB, executionId, e.getMessage());

            // Gửi email ERROR
            String subject = "✗ ERROR: Weather ETL - Extract Failed (System Error)";
            String body = String.format(
                    "=== EXTRACT FAILED ===\n\n" +
                            "Execution ID: %s\n" +
                            "Error Time: %s\n" +
                            "Error Type: System/Infrastructure Error\n" +
                            "Error Message: %s\n\n" +
                            "Stack Trace:\n%s\n\n" +
                            "Status: FAILED",
                    executionId,
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    e.getMessage(),
                    getStackTraceString(e)
            );
            utils.EmailSender.sendError(subject, body, e);

            // Re-throw để caller biết
            throw new RuntimeException("Extract failed", e);
        }
    }

    /**
     * Tạo tên file theo ngày: weatherapi_yyyymmdd.csv
     */
    public static String generateDailyFileName(String dbOutputPath) {
        dbOutputPath = dbOutputPath.replace("\\", "/");
        String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String fileName = "weatherapi_" + dateStr + ".csv";

        if (dbOutputPath.endsWith("/")) {
            return dbOutputPath + fileName;
        } else {
            int lastSeparator = dbOutputPath.lastIndexOf('/');
            if (lastSeparator != -1) {
                String directory = dbOutputPath.substring(0, lastSeparator + 1);
                return directory + fileName;
            } else {
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
        String[] parts = baseUrl.split("\\?");
        String basePath = parts[0];
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

        return String.format("%s,%s,%s,%s," +
                        "%.1f,%.1f,%.1f,%.1f," +
                        "%d,%.1f,%.1f,%d,%s," +
                        "%.1f,%.2f,%.1f,%.2f," +
                        "%d,%.1f,%.1f,%.1f," +
                        "\"%s\",%d," +
                        "%d,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f," +
                        "%s,%s",
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
     * Update log status về SUCCESS
     */
    private static void updateLogSuccess(DBConn controlDB, String executionId, int recordsCount) {
        try {
            System.out.println("[Step 5] Updating log status to SUCCESS...");

            String sql = String.format(
                    "UPDATE log_src SET status='success'::src_status, end_time=NOW(), " +
                            "records_extracted=%d, error_message=NULL WHERE execution_id='%s'",
                    recordsCount, executionId
            );

            controlDB.executeUpdate(sql);
            System.out.println("[Step 5] Log status updated to SUCCESS");
        } catch (Exception e) {
            System.err.println("[Step 5] WARNING: Cannot update log status: " + e.getMessage());
        }
    }

    /**
     * Update log status về FAILED
     */
    private static void updateLogFailed(DBConn controlDB, String executionId, String errorMessage) {
        try {
            System.out.println("[Step 5] Updating log status to FAILED...");

            String escapedError = errorMessage != null ? errorMessage.replace("'", "''") : "";

            String sql = String.format(
                    "UPDATE log_src SET status='failed'::src_status, end_time=NOW(), " +
                            "records_extracted=0, error_message='%s' WHERE execution_id='%s'",
                    escapedError, executionId
            );

            controlDB.executeUpdate(sql);
            System.out.println("[Step 5] Log status updated to FAILED");
        } catch (Exception e) {
            System.err.println("[Step 5] WARNING: Cannot update log status: " + e.getMessage());
        }
    }

    /**
     * Gửi email SUCCESS (tất cả locations thành công)
     */
    private static void sendSuccessEmail(String executionId, int successCount, String outputFile) {
        try {
            String subject = "✓ Weather ETL - Extract Completed Successfully";
            String body = String.format(
                    "=== EXTRACT SUCCESS ===\n\n" +
                            "Execution ID: %s\n" +
                            "Records Extracted: %d\n" +
                            "Output File: %s\n" +
                            "Completion Time: %s\n\n" +
                            "✓ All %d locations extracted successfully!\n\n" +
                            "Status: SUCCESS",
                    executionId,
                    successCount,
                    outputFile,
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),
                    successCount
            );
            utils.EmailSender.sendEmail(subject, body);
            System.out.println("[Step 5] Success email sent");
        } catch (Exception e) {
            System.err.println("[Step 5] Failed to send success email: " + e.getMessage());
        }
    }

    /**
     * Gửi email REPORT (một số thành công, một số fail)
     */
    private static void sendExtractReport(String executionId, int successCount, int failCount,
                                          List<String> failedLocations, String outputFile) {
        try {
            String subject = String.format("⚠ Weather ETL - Extract Completed with Warnings [%s]", executionId);
            StringBuilder body = new StringBuilder();
            body.append("=== EXTRACT REPORT ===\n\n");
            body.append("Execution ID: ").append(executionId).append("\n");
            body.append("Extract Time: ").append(LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
            body.append("Output File: ").append(outputFile).append("\n\n");

            body.append("--- SUMMARY ---\n");
            body.append("Total Locations: ").append(successCount + failCount).append("\n");
            body.append("✓ Success: ").append(successCount).append("\n");
            body.append("✗ Failed: ").append(failCount).append("\n\n");

            body.append("--- FAILED LOCATIONS ---\n");
            for (int i = 0; i < failedLocations.size(); i++) {
                body.append(String.format("%d. %s\n", i + 1, failedLocations.get(i)));
            }
            body.append("\nVui lòng kiểm tra:\n");
            body.append("- API key có còn hợp lệ?\n");
            body.append("- Tên tỉnh thành có đúng format?\n");
            body.append("- Network connection?\n");
            body.append("- Rate limit của API?\n\n");
            body.append("Status: SUCCESS (with warnings)");

            utils.EmailSender.sendEmail(subject, body.toString());
            System.out.println("[Step 5] Extract report email sent");
        } catch (Exception e) {
            System.err.println("[Step 5] Failed to send report email: " + e.getMessage());
        }
    }

    /**
     * Gửi email ERROR (tất cả locations fail)
     */
    private static void sendErrorEmail(String executionId, String errorMessage,
                                       List<String> failedLocations, String outputFile) {
        try {
            String subject = "✗ ERROR: Weather ETL - Extract Failed";
            StringBuilder body = new StringBuilder();
            body.append("=== EXTRACT FAILED ===\n\n");
            body.append("Execution ID: ").append(executionId).append("\n");
            body.append("Error Time: ").append(LocalDateTime.now()
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))).append("\n");
            body.append("Output File: ").append(outputFile).append("\n");
            body.append("Error Message: ").append(errorMessage).append("\n\n");

            body.append("--- ALL LOCATIONS FAILED ---\n");
            for (int i = 0; i < failedLocations.size(); i++) {
                body.append(String.format("%d. %s\n", i + 1, failedLocations.get(i)));
            }
            body.append("\nVui lòng kiểm tra NGAY:\n");
            body.append("- API key có còn hợp lệ không?\n");
            body.append("- API service có đang down không?\n");
            body.append("- Network connection?\n");
            body.append("- Firewall/Proxy settings?\n\n");
            body.append("Status: FAILED");

            utils.EmailSender.sendError(subject, body.toString(), new RuntimeException(errorMessage));
            System.out.println("[Step 5] Error email sent");
        } catch (Exception e) {
            System.err.println("[Step 5] Failed to send error email: " + e.getMessage());
        }
    }

    /**
     * Helper: Get stack trace as string
     */
    private static String getStackTraceString(Exception e) {
        StringBuilder sb = new StringBuilder();
        for (StackTraceElement element : e.getStackTrace()) {
            sb.append("  at ").append(element.toString()).append("\n");
            if (sb.length() > 1000) {
                sb.append("  ...");
                break;
            }
        }
        return sb.toString();
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
        private final String name;
        private final String apiName;
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