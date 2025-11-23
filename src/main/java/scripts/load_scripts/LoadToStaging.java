package scripts.load_scripts;

import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;
import org.w3c.dom.Element;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class LoadToStaging {

    private static DBConn controlDB;
    private static DBConn stagingDB;

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("   WEATHER ETL - STEP 6: LOAD TO RAW");
        System.out.println("========================================");

        try {
            // 1. Load Config
            LoadConfig config = new LoadConfig("config/config.xml");

            // 2. Kết nối DB
            controlDB = connectControlDB(config);
            stagingDB = connectStagingDB(config);

            // 3. Kiểm tra xem hôm nay đã chạy Load chưa
            checkTodayProcessSuccess();

            // 4. Kiểm tra file CSV đầu vào
            String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
            // LƯU Ý: Kiểm tra lại đường dẫn này trên máy bạn
            String csvPath = "D:/DataWareHouse/src/main/java/data/weatherapi_" + dateStr + ".csv";
            File file = new File(csvPath);

            if (!file.exists()) {
                String msg = "Không tìm thấy file dữ liệu: " + csvPath + ".\nCó thể bước Extract chưa chạy hoặc bị lỗi.";
                System.err.println("❌ " + msg);
                EmailSender.sendError("ETL Alert: Load Aborted", msg, new Exception("File Missing"));
                System.exit(1);
            }

            // 5. Chuẩn bị Log Process
            String loadExecutionId = prepareLoadProcess(csvPath);

            // 6. Xóa sạch dữ liệu cũ trong bảng Raw
            truncateRawTables(stagingDB);

            // 7. Thực hiện Load dữ liệu
            int loadedCount = loadToRawTables(file, stagingDB, loadExecutionId);

            // 8. Cập nhật Log thành SUCCESS
            updateProcessLogStatus(loadExecutionId, "success", loadedCount, 0, "Loaded successfully");

            // 9. Archive file
            archiveFile(file);

            System.out.println("\n========================================");
            System.out.println("✅ LOAD PROCESS COMPLETED SUCCESSFULLY");
            System.out.println("========================================");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("\n❌ LOAD PROCESS FAILED");
            e.printStackTrace();
            EmailSender.sendError("ETL Error: Load To Raw Failed", e.getMessage(), e);
            System.exit(1);
        }
    }

    // ============================================================
    // HELPER METHODS (Data Access)
    // ============================================================

    /**
     * Lấy dữ liệu an toàn từ row. Trả về null nếu cột không tồn tại trong CSV.
     */
    private static String safeGet(String[] row, Map<String, Integer> map, String colName) {
        Integer index = map.get(colName);
        if (index == null || index >= row.length) {
            return null;
        }
        return row[index];
    }

    private static void truncateRawTables(DBConn dbConn) {
        System.out.println("🧹 [Cleaning] Truncating RAW tables...");
        try {
            String sql = "TRUNCATE TABLE raw_weather_location, raw_weather_condition, " +
                    "raw_air_quality, raw_weather_observation RESTART IDENTITY CASCADE";
            dbConn.executeUpdate(sql);
            System.out.println("✅ Raw tables truncated.");
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Failed to truncate tables: " + e.getMessage());
        }
    }

    private static void archiveFile(File file) {
        try {
            String archiveDir = "data/archive/";
            File dir = new File(archiveDir);
            if (!dir.exists()) dir.mkdirs();

            Path source = file.toPath();
            Path dest = Paths.get(archiveDir + file.getName());

            Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("📦 [Archive] File moved to: " + dest.toString());
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Could not archive file: " + e.getMessage());
        }
    }

    // ============================================================
    // CONTROL METHODS
    // ============================================================

    private static DBConn connectControlDB(LoadConfig config) throws Exception {
        Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
        return new DBConn(
                LoadConfig.getValue(control, "url"),
                LoadConfig.getValue(control, "username"),
                LoadConfig.getValue(control, "password")
        );
    }

    private static DBConn connectStagingDB(LoadConfig config) throws Exception {
        Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
        Element staging = LoadConfig.getChildElement(database, "staging");
        return new DBConn(
                LoadConfig.getValue(staging, "url"),
                LoadConfig.getValue(staging, "username"),
                LoadConfig.getValue(staging, "password")
        );
    }

    private static void checkTodayProcessSuccess() {
        try {
            String sql = "SELECT check_today_loadstaging_success_prefix('LOD_STG') AS success";
            final boolean[] alreadyLoaded = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) alreadyLoaded[0] = rs.getBoolean("success");
            });

            if (alreadyLoaded[0]) {
                System.out.println("⚠️ Hôm nay đã chạy Load thành công. Dừng tiến trình.");
                System.exit(0);
            }
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Lỗi kiểm tra log. Vẫn tiếp tục.");
        }
    }

    private static String prepareLoadProcess(String csvPath) throws Exception {
        System.out.println("[Prepare] Creating process log entry...");
        String processName = "LOD_STG_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        String getConfigSql = String.format(
                "SELECT get_or_create_config_loadstaging('%s', 'load_staging', '%s', 'staging', 'raw_weather_tables')",
                processName, csvPath
        );

        final int[] configId = {0};
        controlDB.executeQuery(getConfigSql, rs -> {
            if (rs.next()) configId[0] = rs.getInt(1);
        });

        if (configId[0] == 0) throw new Exception("Failed to get config_process ID");

        String createLogSql = "SELECT create_new_loadstaging_log(" + configId[0] + ")";
        final String[] execId = {null};
        controlDB.executeQuery(createLogSql, rs -> {
            if (rs.next()) execId[0] = rs.getString(1);
        });

        return execId[0];
    }

    private static void updateProcessLogStatus(String execId, String status, int inserted, int failed, String message) {
        try {
            String sql = "SELECT update_loadstaging_log_status(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {}, execId, status, inserted, failed, message);
        } catch (Exception e) {
            System.err.println("❌ Failed to update log status: " + e.getMessage());
        }
    }

    // ============================================================
    // CORE LOAD LOGIC (Đã xoá last_updated_epoch)
    // ============================================================

    public static int loadToRawTables(File file, DBConn dbConn, String loadExecId) throws Exception {
        System.out.println("[Process] Loading data to Raw Tables...");
        Connection conn = null;
        PreparedStatement psLoc = null, psCond = null, psAir = null, psObs = null;

        // 1. Location
        String sqlLoc = "INSERT INTO raw_weather_location (name, region, country, lat, lon, tz_id, \"localtime\", localtime_epoch, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        // 2. Condition
        String sqlCond = "INSERT INTO raw_weather_condition (code, text, icon, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?::jsonb)";

        // 3. Air Quality
        String sqlAir = "INSERT INTO raw_air_quality (co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        // 4. Observation (ĐÃ XOÁ last_updated_epoch - còn 25 params)
        String sqlObs = "INSERT INTO raw_weather_observation (" +
                "last_updated, is_day, temp_c, temp_f, feelslike_c, feelslike_f, " +
                "humidity, cloud, vis_km, vis_miles, uv, " +
                "gust_mph, gust_kph, " +
                "wind_mph, wind_kph, wind_degree, wind_dir, " +
                "pressure_mb, pressure_in, precip_mm, precip_in, " +
                "location_name, source_system, batch_id, raw_payload" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {

            conn = dbConn.getConnection();
            conn.setAutoCommit(false);

            psLoc = conn.prepareStatement(sqlLoc);
            psCond = conn.prepareStatement(sqlCond);
            psAir = conn.prepareStatement(sqlAir);
            psObs = conn.prepareStatement(sqlObs);

            String line;
            Map<String, Integer> map = new HashMap<>();

            if ((line = br.readLine()) != null) {
                if (line.startsWith("\uFEFF")) line = line.substring(1);
                String[] headers = line.split(",");
                for (int i = 0; i < headers.length; i++) map.put(headers[i].trim(), i);
            }

            Gson gson = new Gson();

            while ((line = br.readLine()) != null) {
                try {
                    String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    for (int i = 0; i < row.length; i++) row[i] = row[i].replaceAll("^\"|\"$", "");

                    String extractBatchId = safeGet(row, map, "execution_id");
                    String source = "WeatherAPI";

                    JsonObject json = new JsonObject();
                    for (String h : map.keySet()) {
                        String val = safeGet(row, map, h);
                        if(val != null) json.addProperty(h, val);
                    }
                    json.addProperty("load_execution_id", loadExecId);
                    String jsonStr = gson.toJson(json);

                    // --- 1. LOCATION ---
                    psLoc.setString(1, safeGet(row, map, "location_name"));
                    psLoc.setString(2, safeGet(row, map, "region"));
                    psLoc.setString(3, "Vietnam");
                    try { psLoc.setDouble(4, Double.parseDouble(safeGet(row, map, "lat"))); } catch (Exception e) { psLoc.setObject(4, null); }
                    try { psLoc.setDouble(5, Double.parseDouble(safeGet(row, map, "lon"))); } catch (Exception e) { psLoc.setObject(5, null); }
                    psLoc.setString(6, safeGet(row, map, "tz_id"));
                    psLoc.setString(7, safeGet(row, map, "localtime"));

                    // localtime_epoch vẫn giữ
                    try {
                        String val = safeGet(row, map, "localtime_epoch");
                        psLoc.setLong(8, val != null ? Long.parseLong(val) : 0);
                        if (val == null) psLoc.setObject(8, null);
                    } catch (Exception e) { psLoc.setObject(8, null); }

                    psLoc.setString(9, source);
                    psLoc.setString(10, extractBatchId);
                    psLoc.setString(11, jsonStr);
                    psLoc.addBatch();

                    // --- 2. CONDITION ---
                    psCond.setString(1, safeGet(row, map, "condition_code"));
                    psCond.setString(2, safeGet(row, map, "condition_text"));

                    String iconVal = safeGet(row, map, "condition_icon");
                    if(iconVal == null) iconVal = safeGet(row, map, "icon");
                    psCond.setString(3, iconVal);

                    psCond.setString(4, source);
                    psCond.setString(5, extractBatchId);
                    psCond.setString(6, jsonStr);
                    psCond.addBatch();

                    // --- 3. AIR QUALITY ---
                    psAir.setString(1, safeGet(row, map, "co"));
                    psAir.setString(2, safeGet(row, map, "no2"));
                    psAir.setString(3, safeGet(row, map, "o3"));
                    psAir.setString(4, safeGet(row, map, "so2"));
                    psAir.setString(5, safeGet(row, map, "pm2_5"));
                    psAir.setString(6, safeGet(row, map, "pm10"));
                    psAir.setString(7, safeGet(row, map, "aqi_us"));
                    psAir.setString(8, safeGet(row, map, "aqi_gb"));
                    psAir.setString(9, source);
                    psAir.setString(10, extractBatchId);
                    psAir.setString(11, jsonStr);
                    psAir.addBatch();

                    // --- 4. OBSERVATION ---
                    int idx = 1;
                    psObs.setString(idx++, safeGet(row, map, "last_updated")); // 1

                    // ĐÃ XOÁ last_updated_epoch

                    // is_day
                    try {
                        String val = safeGet(row, map, "is_day");
                        psObs.setInt(idx++, val != null ? Integer.parseInt(val) : 0);
                        if (val == null) psObs.setObject(idx-1, null);
                    } catch (Exception e) { psObs.setObject(idx-1, null); }

                    psObs.setString(idx++, safeGet(row, map, "temp_c"));
                    psObs.setString(idx++, safeGet(row, map, "temp_f"));
                    psObs.setString(idx++, safeGet(row, map, "feels_like_c"));
                    psObs.setString(idx++, safeGet(row, map, "feels_like_f"));
                    psObs.setString(idx++, safeGet(row, map, "humidity"));
                    psObs.setString(idx++, safeGet(row, map, "cloud"));
                    psObs.setString(idx++, safeGet(row, map, "vis_km"));
                    psObs.setString(idx++, safeGet(row, map, "vis_miles"));
                    psObs.setString(idx++, safeGet(row, map, "uv"));
                    psObs.setString(idx++, safeGet(row, map, "gust_mph"));
                    psObs.setString(idx++, safeGet(row, map, "gust_kph"));
                    psObs.setString(idx++, safeGet(row, map, "wind_mph"));
                    psObs.setString(idx++, safeGet(row, map, "wind_kph"));
                    psObs.setString(idx++, safeGet(row, map, "wind_degree"));
                    psObs.setString(idx++, safeGet(row, map, "wind_dir"));
                    psObs.setString(idx++, safeGet(row, map, "pressure_mb"));
                    psObs.setString(idx++, safeGet(row, map, "pressure_in"));
                    psObs.setString(idx++, safeGet(row, map, "precip_mm"));
                    psObs.setString(idx++, safeGet(row, map, "precip_in"));
                    psObs.setString(idx++, safeGet(row, map, "location_name"));
                    psObs.setString(idx++, source);
                    psObs.setString(idx++, extractBatchId);
                    psObs.setString(idx++, jsonStr);
                    psObs.addBatch();

                    count++;
                    if (count % 100 == 0) {
                        psLoc.executeBatch();
                        psCond.executeBatch();
                        psAir.executeBatch();
                        psObs.executeBatch();
                    }
                } catch (Exception e) {
                    System.err.println("  ⚠️ Skip row error: " + e.toString());
                }
            }

            psLoc.executeBatch();
            psCond.executeBatch();
            psAir.executeBatch();
            psObs.executeBatch();
            conn.commit();
            System.out.println("✅ Inserted " + count + " rows into RAW tables.");
            return count;

        } catch (Exception e) {
            try { if (conn != null) conn.rollback(); } catch (SQLException ex) {}
            updateProcessLogStatus(loadExecId, "failed", 0, 0, e.getMessage());
            throw e;
        } finally {
            try { if (psLoc != null) psLoc.close(); } catch (SQLException ex) {}
            try { if (psCond != null) psCond.close(); } catch (SQLException ex) {}
            try { if (psAir != null) psAir.close(); } catch (SQLException ex) {}
            try { if (psObs != null) psObs.close(); } catch (SQLException ex) {}
            try { if (conn != null) conn.close(); } catch (SQLException ex) {}
        }
    }
}
