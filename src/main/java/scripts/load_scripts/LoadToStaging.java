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

            // 9. Archive file (Bây giờ sẽ thành công vì file đã được đóng)
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
    // HELPER METHODS (Archive & Clean)
    // ============================================================

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

            // Di chuyển file
            Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("📦 [Archive] File moved to: " + dest.toString());
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Could not archive file: " + e.getMessage());
            e.printStackTrace();
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
            System.out.println("[Check] Checking today's process status...");
            String sql = "SELECT check_today_loadstaging_success_prefix('LOD_STG') AS success";
            final boolean[] alreadyLoaded = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) alreadyLoaded[0] = rs.getBoolean("success");
            });

            if (alreadyLoaded[0]) {
                String msg = "Hôm nay đã chạy Load thành công. Dừng tiến trình.";
                System.out.println("⚠️ " + msg);
                EmailSender.sendEmail("ETL Notification: Load Already Done", msg);
                System.exit(0);
            } else {
                System.out.println("✅ Chưa chạy Load hôm nay. Tiếp tục...");
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

        if (configId[0] == 0) throw new Exception("Failed to get/create config_process ID");

        String createLogSql = "SELECT create_new_loadstaging_log(" + configId[0] + ")";
        final String[] execId = {null};
        controlDB.executeQuery(createLogSql, rs -> {
            if (rs.next()) execId[0] = rs.getString(1);
        });

        if (execId[0] == null) throw new Exception("Failed to create log_process entry");
        System.out.println("[Prepare] Created Execution ID: " + execId[0]);
        return execId[0];
    }

    private static void updateProcessLogStatus(String execId, String status, int inserted, int failed, String message) {
        try {
            String sql = "SELECT update_loadstaging_log_status(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {
            }, execId, status, inserted, failed, message);
            System.out.println("[Log] Updated status to: " + status);
        } catch (Exception e) {
            System.err.println("❌ Failed to update log status: " + e.getMessage());
        }
    }

    // ============================================================
    // CORE LOAD LOGIC (ĐÃ SỬA FILE LOCK)
    // ============================================================

    public static int loadToRawTables(File file, DBConn dbConn, String loadExecId) throws Exception {
        System.out.println("[Process] Loading data to Raw Tables...");
        Connection conn = null;
        PreparedStatement psLoc = null, psCond = null, psAir = null, psObs = null;

        String sqlLoc = "INSERT INTO raw_weather_location (name, region, country, lat, lon, tz_id, \"localtime\", source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        String sqlCond = "INSERT INTO raw_weather_condition (code, text, icon, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?::jsonb)";
        String sqlAir = "INSERT INTO raw_air_quality (co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        // --- ĐÃ SỬA: Thêm gust_mph, gust_kph vào sau cột uv ---
        String sqlObs = "INSERT INTO raw_weather_observation (" +
                "last_updated, temp_c, temp_f, feelslike_c, feelslike_f, " +
                "humidity, cloud, vis_km, vis_miles, uv, " +
                "gust_mph, gust_kph, " + // <--- MỚI THÊM
                "wind_mph, wind_kph, wind_degree, wind_dir, " +
                "pressure_mb, pressure_in, precip_mm, precip_in, " +
                "location_name, source_system, batch_id, raw_payload" +
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {

            conn = dbConn.getConnection();
            conn.setAutoCommit(false);

            psLoc = conn.prepareStatement(sqlLoc);
            psCond = conn.prepareStatement(sqlCond);
            psAir = conn.prepareStatement(sqlAir);
            psObs = conn.prepareStatement(sqlObs);

            String line;
            String[] headers = null;
            Map<String, Integer> map = new HashMap<>();

            if ((line = br.readLine()) != null) {
                if (line.startsWith("\uFEFF")) line = line.substring(1);
                headers = line.split(",");
                for (int i = 0; i < headers.length; i++) map.put(headers[i].trim(), i);
            }

            Gson gson = new Gson();

            while ((line = br.readLine()) != null) {
                try {
                    String[] row = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                    for (int i = 0; i < row.length; i++) row[i] = row[i].replaceAll("^\"|\"$", "");

                    String extractBatchId = row[map.get("execution_id")];
                    String source = "WeatherAPI";

                    JsonObject json = new JsonObject();
                    for (String h : map.keySet()) if (map.get(h) < row.length) json.addProperty(h, row[map.get(h)]);
                    json.addProperty("load_execution_id", loadExecId);
                    String jsonStr = gson.toJson(json);

                    // 1. Location
                    psLoc.setString(1, row[map.get("location_name")]);
                    psLoc.setString(2, row[map.get("region")]);
                    psLoc.setString(3, "Vietnam");
                    try {
                        psLoc.setString(4, row[map.get("lat")]);
                    } catch (Exception e) {
                        psLoc.setObject(4, null);
                    }
                    try {
                        psLoc.setString(5, row[map.get("lon")]);
                    } catch (Exception e) {
                        psLoc.setObject(5, null);
                    }
                    try {
                        psLoc.setString(6, row[map.get("tz_id")]);
                    } catch (Exception e) {
                        psLoc.setObject(6, null);
                    }
                    psLoc.setString(7, row[map.get("last_updated")]);
                    psLoc.setString(8, source);
                    psLoc.setString(9, extractBatchId);
                    psLoc.setString(10, jsonStr);
                    psLoc.addBatch();

                    // 2. Condition
                    psCond.setString(1, row[map.get("condition_code")]);
                    psCond.setString(2, row[map.get("condition_text")]);
                    psCond.setObject(3, null);
                    psCond.setString(4, source);
                    psCond.setString(5, extractBatchId);
                    psCond.setString(6, jsonStr);
                    psCond.addBatch();

                    // 3. Air Quality
                    psAir.setString(1, row[map.get("co")]);
                    psAir.setString(2, row[map.get("no2")]);
                    psAir.setString(3, row[map.get("o3")]);
                    psAir.setString(4, row[map.get("so2")]);
                    psAir.setString(5, row[map.get("pm2_5")]);
                    psAir.setString(6, row[map.get("pm10")]);
                    psAir.setString(7, row[map.get("aqi_us")]);
                    psAir.setString(8, row[map.get("aqi_gb")]);
                    psAir.setString(9, source);
                    psAir.setString(10, extractBatchId);
                    psAir.setString(11, jsonStr);
                    psAir.addBatch();

                    // 4. Observation (Đã thêm gust_mph, gust_kph sau uv)
                    int idx = 1;
                    psObs.setString(idx++, row[map.get("last_updated")]);
                    psObs.setString(idx++, row[map.get("temp_c")]);
                    psObs.setString(idx++, row[map.get("temp_f")]);
                    psObs.setString(idx++, row[map.get("feels_like_c")]);
                    psObs.setString(idx++, row[map.get("feels_like_f")]);
                    psObs.setString(idx++, row[map.get("humidity")]);
                    psObs.setString(idx++, row[map.get("cloud")]);
                    psObs.setString(idx++, row[map.get("vis_km")]);
                    psObs.setString(idx++, row[map.get("vis_miles")]);
                    psObs.setString(idx++, row[map.get("uv")]);

                    // --- MỚI THÊM ---
                    psObs.setString(idx++, row[map.get("gust_mph")]);
                    psObs.setString(idx++, row[map.get("gust_kph")]);
                    // ----------------

                    psObs.setString(idx++, row[map.get("wind_mph")]);
                    psObs.setString(idx++, row[map.get("wind_kph")]);
                    psObs.setString(idx++, row[map.get("wind_degree")]);
                    psObs.setString(idx++, row[map.get("wind_dir")]);
                    psObs.setString(idx++, row[map.get("pressure_mb")]);
                    psObs.setString(idx++, row[map.get("pressure_in")]);
                    psObs.setString(idx++, row[map.get("precip_mm")]);
                    psObs.setString(idx++, row[map.get("precip_in")]);
                    psObs.setString(idx++, row[map.get("location_name")]);
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
                    System.err.println("  ⚠️ Skip row: " + e.getMessage());
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
            try {
                if (conn != null) conn.rollback();
            } catch (SQLException ex) {
            }
            updateProcessLogStatus(loadExecId, "failed", 0, 0, e.getMessage());
            throw e;
        } finally {
            try {
                if (psLoc != null) psLoc.close();
            } catch (SQLException ex) {
            }
            try {
                if (psCond != null) psCond.close();
            } catch (SQLException ex) {
            }
            try {
                if (psAir != null) psAir.close();
            } catch (SQLException ex) {
            }
            try {
                if (psObs != null) psObs.close();
            } catch (SQLException ex) {
            }
            try {
                if (conn != null) conn.close();
            } catch (SQLException ex) {
            }
        }
    }
}
