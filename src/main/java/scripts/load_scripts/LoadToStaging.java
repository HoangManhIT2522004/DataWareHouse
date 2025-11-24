package scripts.load_scripts;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.w3c.dom.Element;
import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;

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
    
    static {
        System.out.println("=== LoadToStaging CLASS LOADED ===");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("Working Directory: " + System.getProperty("user.dir"));
    }

    /**
     * ============================================================
     * Step 1: Load configuration from XML file
     * ============================================================
     */
    public static LoadConfig loadConfig(String path) {
        System.out.println("[Step 1] Loading configuration...");
        try {
            LoadConfig config = new LoadConfig(path);
            System.out.println("[Step 1] Config loaded from: " + path);
            return config;
        } catch (Exception e) {
            handleError("ERROR: Load Configuration Failed", "Unable to load config from: " + path, e);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 2: Connect to databases (Control & Staging)
     * ============================================================
     */
    public static void connectDBs(LoadConfig config) {
        System.out.println("[Step 2] Connecting to databases...");
        try {
            // 1. Control DB
            Element control = LoadConfig.getElement(config.getXmlDoc(), "control");
            controlDB = new DBConn(
                    LoadConfig.getValue(control, "url"),
                    LoadConfig.getValue(control, "username"),
                    LoadConfig.getValue(control, "password")
            );
            System.out.println("[Step 2] Connected to Control DB");

            // 2. Staging DB
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element staging = LoadConfig.getChildElement(database, "staging");
            stagingDB = new DBConn(
                    LoadConfig.getValue(staging, "url"),
                    LoadConfig.getValue(staging, "username"),
                    LoadConfig.getValue(staging, "password")
            );
            System.out.println("[Step 2] Connected to Staging DB");

        } catch (Exception e) {
            handleError("ERROR: Database Connection Failed", "Check DB configuration/network.", e);
        }
    }

    /**
     * ============================================================
     * Step 3: Check today's load status
     * ============================================================
     */
    public static void checkTodayLoadSuccess() {
        try {
            System.out.println("[Step 3] Checking today's load status...");
            String sql = "SELECT check_today_loadstaging_success_prefix('LOD_STG') AS success";
            final boolean[] alreadyLoaded = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) alreadyLoaded[0] = rs.getBoolean("success");
            });

            if (alreadyLoaded[0]) {
                String subject = "Weather ETL - Load Already Completed Today";
                String body = "System detected that Load Staging has already completed successfully today.";
                // EmailSender.sendEmail(subject, body); // Optional
                System.out.println("[Step 3] Load already completed today, stopping process.");
                System.exit(0);
            } else {
                System.out.println("[Step 3] No load success found today, proceeding...");
            }
        } catch (Exception e) {
            handleError("ERROR: Check Today Load Failed", "Error checking log status.", e);
        }
    }

    /**
     * ============================================================
     * Step 4: Verify Input File
     * ============================================================
     */
    public static File verifyInputFile() {
        System.out.println("[Step 4] Verifying input CSV file...");
        String dateStr = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // Cấu hình cứng đường dẫn hoặc lấy từ Config XML tuỳ bạn
        String csvPath = "D:/DataWareHouse/src/main/java/data/weatherapi_" + dateStr + ".csv";
        File file = new File(csvPath);

        if (!file.exists()) {
            handleError("ERROR: Input File Missing", "Cannot find file: " + csvPath + "\nCheck if Extract process ran successfully.", new Exception("File Not Found"));
        }
        System.out.println("[Step 4] File found: " + file.getName());
        return file;
    }

    /**
     * ============================================================
     * Step 5: Prepare Load (Create Log)
     * ============================================================
     */
    public static String prepareLoadProcess(String csvPath) {
        System.out.println("[Step 5] Creating process log...");
        try {
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

            System.out.println("[Step 5] Created execution ID: " + execId[0]);
            return execId[0];

        } catch (Exception e) {
            handleError("ERROR: Prepare Load Failed", "Failed to create log entry.", e);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 6: Truncate Raw Tables
     * ============================================================
     */
    public static void truncateRawTables() {
        System.out.println("[Step 6] Truncating RAW tables...");
        try {
            String sql = "TRUNCATE TABLE raw_weather_location, raw_weather_condition, " +
                    "raw_air_quality, raw_weather_observation RESTART IDENTITY CASCADE";
            stagingDB.executeUpdate(sql);
            System.out.println("[Step 6] Raw tables truncated.");
        } catch (Exception e) {
            System.err.println("⚠️ Warning: Failed to truncate tables: " + e.getMessage());
        }
    }

    /**
     * ============================================================
     * Step 7: Load Data Logic
     * ============================================================
     */
    public static int loadDataToStaging(File file, String loadExecId) {
        System.out.println("[Step 7] Loading data into Staging...");

        Connection conn = null;
        PreparedStatement psLoc = null, psCond = null, psAir = null, psObs = null;

        // SQL Definitions
        String sqlLoc = "INSERT INTO raw_weather_location (name, region, country, lat, lon, tz_id, \"localtime\", localtime_epoch, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        String sqlCond = "INSERT INTO raw_weather_condition (code, text, icon, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?::jsonb)";
        String sqlAir = "INSERT INTO raw_air_quality (co, no2, o3, so2, pm2_5, pm10, us_epa_index, gb_defra_index, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";
        String sqlObs = "INSERT INTO raw_weather_observation (last_updated, is_day, temp_c, temp_f, feelslike_c, feelslike_f, humidity, cloud, vis_km, vis_miles, uv, gust_mph, gust_kph, wind_mph, wind_kph, wind_degree, wind_dir, pressure_mb, pressure_in, precip_mm, precip_in, location_name, source_system, batch_id, raw_payload) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)";

        int count = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(file, StandardCharsets.UTF_8))) {
            conn = stagingDB.getConnection();
            conn.setAutoCommit(false);

            psLoc = conn.prepareStatement(sqlLoc);
            psCond = conn.prepareStatement(sqlCond);
            psAir = conn.prepareStatement(sqlAir);
            psObs = conn.prepareStatement(sqlObs);

            String line;
            Map<String, Integer> map = new HashMap<>();

            // Header processing
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
                        if (val != null) json.addProperty(h, val);
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
                    try {
                        String val = safeGet(row, map, "localtime_epoch");
                        psLoc.setLong(8, val != null ? Long.parseLong(val) : 0);
                    } catch (Exception e) { psLoc.setObject(8, null); }
                    psLoc.setString(9, source);
                    psLoc.setString(10, extractBatchId);
                    psLoc.setString(11, jsonStr);
                    psLoc.addBatch();

                    // --- 2. CONDITION ---
                    psCond.setString(1, safeGet(row, map, "condition_code"));
                    psCond.setString(2, safeGet(row, map, "condition_text"));
                    String iconVal = safeGet(row, map, "condition_icon");
                    if (iconVal == null) iconVal = safeGet(row, map, "icon");
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
                    psObs.setString(idx++, safeGet(row, map, "last_updated"));
                    try {
                        String val = safeGet(row, map, "is_day");
                        psObs.setInt(idx++, val != null ? Integer.parseInt(val) : 0);
                    } catch (Exception e) { psObs.setObject(idx - 1, null); }
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
            System.out.println("[Step 7] Inserted " + count + " rows successfully.");
            return count;

        } catch (Exception e) {
            try { if (conn != null) conn.rollback(); } catch (SQLException ex) {}
            // Log failed status directly here? Or throw up?
            // Throwing up to main to handle exit
            throw new RuntimeException(e);
        } finally {
            try { if (psLoc != null) psLoc.close(); } catch (SQLException ex) {}
            try { if (psCond != null) psCond.close(); } catch (SQLException ex) {}
            try { if (psAir != null) psAir.close(); } catch (SQLException ex) {}
            try { if (psObs != null) psObs.close(); } catch (SQLException ex) {}
            try { if (conn != null) conn.close(); } catch (SQLException ex) {}
        }
    }

    /**
     * ============================================================
     * Step 8: Update Log & Archive
     * ============================================================
     */
    public static void updateLogAndArchive(String execId, int count, File file) {
        System.out.println("[Step 8] Updating logs and archiving file...");
        try {
            // Update Log
            String sql = "SELECT update_loadstaging_log_status(?, ?::process_status, ?, ?, ?)";
            controlDB.executeQuery(sql, rs -> {}, execId, "success", count, 0, "Loaded successfully");
            System.out.println("  -> Log updated to SUCCESS");

            // Archive File
            String archiveDir = "data/archive/";
            File dir = new File(archiveDir);
            if (!dir.exists()) dir.mkdirs();

            Path source = file.toPath();
            Path dest = Paths.get(archiveDir + file.getName());

            Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING);
            System.out.println("  -> File archived to: " + dest.toString());

        } catch (Exception e) {
            System.err.println("⚠️ Warning: Post-processing failed (Log/Archive): " + e.getMessage());
        }
    }

    // ============================================================
    // UTILS
    // ============================================================
    private static String safeGet(String[] row, Map<String, Integer> map, String colName) {
        Integer index = map.get(colName);
        if (index == null || index >= row.length) return null;
        return row[index];
    }

    private static void handleError(String subject, String body, Exception e) {
        EmailSender.sendError(subject, body, e);
        System.err.println("\n[FAILED] " + subject);
        if (e != null) e.printStackTrace();
        System.exit(1);
    }

    private static void printUsage() {
        System.out.println("Usage: java LoadToStaging <config_path>");
    }

    /**
     * ============================================================
     * MAIN METHOD
     * ============================================================
     */
    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Weather ETL - Step 6: Load Process Started");
        System.out.println("========================================\n");

        String configPath = (args.length > 0) ? args[0] : "config/config.xml";
        System.out.println("Config Path: " + configPath);

        try {
            // Step 1: Load config
            LoadConfig config = loadConfig(configPath);

            // Step 2: Connect DBs
            connectDBs(config);

            // Step 3: Check status
            checkTodayLoadSuccess();

            // Step 4: Verify File
            File csvFile = verifyInputFile();

            // Step 5: Prepare Process Log
            String loadExecId = prepareLoadProcess(csvFile.getAbsolutePath());

            // Step 6: Truncate Raw
            truncateRawTables();

            // Step 7: Execute Load
            int loadedCount = 0;
            try {
                loadedCount = loadDataToStaging(csvFile, loadExecId);
            } catch (Exception e) {
                // If load fails, update log to FAILED
                try {
                    String sql = "SELECT update_loadstaging_log_status(?, ?::process_status, ?, ?, ?)";
                    controlDB.executeQuery(sql, rs -> {}, loadExecId, "failed", 0, 0, e.getMessage());
                } catch (Exception ex) {}
                throw e; // Rethrow to main catch
            }

            // Step 8: Update Log & Archive
            updateLogAndArchive(loadExecId, loadedCount, csvFile);

            // SUCCESS
            System.out.println("\n========================================");
            System.out.println("Weather ETL - Load Process COMPLETED");
            System.out.println("========================================");
            System.exit(0);

        } catch (Exception e) {
            handleError("FATAL: Load Process Failed", e.getMessage(), e);
        }
    }
}
