package scripts.extract_scripts;

import org.w3c.dom.Element;
import utils.DBConn;
import utils.EmailSender;
import utils.LoadConfig;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ExtractToFile {

    private static DBConn controlDB;

    // ✅ STATIC BLOCK FOR DEBUG
    static {
        System.out.println("=== ExtractToFile CLASS LOADED ===");
        System.out.println("Java Version: " + System.getProperty("java.version"));
        System.out.println("Working Directory: " + System.getProperty("user.dir"));
        System.out.println("Classpath: " + System.getProperty("java.class.path"));
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
            String subject = "ERROR: Load Configuration Failed";
            String body = "Unable to load configuration file from path: " + path +
                    "\n\nPlease verify:\n" +
                    "- Does the file exist?\n" +
                    "- Is the path correct?\n" +
                    "- Does the file have read permissions?\n" +
                    "- Is the XML structure valid?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 1] FAILED: Cannot load config file");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 2: Connect to database
     * ============================================================
     */
    public static DBConn connectDB(LoadConfig config) {
        System.out.println("[Step 2] Connecting to database...");
        try {
            // Get database element first, then control inside it
            Element database = LoadConfig.getElement(config.getXmlDoc(), "database");
            Element control = LoadConfig.getChildElement(database, "control");

            // Debug logging
            System.out.println("[DEBUG] Database element: " + (database != null ? "Found" : "NULL"));
            System.out.println("[DEBUG] Control element: " + (control != null ? "Found" : "NULL"));

            String url = LoadConfig.getValue(control, "url");
            String username = LoadConfig.getValue(control, "username");
            String password = LoadConfig.getValue(control, "password");

            // Debug logging
            System.out.println("[DEBUG] URL: " + url);
            System.out.println("[DEBUG] Username: " + username);
            System.out.println("[DEBUG] Password: " + (password.isEmpty() ? "EMPTY!" : "***"));

            DBConn db = new DBConn(url, username, password);

            // Test connection
            db.executeQuery("SELECT 1", rs -> {
                if (rs.next()) {
                    System.out.println("[Step 2] Database connection successful");
                }
            });

            return db;

        } catch (Exception e) {
            String subject = "ERROR: Database Connection Failed";
            String body = "Unable to connect to database.\n\n" +
                    "Please verify:\n" +
                    "- Is the database server running?\n" +
                    "- Are the connection details (url, username, password) correct?\n" +
                    "- Is the network connection available?";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 2] FAILED: Cannot connect to database");
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Step 3: Check today's extract status
     * ============================================================
     */
    public static void checkTodayExtractSuccess() {
        try {
            System.out.println("[Step 3] Checking today's extract status...");
            String sql = "SELECT check_today_extract_success() AS success";
            final boolean[] alreadyExtracted = {false};

            controlDB.executeQuery(sql, rs -> {
                if (rs.next()) {
                    alreadyExtracted[0] = rs.getBoolean("success");
                }
            });

            if (alreadyExtracted[0]) {
                String subject = "Weather ETL - Extract Already Completed Today";
                String body = "The system detected that extraction has already been completed successfully today.\n\n" +
                        "Time: " + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                EmailSender.sendEmail(subject, body);
                System.out.println("[Step 3] Extract already completed today, stopping process.");
                System.exit(0);
            } else {
                System.out.println("[Step 3] No extract success found today, proceeding...");
            }

        } catch (Exception e) {
            String subject = "ERROR: Check Today Extract Failed";
            String body = "Error occurred while checking today's extract status";
            EmailSender.sendError(subject, body, e);
            System.err.println("[Step 3] FAILED: Error checking today's extract");
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * ============================================================
     * Step 4: Prepare for extract, load extract config create/get config and create new log
     * ============================================================
     */
    public static ExtractWeatherData.ExtractInfo prepareExtract(LoadConfig c) {
        try {
            System.out.println("[Step 4] Preparing extract...");

            Element config = LoadConfig.getElement(c.getXmlDoc(), "config");
            Element configSource = LoadConfig.getChildElement(config, "configSource");

            String baseName = LoadConfig.getValue(configSource, "config_name");

            String today = java.time.LocalDate.now().format(
                    java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")
            );

            String configName = baseName + "_" + today;
            String sourceType = LoadConfig.getValue(configSource, "source_type");
            String sourceUrl = LoadConfig.getValue(configSource, "source_url");
            String outputPathBase = LoadConfig.getValue(configSource, "output_path");
            boolean isActive = Boolean.parseBoolean(LoadConfig.getValue(configSource, "is_active"));

            String fullOutputPath = ExtractWeatherData.generateDailyFileName(outputPathBase);

            System.out.println("[Step 4] Config info from XML:");
            System.out.println("  - Config Name   : " + configName);
            System.out.println("  - Source Type   : " + sourceType);
            System.out.println("  - Source URL    : " + sourceUrl);
            System.out.println("  - Output Path   : " + fullOutputPath);
            System.out.println("  - Is Active     : " + isActive);

            // Step 1: Get config with FULL PATH
            String getConfigSql = String.format(
                    "SELECT * FROM get_or_create_config('%s', '%s', '%s', '%s', %b)",
                    configName, sourceType, sourceUrl, fullOutputPath, isActive
            );

            final int[] configSrcId = {0};
            final String[] dbSourceUrl = {null};
            final String[] dbOutputPath = {null};

            controlDB.executeQuery(getConfigSql, rs -> {
                if (rs.next()) {
                    configSrcId[0] = rs.getInt("config_src_id");
                    dbSourceUrl[0] = rs.getString("source_url");
                    dbOutputPath[0] = rs.getString("output_path");
                }
            });

            if (configSrcId[0] == 0) {
                throw new Exception("Cannot get or create config");
            }

            System.out.println("[Step 4] Config from DB:");
            System.out.println("  - Config ID     : " + configSrcId[0]);
            System.out.println("  - Source URL    : " + dbSourceUrl[0]);
            System.out.println("  - Output Path   : " + dbOutputPath[0]);

            // Step 2: Create new log
            String createLogSql = String.format(
                    "SELECT create_new_log(%d) AS execution_id",
                    configSrcId[0]
            );

            final String[] executionId = {null};
            controlDB.executeQuery(createLogSql, rs -> {
                if (rs.next()) {
                    executionId[0] = rs.getString("execution_id");
                }
            });

            if (executionId[0] == null) {
                throw new Exception("Cannot create log");
            }

            String currentExecutionId = executionId[0];
            System.out.println("[Step 4] Created execution: " + executionId[0]);

            return new ExtractWeatherData.ExtractInfo(
                    executionId[0],
                    dbSourceUrl[0],
                    dbOutputPath[0]
            );

        } catch (Exception e) {
            EmailSender.sendError("ERROR: Prepare Extract Failed", e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }

    /**
     * ============================================================
     * Print usage instructions
     * ============================================================
     */
    private static void printUsage() {
        System.out.println("Usage: java ExtractToFile <config_path> <extract_config_path>");
        System.out.println();
        System.out.println("Arguments:");
        System.out.println("  config_path         : Path to main configuration file (e.g., config/config.xml)");
        System.out.println("  extract_config_path : Path to extract configuration file (e.g., config/extract_config.xml)");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java ExtractToFile config/config.xml config/extract_config.xml");
    }

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Weather ETL - Extract Process Started");
        System.out.println("========================================\n");

        // Check arguments
        if (args.length < 2) {
            System.err.println("ERROR: Missing required arguments!");
            System.err.println();
            printUsage();
            System.exit(1);
        }

        String configPath = args[0];
        String extractConfigPath = args[1];

        System.out.println("Configuration:");
        System.out.println("  - Main Config    : " + configPath);
        System.out.println("  - Extract Config : " + extractConfigPath);
        System.out.println();

        try {
            // Step 1: Load config
            LoadConfig config = loadConfig(configPath);

            // Step 2: Connect DB
            controlDB = connectDB(config);

            // Step 3: Check extract success today
            checkTodayExtractSuccess();

            // Step 4: Prepare extract create config & log (Load extract config)
            LoadConfig extractConfig = loadConfig(extractConfigPath);
            ExtractWeatherData.ExtractInfo extractInfo = prepareExtract(extractConfig);

            // Step 5: Extract weather to csv file
            ExtractWeatherData.extractWeatherToFile(
                    extractConfig,
                    extractInfo.getExecutionId(),
                    extractInfo.getSourceUrl(),
                    extractInfo.getOutputPath(),
                    controlDB
            );

            // SUCCESS!
            System.out.println("\n========================================");
            System.out.println("Weather ETL - Extract Process COMPLETED");
            System.out.println("========================================");
            System.exit(0);

        } catch (Exception e) {
            // CRITICAL ERROR
            // (ExtractWeatherData has already updated log and sent email)
            System.err.println("\n========================================");
            System.err.println("Weather ETL - Extract Process FAILED");
            System.err.println("========================================");
            e.printStackTrace();
            System.exit(1);
        }
    }
}