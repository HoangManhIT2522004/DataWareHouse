-- ============================================================
-- DROP ALL FUNCTIONS (clear trước để có thể chạy từ đầu)
-- ============================================================
DROP FUNCTION IF EXISTS get_or_create_config(
    config_name_input VARCHAR,
    source_type_input VARCHAR,
    source_url_input TEXT,
    output_path_input VARCHAR,
    is_active_input BOOLEAN
    );

DROP FUNCTION IF EXISTS create_new_log(config_src_id_input INTEGER);

DROP FUNCTION IF EXISTS update_log_status(
    execution_id_input TEXT,
    new_status src_status,
    records_count INT,
    error_msg TEXT
    );

DROP FUNCTION IF EXISTS check_today_extract_success();

-- ============================================================
-- FUNCTION: LẤY HOẶC TẠO CONFIG - Trả về config_src_id
-- ============================================================
CREATE OR REPLACE FUNCTION get_or_create_config(
    config_name_input VARCHAR,
    source_type_input VARCHAR,
    source_url_input TEXT,
    output_path_input VARCHAR,
    is_active_input BOOLEAN DEFAULT TRUE
)
RETURNS TABLE(
    config_src_id INTEGER,
    source_url TEXT,
    output_path VARCHAR
) AS $$
DECLARE
existing_config_id INTEGER;
BEGIN
    -- Bước 1: Thử lấy config có sẵn
SELECT cs.config_src_id
INTO existing_config_id
FROM config_src cs
WHERE cs.config_name = config_name_input
    LIMIT 1;

-- Bước 2: Nếu chưa có → Insert mới
IF existing_config_id IS NULL THEN
        INSERT INTO config_src (
            config_name,
            source_type,
            source_url,
            output_path,
            is_active
        )
        VALUES (
            config_name_input,
            source_type_input,
            source_url_input,
            output_path_input,
            is_active_input
        )
        RETURNING config_src.config_src_id INTO existing_config_id;
END IF;

    -- Bước 3: Trả về đầy đủ thông tin từ DB
RETURN QUERY
SELECT cs.config_src_id, cs.source_url, cs.output_path
FROM config_src cs
WHERE cs.config_src_id = existing_config_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FUNCTION: TẠO LOG MỚI - Tự động tăng từ log cuối cùng
-- ============================================================
CREATE OR REPLACE FUNCTION create_new_log(config_src_id_input INTEGER)
RETURNS VARCHAR AS $$
DECLARE
execution_date_part TEXT;
    max_counter_today INT;
    new_counter INT;
    new_execution_id VARCHAR;
BEGIN
    -- Tạo prefix ngày: YYYYMMDD
    execution_date_part := to_char(CURRENT_DATE, 'YYYYMMDD');

    -- Lấy số thứ tự lớn nhất trong ngày
SELECT COALESCE(MAX(split_part(execution_id, '-', 3)::INT), 0)
INTO max_counter_today
FROM log_src
WHERE config_src_id = config_src_id_input
  AND DATE(start_time) = CURRENT_DATE;

-- Tăng số thứ tự
new_counter := max_counter_today + 1;

    -- Tạo execution_id mới: EXEC-YYYYMMDD-XXX
    new_execution_id := 'EXEC-' || execution_date_part || '-' || LPAD(new_counter::TEXT, 3, '0');

    -- Insert log mới với status = 'running'
INSERT INTO log_src(
    config_src_id,
    execution_id,
    status,
    start_time
)
VALUES(
          config_src_id_input,
          new_execution_id,
          'running',
          NOW()
      );

-- Trả về execution_id để Java sử dụng
RETURN new_execution_id;
END;
$$ LANGUAGE plpgsql;

-- -- Test (nếu cần, bỏ comment để chạy)
-- -- SELECT create_new_log(3);

-- ============================================================
-- FUNCTION: CẬP NHẬT STATUS SAU KHI CHẠY XONG
-- ============================================================
CREATE OR REPLACE FUNCTION update_log_status(
    execution_id_input TEXT,
    new_status src_status,
    records_count INT,
    error_msg TEXT
)
RETURNS VOID AS $$
BEGIN
UPDATE log_src
SET
    status = new_status,
    end_time = NOW(),
    records_extracted = records_count,
    error_message = error_msg
WHERE execution_id = execution_id_input;

IF NOT FOUND THEN
        RAISE EXCEPTION 'Không tìm thấy execution_id: %', execution_id_input;
END IF;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- FUNCTION 1: KIỂM TRA HÔM NAY ĐÃ CÓ LOG SUCCESS CHƯA
-- ============================================================
CREATE OR REPLACE FUNCTION check_today_extract_success()
RETURNS BOOLEAN AS $$
DECLARE
v_count INT;
BEGIN
SELECT COUNT(*)
INTO v_count
FROM log_src
WHERE status = 'success'
  AND DATE(start_time) = CURRENT_DATE;

RETURN v_count > 0;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- KẾT THÚC
-- ============================================================
