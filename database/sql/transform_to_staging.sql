CREATE OR REPLACE FUNCTION get_or_create_config_transformstaging(
    p_process_name VARCHAR, 
    p_process_type VARCHAR, 
    p_source_path VARCHAR, 
    p_target_db VARCHAR, 
    p_target_table VARCHAR
)
RETURNS INT AS $$
DECLARE
    v_id INT;
BEGIN
    -- 1. Tìm xem config đã tồn tại chưa
    SELECT config_process_id INTO v_id
    FROM config_process
    WHERE process_name = p_process_name;
    
    -- 2. Nếu chưa có thì tạo mới
    IF v_id IS NULL THEN
        INSERT INTO config_process (
            process_name, process_type, source_path, target_database, target_table, is_active
        ) VALUES (
            p_process_name, p_process_type, p_source_path, p_target_db, p_target_table, true
        )
        RETURNING config_process_id INTO v_id;
    END IF;
    
    RETURN v_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION create_new_transformstaging_log(p_config_id INT)
RETURNS VARCHAR AS $$
DECLARE
    v_date_part TEXT;
    v_cnt INT;
    v_exec_id VARCHAR;
BEGIN
    v_date_part := to_char(CURRENT_DATE, 'YYYYMMDD');
    
    -- Đếm số log trong ngày của config này để tăng số thứ tự
    SELECT COALESCE(MAX(split_part(execution_id, '_', 3)::INT), 0)
    INTO v_cnt
    FROM log_process
    WHERE config_process_id = p_config_id
      AND DATE(start_time) = CURRENT_DATE;
    
    -- Tạo ID: EXEC-YYYYMMDD-XXX
    v_exec_id := 'TRF_STG_' || v_date_part || '_' || LPAD((v_cnt + 1)::TEXT, 3, '0');
    
    -- Insert log mới (status = running)
    INSERT INTO log_process (config_process_id, execution_id, status, start_time)
    VALUES (p_config_id, v_exec_id, 'running', NOW());
    
    RETURN v_exec_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION update_transformstaging_log_status(
    p_exec_id VARCHAR, 
    p_status process_status, -- Lưu ý: process_status là ENUM
    p_inserted INT, 
    p_failed INT, 
    p_msg TEXT
)
RETURNS VOID AS $$
BEGIN
    UPDATE log_process
    SET status = p_status,
        end_time = NOW(),
        records_inserted = p_inserted,
        records_failed = p_failed,
        error_message = p_msg
    WHERE execution_id = p_exec_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "public"."check_today_transformstaging_success"()
  RETURNS "pg_catalog"."bool" AS $BODY$
BEGIN
    -- Giả sử config_id của Load luôn có tên bắt đầu bằng 'WA_Load%'
    -- Nếu bảng config_src lưu tên config
    RETURN EXISTS (
        SELECT 1 
        FROM logs l   -- Có thể cần sửa thành: log_src l
        JOIN config_src c ON l.config_src_id = c.config_src_id
        WHERE c.config_name LIKE 'TRF_STG%' 
          AND l.status = 'SUCCESS' -- Có thể cần sửa thành: 'success' (chữ thường) nếu dùng enum
          AND DATE(l.start_at) = CURRENT_DATE -- Có thể cần sửa thành: start_time
    );
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
  CREATE OR REPLACE FUNCTION "public"."check_today_transformstaging_success_prefix"("p_process_prefix" varchar)
  RETURNS "pg_catalog"."bool" AS $BODY$
BEGIN
    RETURN EXISTS (
        SELECT 1 
        FROM log_process l
        JOIN config_process c ON l.config_process_id = c.config_process_id
        WHERE c.process_name LIKE p_process_prefix || '%'
          AND l.status = 'success'
          AND DATE(l.start_time) = CURRENT_DATE
    );
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;