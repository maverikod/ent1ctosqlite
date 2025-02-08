import sqlite3
import logging
from typing import Optional

logger = logging.getLogger('vcv_parser')

def create_database(conn: sqlite3.Connection) -> sqlite3.Connection:
    """Создаёт базу данных SQLite и основные таблицы."""
    cursor = conn.cursor()
    
    # Таблица объектов конфигурации
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS objects (
            obj_id INTEGER PRIMARY KEY AUTOINCREMENT,
            obj_type TEXT,                  -- Тип объекта метаданных
            obj_name TEXT,                  -- Имя объекта метаданных
            UNIQUE(obj_id, obj_type, obj_name)
        )
    ''')
    
    # Таблица атрибутов объектов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS obj_attributes (
            obj_attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
            obj_attr_owner INTEGER,         -- Ссылка на родительский объект
            prop_name TEXT,                 -- Имя свойства
            table_part TEXT,                -- Имя табличной части (если свойство в табличной части)
            is_dimension BOOLEAN,           -- Признак измерения регистра
            is_resourse BOOLEAN,            -- Признак ресурса регистра
            is_attribute BOOLEAN,           -- Признак реквизита
            is_standard_attribute BOOLEAN,   -- Признак стандартного реквизита
            is_tbl_part BOOLEAN,            -- Признак табличной части
            FOREIGN KEY(obj_attr_owner) REFERENCES objects(obj_id),
            UNIQUE(obj_attr_id, obj_attr_owner, table_part, prop_name)
        )
    ''')
    
    # Таблица типов атрибутов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS obj_attr_types (
            obj_attr_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
            obj_attr_type_owner INTEGER,
            type_body TEXT,                 -- Исходное описание типа, как оно было в конфигурации
            type_name TEXT,                 -- Имя объекта (например, "ДоговорНаВыполнениеРаботВСВ")
            type_class TEXT,                -- Класс типа на английском (например, "DocumentRef")
            type_class_ru TEXT,             -- Класс типа на русском (например, "ДокументСсылка")
            is_configuration_type BOOLEAN,   -- Признак типа из конфигурации (cfg:)
            FOREIGN KEY(obj_attr_type_owner) REFERENCES obj_attributes(obj_attr_id),
            UNIQUE(obj_attr_type_id, obj_attr_type_owner, type_body)
        )
    ''')

    # Таблица форм и макетов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS commands_templates (
            commands_templates_id INTEGER PRIMARY KEY AUTOINCREMENT,
            commands_templates_owner INTEGER,    -- Ссылка на родительский объект
            commands_templates_name TEXT,        -- Имя формы или макета
            commands_templates_is_form BOOLEAN,  -- Признак формы
            commands_templates_is_templ BOOLEAN, -- Признак макета
            commands_templates_synonym TEXT,     -- Синоним формы или макета
            FOREIGN KEY(commands_templates_owner) REFERENCES objects(obj_id),
            UNIQUE(commands_templates_id, commands_templates_owner, commands_templates_name)
        )
    ''')
    
    # Таблица модулей объектов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS code_body (
            code_body_id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_body_owner_id INTEGER,         -- Ссылка на родительскую форму/макет
            code_body_name TEXT,                -- Имя модуля
            code_body_module TEXT,              -- Текст модуля
            code_body_module_type TEXT,         -- Тип модуля
            code_body_owner INTEGER,            -- Ссылка на объект (для обратной совместимости)
            FOREIGN KEY(code_body_owner_id) REFERENCES commands_templates(commands_templates_id),
            FOREIGN KEY(code_body_owner) REFERENCES objects(obj_id),
            UNIQUE(code_body_id, code_body_owner_id, code_body_name)
        )
    ''')
    
    # Таблица методов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS methods (
            methods_id INTEGER PRIMARY KEY AUTOINCREMENT,
            methods_owner_id INTEGER,           -- Ссылка на родительский модуль
            methods_name TEXT,                  -- Имя метода
            methods_if_func BOOLEAN,            -- Признак функции (иначе процедура)
            methods_is_export BOOLEAN DEFAULT FALSE,  -- Признак экспортируемости
            FOREIGN KEY(methods_owner_id) REFERENCES code_body(code_body_id),
            UNIQUE(methods_id, methods_owner_id, methods_name)
        )
    ''')
    
    # Таблица параметров методов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS methods_args (
            methods_args_id INTEGER PRIMARY KEY AUTOINCREMENT,
            methods_args_owner_id INTEGER,      -- Ссылка на родительский метод
            methods_args_method_name TEXT,      -- Имя метода
            methods_args_arg_name TEXT,         -- Имя параметра
            FOREIGN KEY(methods_args_owner_id) REFERENCES methods(methods_id)
        )
    ''')
    
    # Таблица предопределенных реквизитов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predefined_attrs (
            predefined_attrs_id INTEGER PRIMARY KEY AUTOINCREMENT,
            predefined_attrs_owner INTEGER,     -- Ссылка на родительский атрибут
            predefined_attrs_name TEXT,         -- Имя предопределенного значения
            FOREIGN KEY(predefined_attrs_owner) REFERENCES obj_attributes(obj_attr_id),
            UNIQUE(predefined_attrs_id, predefined_attrs_owner, predefined_attrs_name)
        )
    ''')
    
    # Таблица значений предопределенных реквизитов
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS predefined_attrs_values (
            predefined_attrs_values_id INTEGER PRIMARY KEY AUTOINCREMENT,
            predefined_attrs_values_owner INTEGER,   -- Ссылка на родительский атрибут
            predefined_attrs_values_atr INTEGER,     -- Ссылка на предопределенное значение
            predefined_attrs_values_val TEXT,        -- Значение предопределенного реквизита
            FOREIGN KEY(predefined_attrs_values_owner) REFERENCES obj_attributes(obj_attr_id),
            FOREIGN KEY(predefined_attrs_values_atr) REFERENCES predefined_attrs(predefined_attrs_id)
        )
    ''')
    
    # Таблица движений регистров
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS register_records (
            register_records_id INTEGER PRIMARY KEY AUTOINCREMENT,
            register_records_owner INTEGER,     -- Ссылка на родительский объект
            register_records_name TEXT,         -- Имя регистра
            FOREIGN KEY(register_records_owner) REFERENCES objects(obj_id),
            UNIQUE(register_records_id, register_records_owner, register_records_name)
        )
    ''')
    
    # Таблица оснований
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS based_on (
            based_on_id INTEGER PRIMARY KEY AUTOINCREMENT,
            based_on_owner INTEGER,             -- Ссылка на родительский объект
            based_on_name TEXT,                 -- Имя основания
            FOREIGN KEY(based_on_owner) REFERENCES objects(obj_id),
            UNIQUE(based_on_id, based_on_owner, based_on_name)
        )
    ''')
    conn.commit()
    return conn

def check_database_integrity(db_path: str) -> bool:
    """Проверяет логическую целостность базы данных."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    integrity_issues = []

    try:
        # Включаем поддержку внешних ключей
        cursor.execute("PRAGMA foreign_keys = ON")
        cursor.execute("PRAGMA foreign_keys")
        if cursor.fetchone()[0] != 1:
            raise Exception("Не удалось включить поддержку внешних ключей в SQLite")

        # 1. Проверка obj_attributes -> objects
        cursor.execute("""
            SELECT oa.obj_attr_id, oa.obj_attr_owner, oa.prop_name
            FROM obj_attributes oa
            LEFT JOIN objects o ON oa.obj_attr_owner = o.obj_id
            WHERE o.obj_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Атрибут {row[2]} (ID: {row[0]}) ссылается на несуществующий объект (ID: {row[1]})"
                )

        # 2. Проверка obj_attr_types -> obj_attributes
        cursor.execute("""
            SELECT oat.obj_attr_type_id, oat.obj_attr_type_owner, oa.obj_attr_id, oat.type_body
            FROM obj_attr_types oat
            LEFT JOIN obj_attributes oa ON oat.obj_attr_type_owner = oa.obj_attr_id
            WHERE oa.obj_attr_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Тип атрибута {row[3]} (ID: {row[0]}) ссылается на несуществующий атрибут (ID: {row[1]})"
                )

        # 3. Проверка commands_templates -> objects
        cursor.execute("""
            SELECT ct.commands_templates_id, ct.commands_templates_owner, o.obj_id, ct.commands_templates_name
            FROM commands_templates ct
            LEFT JOIN objects o ON ct.commands_templates_owner = o.obj_id
            WHERE o.obj_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Форма {row[3]} (ID: {row[0]}) ссылается на несуществующий объект (ID: {row[1]})"
                )

        # 4. Проверка code_body -> commands_templates
        cursor.execute("""
            SELECT cb.code_body_id, cb.code_body_owner_id, ct.commands_templates_id, cb.code_body_module_type
            FROM code_body cb
            LEFT JOIN commands_templates ct ON cb.code_body_owner_id = ct.commands_templates_id
            WHERE ct.commands_templates_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Модуль типа {row[3]} (ID: {row[0]}) ссылается на несуществующую форму/макет (ID: {row[1]})"
                )

        # 5. Проверка methods -> code_body
        cursor.execute("""
            SELECT m.methods_id, m.methods_owner_id, cb.code_body_id, m.methods_name
            FROM methods m
            LEFT JOIN code_body cb ON m.methods_owner_id = cb.code_body_id
            WHERE cb.code_body_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Метод {row[3]} (ID: {row[0]}) ссылается на несуществующий модуль (ID: {row[1]})"
                )

        # 6. Проверка methods_args -> methods
        cursor.execute("""
            SELECT ma.methods_args_id, ma.methods_args_owner_id, m.methods_id, ma.methods_args_method_name
            FROM methods_args ma
            LEFT JOIN methods m ON ma.methods_args_owner_id = m.methods_id
            WHERE m.methods_id IS NULL
        """)
        orphaned = cursor.fetchall()
        if orphaned:
            for row in orphaned:
                integrity_issues.append(
                    f"Аргумент метода {row[3]} (ID: {row[0]}) ссылается на несуществующий метод (ID: {row[1]})"
                )

        # Вывод результатов проверки
        if integrity_issues:
            logger.error("\nНайдены проблемы целостности базы данных:")
            for issue in integrity_issues:
                logger.error(f"- {issue}")
            logger.error(f"\nВсего найдено проблем: {len(integrity_issues)}")
        else:
            logger.info("\nПроблем целостности базы данных не обнаружено")

        # Дополнительная статистика
        logger.info("\nСтатистика по таблицам:")
        tables = [
            "objects", "obj_attributes", "obj_attr_types", "commands_templates",
            "code_body", "methods", "methods_args", "predefined_attrs",
            "predefined_attrs_values", "register_records", "based_on"
        ]
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"{table}: {count} записей")

    except Exception as e:
        logger.error(f"Ошибка при проверке целостности базы данных: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

    return len(integrity_issues) == 0

def check_and_update_database_structure(conn: sqlite3.Connection) -> None:
    """Проверяет и обновляет структуру базы данных в соответствии с текущим описанием."""
    logger = logging.getLogger('vcv_parser')
    cursor = conn.cursor()
    
    try:
        # Получаем текущую структуру таблиц
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        existing_tables = {row[0]: {} for row in cursor.fetchall()}
        
        # Получаем структуру каждой существующей таблицы
        for table in existing_tables:
            cursor.execute(f"PRAGMA table_info({table})")
            existing_tables[table] = {
                row[1]: {
                    'type': row[2],
                    'notnull': row[3],
                    'pk': row[5]
                } for row in cursor.fetchall()
            }
        
        # Создаем временную базу в памяти для получения эталонной структуры
        temp_conn = sqlite3.connect(':memory:')
        create_database(temp_conn)  # Используем существующую функцию создания базы
        temp_cursor = temp_conn.cursor()
        
        # Получаем эталонную структуру
        temp_cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        required_tables = {row[0]: {} for row in temp_cursor.fetchall()}
        
        for table in required_tables:
            temp_cursor.execute(f"PRAGMA table_info({table})")
            required_tables[table] = {
                row[1]: {
                    'type': row[2],
                    'notnull': row[3],
                    'pk': row[5]
                } for row in cursor.fetchall()
            }
        
        # Сравниваем и обновляем структуру
        for table, required_columns in required_tables.items():
            if table not in existing_tables:
                logger.info(f"Создание отсутствующей таблицы: {table}")
                # Получаем SQL для создания таблицы
                temp_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
                create_sql = temp_cursor.fetchone()[0]
                cursor.execute(create_sql)
                continue
                
            existing_columns = existing_tables[table]
            
            # Проверяем различия в колонках
            for col_name, col_info in required_columns.items():
                if col_name not in existing_columns:
                    # Добавляем отсутствующую колонку
                    logger.info(f"Добавление колонки {col_name} в таблицу {table}")
                    cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_info['type']}")
                elif existing_columns[col_name] != col_info:
                    # Если структура колонки отличается, создаем новую таблицу
                    logger.info(f"Обновление структуры таблицы {table}")
                    
                    # Получаем SQL для создания новой таблицы
                    temp_cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,))
                    create_sql = temp_cursor.fetchone()[0]
                    
                    # Создаем временную таблицу с новой структурой
                    cursor.execute(f"ALTER TABLE {table} RENAME TO {table}_old")
                    cursor.execute(create_sql)
                    
                    # Копируем данные
                    common_columns = set(existing_columns.keys()) & set(required_columns.keys())
                    columns_str = ', '.join(common_columns)
                    cursor.execute(f"INSERT INTO {table} ({columns_str}) SELECT {columns_str} FROM {table}_old")
                    
                    # Удаляем старую таблицу
                    cursor.execute(f"DROP TABLE {table}_old")
                    break
        
        # Проверяем лишние таблицы
        for table in existing_tables:
            if table not in required_tables:
                logger.warning(f"Обнаружена лишняя таблица: {table}")
        
        conn.commit()
        logger.info("Структура базы данных успешно обновлена")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при обновлении структуры базы данных: {e}")
        raise
    finally:
        temp_conn.close()

def get_table_info(conn: sqlite3.Connection) -> None:
    """Выводит информацию о таблицах базы данных."""
    cursor = conn.cursor()
    
    # Получаем список всех таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    
    for table_tuple in tables:
        table = table_tuple[0]
        
        # Получаем информацию о структуре таблицы
        cursor.execute(f"PRAGMA table_info({table})")
        columns = [column[1] for column in cursor.fetchall()]
        
        print(f"\n=== Таблица {table} ===")
        print("Колонки:", ", ".join(columns))
        
        # Получаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"Количество записей: {count}")
        
        if count > 0:
            # Выводим первые несколько записей
            cursor.execute(f"SELECT * FROM {table} LIMIT 5")
            rows = cursor.fetchall()
            print("\nПримеры записей:")
            for row in rows:
                print(row)

# [Другие функции для работы с БД...] 