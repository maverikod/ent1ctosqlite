import os
import xml.etree.ElementTree as ET
import logging
import sqlite3
import re
from typing import List, Tuple, Optional
from .utils import (
    find_configuration_root, 
    get_english_folder,
    extract_synonym,
    determine_module_type,
    get_type_ru,
    get_type_en,
    is_in_excluded_types
)
import zipfile

logger = logging.getLogger('ent1ctosqlite')

def extract_vcv(zip_path: str, extract_path: str) -> str:
    """Распаковывает zip архив и находит корневой каталог конфигурации."""
    
    try:
        if not os.path.exists(zip_path):
            logger.error(f"Файл архива не найден: {zip_path}")
            raise FileNotFoundError(f"Файл архива не найден: {zip_path}")
    except Exception as e:
        logger.error(f"Ошибка при проверке файла архива: {str(e)}")
        raise
            
    logger.info(f"Начинаю распаковку архива: {zip_path}")
    logger.info(f"Целевой каталог: {extract_path}")
    
    # Создаем каталог для распаковки
    os.makedirs(extract_path, exist_ok=True)
    
    # Очищаем каталог, если он не пустой
    if os.path.exists(extract_path) and os.listdir(extract_path):
        logger.info("Очистка существующего каталога...")
        for root, dirs, files in os.walk(extract_path, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        logger.info("Каталог очищен")
    
    # Распаковываем архив
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Выводим содержимое архива
        logger.debug("\nСодержимое архива:")
        for file in zip_ref.namelist():
            logger.debug(f"  {file}")
        
        # Распаковываем
        logger.info("\nРаспаковка...")
        zip_ref.extractall(extract_path)
        logger.info("Распаковка завершена")
    
    # Проверяем содержимое распакованного каталога
    logger.info(f"\nПроверка содержимого {extract_path}:")
    if os.path.exists(extract_path):
        files = os.listdir(extract_path)
        logger.debug(f"Файлы в корне: {files}")
        
        # Если в корне только один каталог, проверяем его
        if len(files) == 1 and os.path.isdir(os.path.join(extract_path, files[0])):
            subdir = os.path.join(extract_path, files[0])
            logger.debug(f"Проверка подкаталога: {subdir}")
            subfiles = os.listdir(subdir)
            logger.debug(f"Файлы в подкаталоге: {subfiles}")
    else:
        logger.error("Каталог распаковки не существует!")
    
    # Ищем Configuration.xml
    config_root = find_configuration_root(extract_path)
    if config_root:
        logger.info(f"Успешно найден корневой каталог: {config_root}")
        return config_root
    else:
        logger.error("Не удалось найти каталог с Configuration.xml")
        raise FileNotFoundError("Configuration.xml не найден в распакованном архиве")

def parse_configuration(config_path: str, conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    """Разбирает файл Configuration.xml и возвращает список объектов конфигурации."""
    
    try:
        logger.info(f"Начинаю парсинг файла: {config_path}")
        root_dir = os.path.dirname(config_path)
        
        tree = ET.parse(config_path)
        root = tree.getroot()
    
        ns = {'ns': 'http://v8.1c.ru/8.3/MDClasses'}
        child_objects = root.find('.//ns:Configuration/ns:ChildObjects', namespaces=ns)
        if child_objects is None:
            logger.error("Не найден элемент ChildObjects")
            return []
            
        objects_found = []  # Список для возврата
        total_objects = 0
        
        for obj in child_objects:
            object_type = obj.tag.split('}')[-1]  # Убираем пространство имен из тега
            name = obj.text.strip() if obj.text else ''
            
            if name and not is_in_excluded_types(object_type):
                total_objects += 1
                
                # Сохраняем информацию об объекте
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO objects (obj_type, obj_name)
                    VALUES (?, ?)
                """, (object_type, name))
                
                objects_found.append((object_type, name))
                logger.debug(f"Добавлен объект: {object_type}/{name}")
        
        logger.info(f"Всего найдено объектов: {total_objects}")
        conn.commit()
        return objects_found
        
    except ET.ParseError as e:
        logger.error(f"Ошибка парсинга XML: {e}")
        raise
    except Exception as e:
        logger.error(f"Неожиданная ошибка при разборе конфигурации: {e}")
        logger.exception("Полный стек ошибки:")
        raise

def parse_predefined(obj_id: int, predefined_path: str, conn: sqlite3.Connection) -> None:
    """Разбирает предопределенные значения объекта."""
    cursor = conn.cursor()
    
    try:
        tree = ET.parse(predefined_path)
        root = tree.getroot()
        ns = {'v8': 'http://v8.1c.ru/8.1/data/core'}
        
        for item in root.findall(".//Item", namespaces=ns):
            name = item.findtext("Name", namespaces=ns)
            
            # Добавляем запись в predefined_attrs
            cursor.execute("""
                INSERT INTO predefined_attrs (
                    predefined_attrs_owner,
                    predefined_attrs_name
                ) VALUES (?, ?)
            """, (obj_id, name))
            
            predefined_id = cursor.lastrowid
            
            # Обрабатываем значения атрибутов
            for child in item:
                if child.tag != 'Name':  # пропускаем уже обработанное имя
                    cursor.execute("""
                        INSERT INTO predefined_attrs_values (
                            predefined_attrs_values_owner,
                            predefined_attrs_values_atr,
                            predefined_attrs_values_val
                        ) VALUES (?, ?, ?)
                    """, (obj_id, predefined_id, child.text))
        
        conn.commit()
        logger.debug(f"Обработаны предопределенные значения: {predefined_path}")
        
    except Exception as e:
        logger.error(f"Ошибка при разборе предопределенных значений {predefined_path}: {e}")
        raise

def parse_form_and_code(obj_id: int, form_path: str, conn: sqlite3.Connection) -> None:
    """Разбирает форму и её модуль."""
    cursor = conn.cursor()
    
    try:
        # Получаем имя формы из пути
        form_name = os.path.basename(os.path.dirname(form_path))
        
        # Читаем XML формы для получения синонима
        tree = ET.parse(form_path)
        root = tree.getroot()
        
        # Регистрируем пространство имен
        ns = {'v8': 'http://v8.1c.ru/8.1/data/core'}
        synonym = root.findtext(".//Properties/Synonym/v8:item/v8:content", namespaces=ns) or ""
        
        # Проверяем существование записи
        cursor.execute("""
            SELECT commands_templates_id FROM commands_templates 
            WHERE commands_templates_owner = ? AND commands_templates_name = ?
        """, (obj_id, form_name))
        
        if not cursor.fetchone():
            # Добавляем запись в commands_templates
            cursor.execute("""
                INSERT INTO commands_templates (
                    commands_templates_owner, 
                    commands_templates_name,
                    commands_templates_is_form,
                    commands_templates_is_templ,
                    commands_templates_synonym
                ) VALUES (?, ?, ?, ?, ?)
            """, (obj_id, form_name, True, False, synonym))
            
            template_id = cursor.lastrowid
            logger.debug(f"Добавлена форма: {form_name} (ID: {template_id})")
            
            # Проверяем наличие модуля формы
            module_path = os.path.join(os.path.dirname(form_path), "Module.bsl")
            if os.path.exists(module_path):
                with open(module_path, 'r', encoding='utf-8-sig') as f:
                    module_code = f.read()
                
                # Добавляем запись в code_body
                cursor.execute("""
                    INSERT INTO code_body (
                        code_body_owner_id,
                        code_body_name,
                        code_body_module,
                        code_body_module_type
                    ) VALUES (?, ?, ?, ?)
                """, (template_id, form_name, module_code, "МодульФормы"))
                
                code_body_id = cursor.lastrowid
                logger.debug(f"Добавлен модуль формы для: {form_name} (ID: {code_body_id})")
                
                # Разбираем методы модуля
                parse_methods(module_code, code_body_id, conn)
            
            conn.commit()
            logger.debug(f"Обработана форма: {form_name}")
        
    except Exception as e:
        logger.error(f"Ошибка при разборе формы {form_path}: {e}")
        raise

def parse_methods(module_code: str, code_body_id: int, conn: sqlite3.Connection) -> None:
    """Разбирает код модуля на методы."""
    cursor = conn.cursor()
    
    # Обновленные регулярные выражения для поиска процедур и функций с учетом экспорта
    patterns = {
        'function': r'(?:Функция|Function)\s+([a-zA-Zа-яА-Я0-9_]+)\s*\((.*?)\)(?:\s+Экспорт)?',
        'procedure': r'(?:Процедура|Procedure)\s+([a-zA-Zа-яА-Я0-9_]+)\s*\((.*?)\)(?:\s+Экспорт)?'
    }
    
    try:
        for method_type, pattern in patterns.items():
            is_function = method_type == 'function'
            
            for match in re.finditer(pattern, module_code, re.IGNORECASE | re.MULTILINE):
                method_name = match.group(1)
                params_str = match.group(2)
                # Проверяем наличие ключевого слова Экспорт
                is_export = bool(re.search(r'\bЭкспорт\b|\bExport\b', match.group(0), re.IGNORECASE))
                
                # Проверяем существование метода
                cursor.execute("""
                    SELECT methods_id FROM methods 
                    WHERE methods_owner_id = ? AND methods_name = ?
                """, (code_body_id, method_name))
                
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO methods (
                            methods_owner_id,
                            methods_name,
                            methods_if_func,
                            methods_is_export
                        ) VALUES (?, ?, ?, ?)
                    """, (code_body_id, method_name, is_function, is_export))
                    
                    method_id = cursor.lastrowid
                    logger.debug(f"Добавлен метод: {method_name} (ID: {method_id}, Экспорт: {is_export})")
                    
                    # Разбираем параметры метода
                    parse_method_args(method_id, params_str, method_name, conn)
        
        conn.commit()
    
    except Exception as e:
        logger.error(f"Ошибка при разборе методов модуля: {e}")
        raise

def parse_method_args(method_id: int, params_str: str, method_name: str, conn: sqlite3.Connection) -> None:
    """Разбирает параметры метода."""
    cursor = conn.cursor()
    
    try:
        if params_str.strip():
            # Разбиваем строку параметров на отдельные параметры
            params = [p.strip() for p in params_str.split(',')]
            
            for param in params:
                # Убираем знаки экспорта и значения по умолчанию
                param_name = param.split('=')[0].strip()
                param_name = param_name.split(' ')[0].strip()
                
                # Проверяем существование параметра
                cursor.execute("""
                    SELECT methods_args_id FROM methods_args 
                    WHERE methods_args_owner_id = ? AND methods_args_method_name = ? AND methods_args_arg_name = ?
                """, (method_id, method_name, param_name))
                
                if not cursor.fetchone():
                    cursor.execute("""
                        INSERT INTO methods_args (
                            methods_args_owner_id,
                            methods_args_method_name,
                            methods_args_arg_name
                        ) VALUES (?, ?, ?)
                    """, (method_id, method_name, param_name))
                    
                    logger.debug(f"Добавлен параметр {param_name} для метода {method_name}")
    
            conn.commit()
    
    except Exception as e:
        logger.error(f"Ошибка при разборе параметров метода {method_name}: {e}")
        raise

def parse_module(module_path: str, template_id: int, module_type: str, conn: sqlite3.Connection) -> None:
    """Разбирает модуль и сохраняет его код."""
    cursor = conn.cursor()
    
    try:
        with open(module_path, 'r', encoding='utf-8-sig') as f:
            module_code = f.read()
        
        # Получаем owner_id из commands_templates
        cursor.execute("""
            SELECT commands_templates_owner 
            FROM commands_templates 
            WHERE commands_templates_id = ?
        """, (template_id,))
        result = cursor.fetchone()
        owner_id = result[0] if result else None
        
        # Проверяем существование записи в code_body
        cursor.execute("""
            SELECT code_body_id FROM code_body 
            WHERE code_body_owner_id = ? AND code_body_module_type = ?
        """, (template_id, module_type))
        
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO code_body (
                    code_body_owner_id,
                    code_body_name,
                    code_body_module,
                    code_body_module_type,
                    code_body_owner
                ) VALUES (?, ?, ?, ?, ?)
            """, (template_id, os.path.basename(module_path), module_code, module_type, owner_id))
            
            code_body_id = cursor.lastrowid
            logger.debug(f"Добавлен модуль типа {module_type} (ID: {code_body_id})")
            
            # Разбираем методы модуля
            parse_methods(module_code, code_body_id, conn)
        conn.commit()            
    except Exception as e:
        logger.error(f"Ошибка при разборе модуля {module_path}: {e}")
        conn.rollback()
        raise

def analyze_object(cursor: sqlite3.Cursor, obj_type: str, obj_name: str) -> None:
    """Анализирует структуру объекта метаданных и выводит подробную информацию."""
    eng_type = get_type_en(obj_type)
    if not eng_type:
        print(f"Тип объекта {obj_type} не найден")
        return
    
    print(f"\n=== Поиск объекта типа {eng_type} с именем/синонимом '{obj_name}' ===")
    
    # Ищем объект по имени или синониму
    cursor.execute("""
        SELECT o.obj_id, o.obj_name,
               (SELECT pav.predefined_attrs_values_val 
                FROM predefined_attrs_values pav
                JOIN predefined_attrs pa ON pa.predefined_attrs_id = pav.predefined_attrs_values_atr
                JOIN obj_attributes a ON a.obj_attr_id = pa.predefined_attrs_owner
                WHERE a.obj_attr_owner = o.obj_id 
                AND pa.predefined_attrs_name = 'Synonym'
                LIMIT 1) as synonym
        FROM objects o
        WHERE o.obj_type = ? 
        AND (o.obj_name LIKE ? 
             OR EXISTS (
                 SELECT 1 
                 FROM predefined_attrs_values pav
                 JOIN predefined_attrs pa ON pa.predefined_attrs_id = pav.predefined_attrs_values_atr
                 JOIN obj_attributes a ON a.obj_attr_id = pa.predefined_attrs_owner
                 WHERE a.obj_attr_owner = o.obj_id 
                 AND pa.predefined_attrs_name = 'Synonym'
                 AND pav.predefined_attrs_values_val LIKE ?
             ))
    """, (obj_type, f"%{obj_name}%", f"%{obj_name}%"))
    
    results = cursor.fetchall()
    
    if not results:
        print(f"Объект не найден")
        return
        
    if len(results) > 1:
        print("\nНайдено несколько объектов:")
        for row in results:
            synonym = f" ({row[2]})" if row[2] else ""
            print(f"- {row[1]}{synonym}")
        return
        
    obj_id = results[0][0]
    obj_full_name = results[0][1]
    obj_synonym = f" ({results[0][2]})" if results[0][2] else ""
    
    print(f"\n=== Информация об объекте {obj_full_name}{obj_synonym} ===")
    
    # Получаем реквизиты объекта
    cursor.execute("""
        SELECT a.prop_name, a.table_part,
               (SELECT pav.predefined_attrs_values_val 
                FROM predefined_attrs_values pav
                JOIN predefined_attrs pa ON pa.predefined_attrs_id = pav.predefined_attrs_values_atr
                WHERE pa.predefined_attrs_owner = a.obj_attr_id 
                AND pa.predefined_attrs_name = 'Synonym'
                LIMIT 1) as synonym,
               GROUP_CONCAT(
                   CASE 
                       WHEN t.is_configuration_type = 1 THEN t.type_body 
                       ELSE t.type_class_ru 
                   END
               ) as types
        FROM obj_attributes a
        LEFT JOIN obj_attr_types t ON t.obj_attr_type_owner = a.obj_attr_id
        WHERE a.obj_attr_owner = ? AND a.is_attribute = 1
        GROUP BY a.prop_name, a.table_part
        ORDER BY a.table_part NULLS FIRST, a.prop_name
    """, (obj_id,))
    
    attributes = cursor.fetchall()
    if attributes:
        print("\nРеквизиты объекта:")
        current_table = None
        for attr in attributes:
            prop_name = attr[0]
            table_part = attr[1]
            synonym = f" ({attr[2]})" if attr[2] else ""
            types = f" - {attr[3]}" if attr[3] else ""
            
            if table_part != current_table:
                current_table = table_part
                if current_table:
                    cursor.execute("""
                        SELECT pav.predefined_attrs_values_val 
                        FROM predefined_attrs_values pav
                        JOIN predefined_attrs pa ON pa.predefined_attrs_id = pav.predefined_attrs_values_atr
                        JOIN obj_attributes a ON a.obj_attr_id = pa.predefined_attrs_owner
                        WHERE a.obj_attr_owner = ? 
                        AND a.prop_name = ?
                        AND pa.predefined_attrs_name = 'Synonym'
                        LIMIT 1
                    """, (obj_id, current_table))
                    table_synonym = cursor.fetchone()
                    table_synonym_str = f" ({table_synonym[0]})" if table_synonym else ""
                    print(f"\nТабличная часть '{current_table}'{table_synonym_str}:")
            
            print(f"  - {prop_name}{synonym}{types}")
            
    # Для документов добавляем специфичную информацию
    if obj_type == 'Document':
        # Получаем основания документа
        cursor.execute("""
            SELECT based_on_name FROM based_on 
            WHERE based_on_owner = ?
            ORDER BY based_on_name
        """, (obj_id,))
        based_on = cursor.fetchall()
        if based_on:
            print("\nОснования документа:")
            for base in based_on:
                print(f"  - {base[0]}")
                
        # Получаем регистры, в которые пишет документ
        cursor.execute("""
            SELECT register_records_name FROM register_records 
            WHERE register_records_owner = ?
            ORDER BY register_records_name
        """, (obj_id,))
        registers = cursor.fetchall()
        if registers:
            print("\nРегистры, в которые пишет документ:")
            for reg in registers:
                print(f"  - {reg[0]}")
                
    # Получаем методы объекта
    cursor.execute("""
        SELECT m.methods_name, m.methods_if_func, m.methods_is_export
        FROM methods m
        JOIN code_body cb ON m.methods_owner_id = cb.code_body_id
        WHERE cb.code_body_owner_id = ?
        ORDER BY m.methods_name
    """, (obj_id,))
    
    methods = cursor.fetchall()
    if methods:
        print("\nМетоды объекта:")
        for method in methods:
            method_type = "Функция" if method[1] else "Процедура"
            export_mark = " Экспорт" if method[2] else ""
            print(f"  - {method_type} {method[0]}(){export_mark}")

def analyze_directory(base_path: str, conn: sqlite3.Connection) -> None:
    """Анализирует структуру каталогов конфигурации."""
    cursor = conn.cursor()
    
    # Находим корневой каталог конфигурации
    root_path = find_configuration_root(base_path)
    if not root_path:
        raise ValueError("Не найден корневой каталог конфигурации (Configuration.xml)")
    
    # Получаем словарь соответствия путей и obj_id
    cursor.execute("SELECT obj_id, obj_type, obj_name FROM objects")
    objects_map = {}
    for row in cursor.fetchall():
        obj_id, obj_type, obj_name = row
        eng_folder = get_english_folder(obj_type)
        if eng_folder:
            objects_map[f"{eng_folder}/{obj_name}"] = obj_id
    
    for root, dirs, files in os.walk(base_path):
        try:
            rel_path = os.path.relpath(root, base_path)
            if rel_path == '.':
                continue
            
            # Находим владельца (объект) для текущей директории
            owner_id = None
            for obj_path, obj_id in objects_map.items():
                if obj_path in rel_path:
                    owner_id = obj_id
                    break
                    
            if owner_id is None:
                continue
            
            for file in files:
                try:
                    file_path = os.path.join(root, file)
                    if file.endswith(".xml"):
                        synonym = extract_synonym(file_path)
                        cursor.execute('''
                            INSERT OR IGNORE INTO commands_templates (
                            commands_templates_owner,
                            commands_templates_name,
                            commands_templates_is_form,
                            commands_templates_is_templ,
                            commands_templates_synonym
                            ) VALUES (?, ?, ?, ?, ?)''',
                            (owner_id, os.path.splitext(file)[0],
                            'Форма' in file, 'Макет' in file, synonym))
                        conn.commit()
                
                        cursor.execute('''
                            SELECT commands_templates_id 
                            FROM commands_templates 
                            WHERE commands_templates_owner = ? 
                            AND commands_templates_name = ?''',
                            (owner_id, os.path.splitext(file)[0]))
                        result = cursor.fetchone()
                        if result is None:
                            continue
                        template_id = result[0]
                        
                    elif file.endswith(".bsl"):
                        module_code = ""
                        module_type = determine_module_type(file_path)
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                module_code = f.read()
                        except UnicodeDecodeError:
                            with open(file_path, 'r', encoding='windows-1251') as f:
                                module_code = f.read()
                        try:
                            cursor.execute('''
                                INSERT OR IGNORE INTO code_body (
                                code_body_owner_id,
                                code_body_module,
                                code_body_module_type                                        ) VALUES (?, ?, ?)''',
                            (template_id, module_code, module_type))
                            conn.commit()
                        except sqlite3.OperationalError as e:
                            logger.error(f"Ошибка при добавлении записи в таблицу code_body: {e}")
                            conn.rollback()
                            raise
                
                        try:
                            cursor.execute('''
                                SELECT code_body_id 
                                FROM code_body 
                                WHERE code_body_owner_id = ? 
                                AND code_body_module_type = ?''',
                                (template_id, module_type))
                            result = cursor.fetchone()
                            if result is None:
                                continue
                            code_body_id = result[0]        
                            parse_methods(module_code, code_body_id, conn)
                            conn.commit()
                        except sqlite3.OperationalError as e:
                            logger.error(f"Ошибка при добавлении записи в таблицу code_body: {e}")
                            conn.rollback()
                            raise
                except Exception as e:
                    logger.error(f"Ошибка при обработке файла {file}: {e}")
                    raise
        except Exception as e:
            logger.error(f"Ошибка при обработке каталога {root}: {e}")
            raise


