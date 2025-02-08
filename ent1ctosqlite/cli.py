import argparse
import os
import logging
import sqlite3
from typing import Optional
from .core import extract_vcv, parse_configuration
from .database import create_database, check_database_integrity
from .utils import setup_logger

def parse_args() -> argparse.Namespace:
    """Разбор аргументов командной строки."""
    parser = argparse.ArgumentParser(
        description='Парсер конфигураций 1С:Предприятие 8.3',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'zip_path',
        help='Путь к zip-архиву с выгрузкой конфигурации'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Путь к каталогу для распаковки (по умолчанию: temp)',
        default='temp'
    )
    
    parser.add_argument(
        '-d', '--database',
        help='Путь к файлу базы данных SQLite (по умолчанию: vcv_parser.db)',
        default='vcv_parser.db'
    )
    
    parser.add_argument(
        '--log-file',
        help='Сохранять лог в файл',
        action='store_true'
    )
    
    parser.add_argument(
        '--debug',
        help='Включить режим отладки',
        action='store_true'
    )
    
    parser.add_argument(
        '--check-db',
        help='Проверить целостность базы данных',
        action='store_true'
    )
    
    return parser.parse_args()

def main() -> Optional[int]:
    """Основная функция программы."""
    args = parse_args()
    
    # Настраиваем логирование
    logger = setup_logger(args.log_file, args.debug)
    
    try:
        # Проверяем существование zip-файла
        if not os.path.exists(args.zip_path):
            logger.error(f"Файл не найден: {args.zip_path}")
            return 1
            
        # Если указан флаг проверки БД
        if args.check_db:
            if not os.path.exists(args.database):
                logger.error(f"База данных не найдена: {args.database}")
                return 1
            check_database_integrity(args.database)
            return 0
            
        # Создаем/подключаемся к базе данных
        conn = sqlite3.connect(args.database)
        create_database(conn)
        
        # Распаковываем архив
        config_path = extract_vcv(args.zip_path, args.output)
        if not config_path:
            logger.error("Не удалось найти Configuration.xml")
            return 1
            
        # Разбираем конфигурацию
        objects = parse_configuration(os.path.join(config_path, "Configuration.xml"), conn)
        
        logger.info(f"\nОбработка завершена. Найдено объектов: {len(objects)}")
        logger.info(f"База данных сохранена в: {os.path.abspath(args.database)}")
        
        return 0
        
    except Exception as e:
        logger.exception("Произошла непредвиденная ошибка:")
        return 1
        
    finally:
        try:
            conn.close()
        except:
            pass

if __name__ == '__main__':
    exit(main()) 