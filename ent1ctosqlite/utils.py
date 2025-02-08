import os
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
from typing import Optional, Dict, Union

logger = logging.getLogger('vcv_parser')

def setup_logger(log_to_file: bool = False, debug_mode: bool = False) -> logging.Logger:
    """Настраивает систему логирования."""
    # Получаем абсолютный путь к директории скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Создаем логгер
    logger = logging.getLogger('vcv_parser')
    logger.setLevel(logging.INFO)
    
    # Очищаем существующие обработчики
    logger.handlers = []
    
    # Форматтер для консоли (более простой)
    console_formatter = logging.Formatter('%(message)s')
    
    # Форматтер для файла (подробный)
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Добавляем вывод в консоль
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.DEBUG)
    logger.addHandler(console_handler)
    
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
    
    # Добавляем вывод в файл, если требуется
    if log_to_file:
        # Создаем каталог logs рядом со скриптом
        log_dir = os.path.join(script_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"vcv_parser_{current_time}.log")
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG if debug_mode else logging.DEBUG)
        logger.addHandler(file_handler)
        
        logger.debug(f"Лог-файл создан: {log_file}")
    
    return logger

def is_in_excluded_types(type_name: str) -> bool:
    """Проверяет, входит ли тип в список исключаемых."""
    return type_name in [
        'SessionParameter',
        'StyleItem',
        'Subsystem',
        'XDTOPackage',
        'Language',
        'Interface',
        'SheduledJob',
        'CommandGroup',
        'SettingsStorage',
        'Style',
        'StyleItem',
        'Subsystem',
        'XDTOPackage',
        'ScheduledJob'
    ]

def find_configuration_root(path: str) -> Optional[str]:
    """Находит каталог, содержащий Configuration.xml."""
    logger.debug(f"Поиск Configuration.xml в: {path}")
    
    if not os.path.exists(path):
        logger.error(f"Каталог не существует: {path}")
        return None
        
    try:
        # Выводим полное дерево каталогов
        logger.debug("Структура каталогов:")
        for root, dirs, files in os.walk(path):
            level = root.replace(path, '').count(os.sep)
            indent = ' ' * 4 * level
            logger.debug(f"{indent}[{os.path.basename(root)}]")
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                logger.debug(f"{subindent}{f}")
                if f == "Configuration.xml":
                    logger.debug(f"\nНайден Configuration.xml в: {root}")
                    return root
                    
        logger.warning("Configuration.xml не найден!")
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при поиске Configuration.xml: {e}")
        return None

def extract_synonym(file_path: str) -> Optional[str]:
    """Извлекает синоним из XML файла."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        ns = {'v8': 'http://v8.1c.ru/8.1/data/core'}
        
        # Ищем элемент синонима
        synonym = root.findtext(".//Properties/Synonym/v8:item/v8:content", namespaces=ns)
        return synonym if synonym else None
        
    except ET.ParseError:
        logger.warning(f"Ошибка парсинга XML файла: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Ошибка при извлечении синонима из {file_path}: {e}")
        return None

def determine_module_type(file_path: str) -> str:
    """Определяет тип модуля по его расположению в структуре каталогов."""
    module_types = {
        'Forms': 'МодульФормы',
        'Commands': 'МодульКоманды',
        'CommonModules': 'ОбщийМодуль',
        'CommonForms': 'МодульОбщейФормы',
        'DataProcessors': 'МодульОбработки',
        'Reports': 'МодульОтчета',
        'Documents': 'МодульДокумента',
        'Catalogs': 'МодульСправочника',
        'InformationRegisters': 'МодульРегистраСведений',
        'AccumulationRegisters': 'МодульРегистраНакопления'
    }
    
    for type_path, module_type in module_types.items():
        if type_path in file_path:
            return module_type
    
    return 'НеопределенныйМодуль'

def get_type_mappings() -> Dict[str, Dict[str, str]]:
    """Возвращает словарь соответствий типов на русском и английском языках."""
    return {
        'ru_to_en': {
            'Документ': 'Document',
            'Справочник': 'Catalog',
            'РегистрСведений': 'InformationRegister',
            'РегистрНакопления': 'AccumulationRegister',
            'Перечисление': 'Enum',
            'ПланВидовХарактеристик': 'ChartOfCharacteristicTypes',
            'ПланСчетов': 'ChartOfAccounts',
            'РегистрБухгалтерии': 'AccountingRegister',
            'Обработка': 'DataProcessor',
            'Отчет': 'Report',
            'ОбщийМодуль': 'CommonModule',
            'ОбщаяФорма': 'CommonForm',
            'ХранилищеНастроек': 'SettingsStorage',
            'Константа': 'Constant',
            'ЖурналДокументов': 'DocumentJournal',
            'ПланОбмена': 'ExchangePlan',
            'БизнесПроцесс': 'BusinessProcess',
            'Задача': 'Task'
        },
        'en_to_ru': {
            'Document': 'Документ',
            'Catalog': 'Справочник',
            'InformationRegister': 'РегистрСведений',
            'AccumulationRegister': 'РегистрНакопления',
            'Enum': 'Перечисление',
            'ChartOfCharacteristicTypes': 'ПланВидовХарактеристик',
            'ChartOfAccounts': 'ПланСчетов',
            'AccountingRegister': 'РегистрБухгалтерии',
            'DataProcessor': 'Обработка',
            'Report': 'Отчет',
            'CommonModule': 'ОбщийМодуль',
            'CommonForm': 'ОбщаяФорма',
            'SettingsStorage': 'ХранилищеНастроек',
            'Constant': 'Константа',
            'DocumentJournal': 'ЖурналДокументов',
            'ExchangePlan': 'ПланОбмена',
            'BusinessProcess': 'БизнесПроцесс',
            'Task': 'Задача'
        }
    }

def get_type_ru(en_type: str) -> Optional[str]:
    """Возвращает русское название типа по английскому."""
    mappings = get_type_mappings()
    return mappings['en_to_ru'].get(en_type)

def get_type_en(ru_type: str) -> Optional[str]:
    """Возвращает английское название типа по русскому."""
    mappings = get_type_mappings()
    return mappings['ru_to_en'].get(ru_type)

def get_english_folder(ru_type: str) -> Optional[str]:
    """Преобразует русский тип объекта в английское название каталога."""
    folder_mapping = {
        'РегистрБухгалтерии': 'AccountingRegisters',
        'РегистрНакопления': 'AccumulationRegisters',
        'Справочник': 'Catalogs',
        'ПланСчетов': 'ChartsOfAccounts',
        'ПланВидовХарактеристик': 'ChartsOfCharacteristicTypes',
        'ОбщаяКоманда': 'CommonCommands',
        'ОбщаяФорма': 'CommonForms',
        'ОбщийМодуль': 'CommonModules',
        'ОбщаяКартинка': 'CommonPictures',
        'ОбщийМакет': 'CommonTemplates',
        'Константа': 'Constants',
        'Обработка': 'DataProcessors',
        'Документ': 'Documents',
        'ЖурналДокументов': 'DocumentJournals',
        'Перечисление': 'Enums',
        'ПодпискаНаСобытие': 'EventSubscriptions',
        'ПланОбмена': 'ExchangePlans',
        'ФункциональнаяОпция': 'FunctionalOptions',
        'HTTPСервис': 'HTTPServices',
        'РегистрСведений': 'InformationRegisters',
        'Отчет': 'Reports',
        'Роль': 'Roles'
    }
    
    if is_in_excluded_types(ru_type):
        return None
    
    result = folder_mapping.get(ru_type)
    if not result:
        logger.warning(f"Не найдено английское название каталога для типа '{ru_type}'")
    
    return result

# [Другие вспомогательные функции...] 