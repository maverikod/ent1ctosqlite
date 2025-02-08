"""
VCV Parser - инструмент для анализа конфигураций 1С:Предприятие 8.3
"""

from .core import (
    extract_vcv,
    parse_configuration,
    parse_form_and_code,
    parse_module,
    parse_methods,
    parse_method_args,
    parse_predefined,
    analyze_object
)

from .database import (
    create_database,
    check_database_integrity,
    check_and_update_database_structure,
    get_table_info
)

from .utils import (
    setup_logger,
    find_configuration_root,
    extract_synonym,
    determine_module_type,
    get_type_ru,
    get_type_en,
    get_english_folder,
    is_in_excluded_types
)

__version__ = '0.1.1'
__author__ = 'maverikod'
__email__ = 'vasilyvz@gmail.com'

# Настройка логгера по умолчанию
import logging
logging.getLogger('ent1ctosqlite').addHandler(logging.NullHandler())