import unittest
import os
import sqlite3
from ent1ctosqlite.core import parse_configuration, analyze_directory
from ent1ctosqlite.database import create_database, check_database_integrity
import logging
import tempfile
import shutil

# Change logger name
logger = logging.getLogger('ent1ctosqlite')

class TestParser(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.conn = sqlite3.connect(':memory:')
        create_database(self.conn)
        self.temp_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Tear down test fixtures."""
        self.conn.close()
        shutil.rmtree(self.temp_dir)
        
    def test_parse_configuration(self):
        """Test parsing a configuration file."""
        # Create a minimal test Configuration.xml
        config_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <MetaDataObject xmlns="http://v8.1c.ru/8.3/MDClasses" xmlns:ns="http://v8.1c.ru/8.3/MDClasses">
            <Configuration>
                <Name>TestConfig</Name>
                <ChildObjects>
                    <ns:Document>TestDocument</ns:Document>
                    <ns:Catalog>TestCatalog</ns:Catalog>
                </ChildObjects>
            </Configuration>
        </MetaDataObject>
        """
        config_path = os.path.join(self.temp_dir, "Configuration.xml")
        with open(config_path, "w", encoding="utf-8") as f:
            f.write(config_xml)
            
        # Test parsing
        objects = parse_configuration(config_path, self.conn)
        self.assertEqual(len(objects), 2)
        self.assertEqual(objects[0], ("Document", "TestDocument"))
        self.assertEqual(objects[1], ("Catalog", "TestCatalog"))
        
        # Test database integrity
        cursor = self.conn.cursor()
        cursor.execute("SELECT obj_type, obj_name FROM objects ORDER BY obj_id")
        results = cursor.fetchall()
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], ("Document", "TestDocument"))
        self.assertEqual(results[1], ("Catalog", "TestCatalog"))
        
    def test_analyze_directory(self):
        """Test analyzing a configuration directory."""
        # Create test directory structure
        os.makedirs(os.path.join(self.temp_dir, "Documents", "TestDoc"))
        os.makedirs(os.path.join(self.temp_dir, "Catalogs", "TestCat"))
        
        # Create test Configuration.xml
        config_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <MetaDataObject xmlns="http://v8.1c.ru/8.3/MDClasses" xmlns:ns="http://v8.1c.ru/8.3/MDClasses">
            <Configuration>
                <Name>TestConfig</Name>
                <ChildObjects>
                    <ns:Document>TestDoc</ns:Document>
                    <ns:Catalog>TestCat</ns:Catalog>
                </ChildObjects>
            </Configuration>
        </MetaDataObject>
        """
        with open(os.path.join(self.temp_dir, "Configuration.xml"), "w", encoding="utf-8") as f:
            f.write(config_xml)
            
        # Create test module file
        module_path = os.path.join(self.temp_dir, "Documents", "TestDoc", "Ext", "ObjectModule.bsl")
        os.makedirs(os.path.dirname(module_path))
        with open(module_path, "w", encoding="utf-8") as f:
            f.write("""
            Процедура ТестовыйМетод() Экспорт
                // Test method
            КонецПроцедуры
            
            Функция ТестоваяФункция(Параметр1, Параметр2) Экспорт
                Возврат Истина;
            КонецФункции
            """)
            
        # Test analysis
        analyze_directory(self.temp_dir, self.conn)
        
        # Verify results
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM objects")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)  # Should find both objects
        
        cursor.execute("SELECT COUNT(*) FROM code_body")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 1)  # Should find the module
        
        cursor.execute("SELECT COUNT(*) FROM methods")
        count = cursor.fetchone()[0]
        self.assertEqual(count, 2)  # Should find both methods
        
        cursor.execute("SELECT methods_name, methods_if_func, methods_is_export FROM methods ORDER BY methods_name")
        methods = cursor.fetchall()
        self.assertEqual(methods[0], ("ТестоваяФункция", 1, 1))  # Function, Export
        self.assertEqual(methods[1], ("ТестовыйМетод", 0, 1))    # Procedure, Export

if __name__ == '__main__':
    unittest.main() 