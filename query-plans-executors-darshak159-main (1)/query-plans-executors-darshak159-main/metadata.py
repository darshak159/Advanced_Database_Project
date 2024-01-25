# DB catalog & co.
#This file contains the definition of three classes: 
#* Catalog:  The Catalog is designed for use by executors within the DBMS execution engine. 
#It handles table creation, table lookup, index creation, and index lookup (the later will be implemented in a near future).    
#* Table: As the Catalog keeps track of the DB Tables
#* Schema: The corresponding schema for each table

# setup_logger.py
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Schema():
    """
    Schema of a table with the corresponding table names and columns.
    
    Attributes
    ----------
        table_name: str
            table name
        columns: list
            vector of columns name

    Methods
    ------
        getColumns():
            return all the columns in the schema
        getColumns(col_id):
            Returns a specific column from the schema.
        getColumnIdx(col_name);
            Looks up and returns the index of the first column in the schema with the specified name.
        getColumnCount():
            return number of columns
        getTableName():
            return table name
    """
    def __init__(self, table_name, columns):
        self.table_name = table_name
        self.columns = columns
        
    def getColumns(self):
        return self.columns

    def getColumn(self, col_id):
        return self.columns[col_id]
    
    def getColunmIdx(self, col_name):
        for idx, val in enumerate(self.columns):
            if(val == col_name):
                return idx
        return None
    
    def getColumnCount(self):
        return self.columns.size()
    
    def getTableName(self):
        return self.table_name
    
class Table:
    """
    Class representing a table
    
    Attributes
    ----------
        table_id: str
            table id
        schema: Schema
            the corresponding table schema
        dbFile: str
            placeholder to location in disk - We are not using the disk Manager in this assigment

    Methods
    ------
        getSchema()
            returns the correspoding schema
        getTableID()
            returns the table ID
        getDatabaseFile()
            return the location in disk
    """
    def __init__(self,table_id, schema, db_file):
        self.table_id = table_id
        self.schema = schema
        self.dbFile = db_file
    
    def getSchema(self):
        return self.schema
    
    def getTableID(self):
        return self.table_id
    
    def getIterator(self):
        return self.dbFile.getIterator()

class Catalog():
    """
    The Catalog tracks all exisitng tables and their schema
    
    Attributes
    ----------
        table_id_count: int
             track table IDs that will be given to the next table created
        table_names: list
            vector of the existing table names
        tables_name2id: dict
            maps table names to table ID
        tables_id2name: dict
            maps table ID to table names

    Methods
    ------
        createTable():
            creates a new table and returns its ID.
        getTable(table_name):
            Returns the table given its name
        getTable(table_id);
            Looks up for the table given its ID
        getNextID():
            return the id to be given to the next table
    """
    
    
    def __init__(self):
        self.table_id_count = 0
        self.table_names = []
        self.tables_name2id = {}
        self.tables_id2table = {}
        
    def createTable(self, schema, db_file):
        table_id = self.getNextID()
        table = Table(table_id, schema, db_file)
        self.table_names.append(schema.getTableName())
        self.tables_name2id[schema.getTableName()] = table_id
        self.tables_id2table[table_id] = table
        return table_id

    def getTable(self, table_name):
        if(self.table_names.contains(table_name)):
            table_id = self.tables_name2id[table_name]
            return self.tables_id2table[table_id]
        return None
        
    def getTable(self, table_id):
        return self.tables_id2table[table_id]
             
    def getNextID(self):
        self.table_id_count += 1
        return self.table_id_count
    

class Column:
    def __init__(self, col_name, col_type):
        self.column_name = col_name
        self.column_type = col_type
        
    def getName(self):
        return self.colum_name
    
    def getType(self):
        return self.column_type 
  
    #Return the size in bytes of the type.
    #@param type type whose size is to be determined
    #@return size in bytes
    def getLength(self):
        if (self.column_type == 'BOOLEAN'):
                return 1
        elif (self.column_type == 'TINYINT'):
                return 1
        elif (self.column_type == 'SMALLINT'):
                return 2
        elif (self.column_type == 'INTEGER'):
                return 4
        elif (self.column_type == 'BIGINT'):
                return 8
        elif (self.column_type == 'DECIMAL'):
                return 8
        elif (self.column_type == 'TIMESTAMP'):
                return 8
        elif (self.column_type == 'VARCHAR'):
                return 12
        return 0