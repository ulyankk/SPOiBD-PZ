import mysql.connector
import csv
from typing import List, Dict, Any, Optional


class Table:
    def __init__(self, db_type: str, host: str, user: str, password: str, database: str, table_name: str):
        self.db_type = db_type.lower()
        self.table_name = table_name
        self.connection = self._connect(host, user, password, database)
        self.cursor = self.connection.cursor()
        self.primary_key = self._find_primary_key()

    def _connect(self, host: str, user: str, password: str, database: str):
        if self.db_type == 'mysql':
            return mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
        elif self.db_type == 'postgresql':
            import psycopg2
            return psycopg2.connect(
                host=host, user=user, password=password, dbname=database
            )
        else:
            raise ValueError("Используй 'mysql' или 'postgresql'")

    def _find_primary_key(self) -> Optional[str]:
        if self.db_type == 'mysql':
            self.cursor.execute(f"SHOW KEYS FROM {self.table_name} WHERE Key_name = 'PRIMARY'")
            result = self.cursor.fetchone()
            return result[4] if result else None
        else:
            self.cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (self.table_name,))
            result = self.cursor.fetchone()
            return result[0] if result else None

    def _build_where(self, filters: Optional[Dict[str, Any]] = None):
        if not filters:
            return "", []
        conditions = []
        values = []
        for col, val in filters.items():
            conditions.append(f"{col} = %s")
            values.append(val)
        return "WHERE " + " AND ".join(conditions), values

    def select_column_sorted(self, column: str, ascending: bool = True, filters: Optional[Dict[str, Any]] = None):
        where_clause, values = self._build_where(filters)
        order = "ASC" if ascending else "DESC"
        query = f"SELECT {column} FROM {self.table_name} {where_clause} ORDER BY {column} {order}"
        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def select_id_range(self, start_id: int, end_id: int, filters: Optional[Dict[str, Any]] = None):
        if not self.primary_key:
            raise ValueError("Нет первичного ключа")
        where_clause, values = self._build_where(filters)
        if where_clause:
            query = f"SELECT * FROM {self.table_name} WHERE {self.primary_key} BETWEEN %s AND %s AND {where_clause[6:]}"
            values = [start_id, end_id] + values
        else:
            query = f"SELECT * FROM {self.table_name} WHERE {self.primary_key} BETWEEN %s AND %s"
            values = [start_id, end_id]
        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def delete_id_range(self, start_id: int, end_id: int):
        if not self.primary_key:
            raise ValueError("Нет первичного ключа")
        query = f"DELETE FROM {self.table_name} WHERE {self.primary_key} BETWEEN %s AND %s"
        self.cursor.execute(query, (start_id, end_id))
        self.connection.commit()
        return self.cursor.rowcount

    def show_structure(self):
        if self.db_type == 'mysql':
            self.cursor.execute(f"DESCRIBE {self.table_name}")
        else:
            self.cursor.execute("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = %s
            """, (self.table_name,))
        return self.cursor.fetchall()

    def search_by_value(self, column: str, value: Any, filters: Optional[Dict[str, Any]] = None):
        where_clause, values = self._build_where(filters)
        if where_clause:
            query = f"SELECT * FROM {self.table_name} WHERE {column} = %s AND {where_clause[6:]}"
            values.insert(0, value)
        else:
            query = f"SELECT * FROM {self.table_name} WHERE {column} = %s"
            values = [value]
        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def drop_table(self):
        self.cursor.execute(f"DROP TABLE IF EXISTS {self.table_name}")
        self.connection.commit()

    def add_column(self, column_name: str, column_type: str):
        query = f"ALTER TABLE {self.table_name} ADD COLUMN {column_name} {column_type}"
        self.cursor.execute(query)
        self.connection.commit()

    def drop_column(self, column_name: str):
        query = f"ALTER TABLE {self.table_name} DROP COLUMN {column_name}"
        self.cursor.execute(query)
        self.connection.commit()

    def export_to_csv(self, filename: str):
        self.cursor.execute(f"SELECT * FROM {self.table_name}")
        data = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(data)

    def import_from_csv(self, filename: str):
        with open(filename, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            columns = next(reader)
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            for row in reader:
                self.cursor.execute(query, row)
        self.connection.commit()

    def join(self, other_table, join_type: str, left_col: str, right_col: str,
             columns: Optional[List[str]] = None, filters: Optional[Dict[str, Any]] = None):
        join_type = join_type.upper()
        select = "*" if not columns else ", ".join(columns)
        where, values = self._build_where(filters)

        if join_type == 'FULL' and self.db_type == 'mysql':
            left_query = f"""
                SELECT {select}
                FROM {self.table_name}
                LEFT JOIN {other_table.table_name}
                ON {self.table_name}.{left_col} = {other_table.table_name}.{right_col}
                {where}
            """
            right_query = f"""
                SELECT {select}
                FROM {self.table_name}
                RIGHT JOIN {other_table.table_name}
                ON {self.table_name}.{left_col} = {other_table.table_name}.{right_col}
                {where}
            """
            query = f"{left_query} UNION {right_query}"
            self.cursor.execute(query, values + values)
            return self.cursor.fetchall()

        query = f"""
            SELECT {select}
            FROM {self.table_name}
            {join_type} JOIN {other_table.table_name}
            ON {self.table_name}.{left_col} = {other_table.table_name}.{right_col}
            {where}
        """
        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def union(self, other_table, columns: List[str],
              filters_self: Optional[Dict[str, Any]] = None,
              filters_other: Optional[Dict[str, Any]] = None,
              union_all: bool = False):
        union_type = "UNION ALL" if union_all else "UNION"
        where1, vals1 = self._build_where(filters_self)
        where2, vals2 = self._build_where(filters_other)
        cols = ", ".join(columns)
        query = f"""
            SELECT {cols} FROM {self.table_name} {where1}
            {union_type}
            SELECT {cols} FROM {other_table.table_name} {where2}
        """
        self.cursor.execute(query, vals1 + vals2)
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()
        self.connection.close()


if __name__ == "__main__":
    db = Table('mysql', '127.0.0.1', 'root', 'admin', 'hospital', 'doctor')

    print("СТРУКТУРА ТАБЛИЦЫ")
    print(db.show_structure())

    print("\nВСЕ ФАМИЛИИ")
    print(db.select_column_sorted('lastname', ascending=True))

    print("\nТОЛЬКО ХИРУРГИ")
    print(db.select_column_sorted('lastname', ascending=True, filters={'specialty': 'Хирург'}))

    print("\nПОИСК ПО СПЕЦИАЛЬНОСТИ")
    print(db.search_by_value('specialty', 'Кардиолог'))

    print("\nJOIN (INNER)")
    appointment = Table('mysql', '127.0.0.1', 'root', 'admin', 'hospital', 'appointment')
    result = db.join(appointment, 'INNER', 'id_doctor', 'doctor_id')
    print(result)

    print("\nUNION")
    archive = Table('mysql', '127.0.0.1', 'root', 'admin', 'hospital', 'doctor')
    result = db.union(archive, ['lastname', 'firstname', 'specialty'])
    print(result)

    print("\nPOSTGRESQL")
    db_pg = Table('postgresql', '127.0.0.1', 'postgres', 'admin', 'hospital', 'doctor')
    print(db_pg.select_column_sorted('lastname', ascending=True))
    db_pg.close()

    db.close()
