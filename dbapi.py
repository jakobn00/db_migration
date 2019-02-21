import sqlite3
import time


#Todo:
"""
Clean up a lot here. Should be only one method for each sql command.
Example:
get_table_info
read_table
insert_into_db
update_db
delete_from_db

Probably would be better to read the whole db, no WHERE statements.
Improve usage of variables in sql strings.
"""

class DBAPI:

    def __init__(self, __db_name, conf):
        self.conf = conf
        self.conn = sqlite3.connect(__db_name)
        self.c = self.conn.cursor()

    def get_field_names(self, table):
        #Todo: f-strings are dangerous in sql code! Replace with question marks (?).
        self.c.execute(f'PRAGMA table_info({table})')
        column_data = self.c.fetchall()
        field_names = [row[1] for row in column_data]
        return field_names

    def read_table(self, table, sql_string=None):
        """General read db method. Will hopefully replace the other special read methods in the future."""

        self.c.execute("SELECT * FROM devices")
        return self.c.fetchall()

    def read_table_devices(self):
        if self.conf["naming"] == "old":
            self.c.execute('SELECT * FROM devices WHERE name LIKE "SmartDEN%"')
        elif self.conf["naming"] == "new":
            self.c.execute('SELECT * FROM devices WHERE description LIKE "SmartDEN%"')
        return self.c.fetchall()

    def read_table_entities(self):
        if self.conf["naming"] == "old":
            self.c.execute('SELECT * FROM entities WHERE devicetype = "switch" OR devicetype = "output"'
                           'OR devicetype = "schedule"')
        elif self.conf["naming"] == "new":
            self.c.execute('SELECT * FROM entities WHERE devicetype = "switch" OR devicetype = "output"'
                           'OR devicetype = "schedule"')
        return self.c.fetchall()

    def read_table_actions(self):
        if self.conf["naming"] == "old":
            self.c.execute("SELECT * FROM actions WHERE source LIKE 'SmartDEN%' OR target LIKE 'SmartDEN%'")
        elif self.conf["naming"] == "new":
            self.c.execute("SELECT * FROM actions WHERE source LIKE 'input%' OR target LIKE 'output%'")
        return self.c.fetchall()

    def read_table_system_settings(self):
        self.c.execute('SELECT * FROM system_settings')
        return self.c.fetchall()

    def insert_into_db(self, table_dicts):
        devices_dict, entities_dict, actions_dict = table_dicts

        try:
            for row_dict in devices_dict.values():
                row = tuple(row_dict.values())
                # self.c.execute("""INSERT INTO devices
                # (device, type, password, group, description, address, hw_address, active)
                # VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", row)
                self.c.execute("""INSERT INTO devices VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", row)

            for row in entities_dict.values():
                self.c.execute('INSERT INTO entities (timestamp, timerrunning, active, type, '
                               'device, port, state, description, remark)'
                               'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(row))

            for row in actions_dict.values():
                self.c.execute('INSERT INTO actions (active, source, target, '
                               'newstate, conditions, s_description, t_description, '
                               'enable_time, d_start, d_stop, t_start, t_stop, weekdays, '
                               'comment, trigger, triggerpressedtime, timer) '
                               'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tuple(row))
        except Exception as e:
            print(f"Insertion error: {e}")

    def update_db(self, _table, table_dict):
        if _table == "system_settings":
            if not self.conf["read_system_settings"]:
                try:
                    for variable, data in table_dict.items():
                        self.c.execute("""UPDATE system_settings
                                       SET data = ?
                                       WHERE variable = ?""", (data, variable))
                except Exception as e:
                    print(f"Update system_settings error: {e}")
            else:
                for row_dict in table_dict.values():
                    variable, data = row_dict["variable"], row_dict["data"]
                    self.c.execute("""UPDATE system_settings
                                       SET data = ?
                                       WHERE variable = ?""", (data, variable))



