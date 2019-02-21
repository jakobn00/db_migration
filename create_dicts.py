from configdata import system_settings_dict


class PrepareData:

    def __init__(self, conf):
        self.conf = conf
        self.devices_dict = {}
        self.entities_dict = {}
        self.actions_dict = {}
        self.system_settings_dict = {}

    def get_data_from_db(self, dbapi):

        field_names = dbapi.get_field_names('devices')
        rows = dbapi.read_table_devices()
        self.create_devices_dict(field_names, rows)

        field_names = dbapi.get_field_names('entities')
        rows = dbapi.read_table_entities()
        self.create_entities_dict(field_names, rows)

        field_names = dbapi.get_field_names('actions')
        rows = dbapi.read_table_actions()
        self.create_actions_dict(field_names, rows)

        try:
            if self.conf["read_system_settings"]:
                field_names = dbapi.get_field_names('system_settings')
                rows = dbapi.read_table_system_settings()
                self.create_system_settings_dict(field_names, rows)
            else:
                self.system_settings_dict = system_settings_dict
        except:
            pass

        dbapi.conn.commit()
        dbapi.conn.close()

    def prepare_to_insert(self, dbapi):
        self.get_data_from_db(dbapi)
        return self.devices_dict, self.entities_dict, self.actions_dict, self.system_settings_dict

    def update_naming(self, single_row_dict):
        device = single_row_dict.get('device', None)
        port = single_row_dict.get('port', None)
        source = single_row_dict.get('source', None)
        target = single_row_dict.get('target', None)

        if device:
            num = device[len('smartden'):]
            if len(num) == 1:
                device = 'input' + num
            elif len(num) == 3:
                device = 'output' + num

            if port:
                if 'Relay' in port:
                    num = port[len('relay'):]
                    port = 'DI' + num
                single_row_dict.update({'device': device, 'port': port})
            else:
                single_row_dict.update({'device': device})

        elif source and target:
            row = [source, target]
            new_row = []
            for device_port in row:
                # Splits up source or target (device_port) if they consist of several sources or targets
                if 'SmartDEN' in device_port:
                    device_port = [item.strip() for item in device_port.split(',')]
                    new_device_port = ''
                    for dp in device_port:
                        if 'SmartDEN' in dp:
                            device, port = dp.split('.')
                            num = device[len('smartden'):]
                            if len(num) == 1:
                                device = 'input' + num
                            elif len(num) == 3:
                                device = 'output' + num

                            if 'Relay' in port:
                                num = port[len('relay'):]
                                port = 'DI' + num

                            dp = device + '.' + port
                            new_device_port += dp + ', '
                    new_row.append(new_device_port[:-2])
                else:
                    new_row.append(device_port)
            source, target = new_row
            single_row_dict.update({'source': source, 'target': target})

    def create_devices_dict(self, field_names, rows):
        #Todo: Make all table dictionaries nested. Not only this one
        """Creates a nested dictionary of all data in devices table from database source. Each value contains
        a dictionary with the field name as key paired with one value from the field."""

        if field_names[0] != 'device':
            del field_names[0]
            field_names.insert(0, 'device')
        for row in rows:
            row = list(row)
            if self.conf["add_field"]["devices-type"]:
                if field_names[1] != 'type':
                    field_names.insert(1, 'type')
                row.insert(1, None)

            single_row_dict = {key: value for key, value in zip(field_names, row)}
            if self.conf["naming"] == "old":
                self.update_naming(single_row_dict)

            device = single_row_dict['device']
            self.devices_dict[device] = single_row_dict

    def create_entities_dict(self, field_names, rows):
        '''Creates a dictionary from entities table with all columns except id'''
        new_field_names = []
        for name in field_names:
            if name == 'devicetype':
                name = 'type'
            new_field_names.append(name)
        field_names = new_field_names

        for row in rows:
            single_row_dict = {key: value for key, value in zip(field_names, list(row)) if key != 'id'}
            if self.conf["naming"] == "old":
                self.update_naming(single_row_dict)
                if single_row_dict["timerrunning"]:
                    single_row_dict["timerrunning"] = None

            name = single_row_dict['device'] + '.' + single_row_dict['port']
            _row = list(single_row_dict.values())
            self.entities_dict[name] = _row

    def create_actions_dict(self, field_names, rows):
        '''Creates a dictionary from actions table with all data except key is "key_id" and is not a value.
        Therefore not inserted to the database target.'''

        try:
            for row in rows:
                single_row_dict = {key: value for key, value in zip(field_names, list(row))}
                key_id = single_row_dict.pop('key_id')
                if self.conf["naming"] == "old":
                    self.update_naming(single_row_dict)
                _row = list(single_row_dict.values())
                self.actions_dict[key_id] = _row
        except:
            print('actions dict failed.')

    def create_system_settings_dict(self, field_names, rows):
        """Creates a nested dictionary of system settings table. Runs if json configuration allows system_settings
        table to be read"""
        for row in rows:
            single_row_dict = {key: value for key, value in zip(field_names, list(row))}
            id = single_row_dict["id"]
            self.system_settings_dict[id] = single_row_dict
