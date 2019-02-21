from dbapi import DBAPI
from create_dicts import PrepareData
import json
import shutil
import os
from configdata import json_file_name, logfile_name, sheap_path


class Migrate():
    def __init__(self):
        self.conf = None
        self.db_source = None
        self.db_target = None

    def load_json(self):
        try:
            with open(json_file_name, 'r') as f:
                self.conf = json.load(f)
        except FileNotFoundError as e:
            print(e)

    def commit_changes(self, db_target):
        if self.conf["commit"]:
            if not os.path.exists("UpdatedDBfiles"):
                os.makedirs("UpdatedDBfiles")
            shutil.copyfile(self.conf["db_target"], "temp.db")
            db_target.conn.commit()
            db_target.conn.close()

            if self.conf["move_to_sheap"]:
                shutil.move(self.conf["db_target"], "{}/based_on_{}".format(sheap_path, self.conf["db_source"]))
            else:
                shutil.move(self.conf["db_target"], "UpdatedDBfiles/based_on_{}".format(self.conf["db_source"]))
            os.rename("temp.db", self.conf["db_target"])

            print("Data migrated")
        else:
            db_target.conn.close()
            print("Look at json configuration file to commit changes.")

    def main(self):
        self.load_json()

        db_source = DBAPI(self.conf["db_source"], self.conf)
        db_target = DBAPI(self.conf["db_target"], self.conf)

        data = PrepareData(self.conf)
        devices_dict, entities_dict, actions_dict, system_settings_dict = data.prepare_to_insert(db_source)

        try:
            db_target.insert_into_db([devices_dict, entities_dict, actions_dict])
            db_target.update_db('system_settings', system_settings_dict)
        except Exception as e:
            print(e)
        else:
            self.commit_changes(db_target)


if __name__ == '__main__':
    Migrate().main()