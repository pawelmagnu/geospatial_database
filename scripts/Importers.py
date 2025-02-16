from time import time
import subprocess
import os


class Importer:
    def __init__(self, base_filename, loader_class, csv_filename, arcade_data_path="/home/arcadedb/data",
                 loader_kwargs=None):
        self.base_filename = base_filename
        self.loader_class = loader_class
        self.loader = None
        self.arcade_data_path = arcade_data_path
        self.full_csv_path = f"{self.arcade_data_path}/{csv_filename}"
        self.full_sql_path = f"{self.arcade_data_path}/{base_filename}.sql"
        self.loader_kwargs = loader_kwargs if loader_kwargs is not None else {}

    def __del__(self):
        del self.loader

    def import_data(self):
        start = time()
        self.loader = self.loader_class(filename=self.full_csv_path, **self.loader_kwargs)
        end = time()
        df_creation_time = end - start

        start = time()
        self.loader.create_sql(self.full_sql_path)
        end = time()
        sql_creation_time = end - start

        sql_files = [f for f in os.listdir(self.arcade_data_path) if self.base_filename in f and ".sql" in f]

        start = time()
        for file in sql_files:
            subprocess.run(["/home/arcadedb/bin/console.sh", "-b", f"load {self.arcade_data_path}/{file}"])
        end = time()

        import_time = end - start

        return df_creation_time, sql_creation_time, import_time


class RelationImporter:
    def __init__(self, loader_class, csv_filenames, relation_name, relation_type="within",
                 arcade_data_path="/home/arcadedb/data"):
        # self.base_filename = base_filename
        self.loader_class = loader_class
        self.loader = None
        self.arcade_data_path = arcade_data_path
        self.full_sql_path = f"{self.arcade_data_path}/{relation_name}.sql"
        self.csv_filenames = [f"{arcade_data_path}/{filename}" for filename in csv_filenames]
        self.relation_name = relation_name
        self.relation_type = relation_type

    def __del__(self):
        del self.loader

    def import_data(self):
        start = time()
        self.loader = self.loader_class(filenames=self.csv_filenames, relation_name=self.relation_name,
                                        relation_type=self.relation_type)
        end = time()
        df_creation_time = end - start

        start = time()
        self.loader.create_sql(self.full_sql_path)
        end = time()
        sql_creation_time = end - start

        sql_files = [f for f in os.listdir(self.arcade_data_path) if self.relation_name in f and ".sql" in f]

        start = time()
        for file in sql_files:
            subprocess.run(["/home/arcadedb/bin/console.sh", "-b", f"load {self.arcade_data_path}/{file}"])
        end = time()

        import_time = end - start

        return df_creation_time, sql_creation_time, import_time
