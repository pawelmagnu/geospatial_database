from Loaders import RegionLoader, TreeLoader, BuildingLoader, RailwayLoader, RoadLoader
from Importers import Importer
import gc

buildings_importer0 = Importer(base_filename="buildings0", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_0.csv")
buildings_times0 = buildings_importer0.import_data()
del buildings_importer0
gc.collect()

buildings_importer1 = Importer(base_filename="buildings1", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_1.csv")
buildings_times1 = buildings_importer1.import_data()
del buildings_importer1
gc.collect()

buildings_importer2 = Importer(base_filename="buildings2", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_2.csv")
buildings_times2 = buildings_importer2.import_data()
del buildings_importer2
gc.collect()

buildings_importer3 = Importer(base_filename="buildings3", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_3.csv")
buildings_times3 = buildings_importer3.import_data()
del buildings_importer3
gc.collect()

buildings_importer4 = Importer(base_filename="buildings4", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_4.csv")
buildings_times4 = buildings_importer4.import_data()
del buildings_importer4
gc.collect()

buildings_importer5 = Importer(base_filename="buildings5", loader_class=BuildingLoader,
                               csv_filename="ads24-buildings_5.csv")
buildings_times5 = buildings_importer5.import_data()
del buildings_importer5
gc.collect()

buildings_times_all = [buildings_times0, buildings_times1, buildings_times2, buildings_times3, buildings_times4, buildings_times5]
buildings_times = [sum(x) for x in zip(*buildings_times_all)]

with open("/home/arcadedb/logs/buildings_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {buildings_times[0]}\n")
    f.write(f"SQL creation time: {buildings_times[1]}\n")
    f.write(f"Import time: {buildings_times[2]}\n")
gc.collect()

tree_importer = Importer(base_filename="trees", loader_class=TreeLoader, csv_filename="ads24-trees.csv")
tree_times = tree_importer.import_data()
with open("/home/arcadedb/logs/trees_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {tree_times[0]}\n")
    f.write(f"SQL creation time: {tree_times[1]}\n")
    f.write(f"Import time: {tree_times[2]}\n")
del tree_importer
gc.collect()

country_importer = Importer(base_filename="countries", loader_class=RegionLoader, csv_filename="ads24-countries.csv")
country_times = country_importer.import_data()
with open("/home/arcadedb/logs/countries_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {country_times[0]}\n")
    f.write(f"SQL creation time: {country_times[1]}\n")
    f.write(f"Import time: {country_times[2]}\n")
del country_importer
gc.collect()

voivo_kwargs = {"region_type": "voivodeship"}
voivodeship_importer = Importer(base_filename="voivodeships", loader_class=RegionLoader, csv_filename="ads24-voivodships.csv", loader_kwargs=voivo_kwargs)
voivodeship_times = voivodeship_importer.import_data()
with open("/home/arcadedb/logs/voivodeships_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {voivodeship_times[0]}\n")
    f.write(f"SQL creation time: {voivodeship_times[1]}\n")
    f.write(f"Import time: {voivodeship_times[2]}\n")
del voivodeship_importer
gc.collect()

powiat_kwargs = {"region_type": "powiat"}
powiat_importer = Importer(base_filename="powiats", loader_class=RegionLoader, csv_filename="ads24-powiats.csv", loader_kwargs=powiat_kwargs)
powiat_times = powiat_importer.import_data()
with open("/home/arcadedb/logs/powiats_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {powiat_times[0]}\n")
    f.write(f"SQL creation time: {powiat_times[1]}\n")
    f.write(f"Import time: {powiat_times[2]}\n")
del powiat_importer
gc.collect()

commune_kwargs = {"region_type": "commune"}
commune_importer = Importer(base_filename="communes", loader_class=RegionLoader, csv_filename="ads24-communes.csv", loader_kwargs=commune_kwargs)
commune_times = commune_importer.import_data()
with open("/home/arcadedb/logs/communes_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {commune_times[0]}\n")
    f.write(f"SQL creation time: {commune_times[1]}\n")
    f.write(f"Import time: {commune_times[2]}\n")
del commune_importer
gc.collect()

city_kwargs = {"region_type": "city"}
city_importer = Importer(base_filename="cities", loader_class=RegionLoader, csv_filename="ads24-cities.csv", loader_kwargs=city_kwargs)
city_times = city_importer.import_data()
with open("/home/arcadedb/logs/cities_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {city_times[0]}\n")
    f.write(f"SQL creation time: {city_times[1]}\n")
    f.write(f"Import time: {city_times[2]}\n")
del city_importer
gc.collect()

roads_importer = Importer(base_filename="roads", loader_class=RoadLoader, csv_filename="ads24-roads.csv")
roads_times = roads_importer.import_data()
with open("/home/arcadedb/logs/roads_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {roads_times[0]}\n")
    f.write(f"SQL creation time: {roads_times[1]}\n")
    f.write(f"Import time: {roads_times[2]}\n")
del roads_importer
gc.collect()

railways_importer = Importer(base_filename="railways", loader_class=RailwayLoader, csv_filename="ads24-railways.csv")
railways_times = railways_importer.import_data()
with open("/home/arcadedb/logs/railways_times.txt", "w", encoding='utf-8') as f:
    f.write(f"DataFrame creation time: {railways_times[0]}\n")
    f.write(f"SQL creation time: {railways_times[1]}\n")
    f.write(f"Import time: {railways_times[2]}\n")
del railways_importer
gc.collect()
