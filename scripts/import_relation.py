from Loaders import CityWithinCommuneLoader, CommuneWithinPowiatLoader, PowiatWithinVoivodeshipLoader, \
    VoivodeshipWithinCountryLoader, NeighbouringCommunesLoader, TreesNearRoadLoader, TreesNearTreesLoader
from Importers import RelationImporter
import gc
import sys

if len(sys.argv) > 1:
    relation_no = int(sys.argv[1])
else:
    relation_no = 1

if relation_no == 1:
    city_within_commune_importer = RelationImporter(loader_class=CityWithinCommuneLoader,
                                                    csv_filenames=["ads24-cities.csv", "ads24-communes.csv"],
                                                    relation_name="city_within_commune")
    city_within_commune_times = city_within_commune_importer.import_data()
    with open("/home/arcadedb/logs/city_within_commune_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {city_within_commune_times[0]}\n")
        f.write(f"SQL creation time: {city_within_commune_times[1]}\n")
        f.write(f"Import time: {city_within_commune_times[2]}\n")
    del city_within_commune_importer
    gc.collect()
elif relation_no == 2:
    commune_within_powiat_importer = RelationImporter(loader_class=CommuneWithinPowiatLoader,
                                                      csv_filenames=["ads24-powiats.csv", "ads24-communes.csv"],
                                                      relation_name="commune_within_powiat")
    commune_within_powiat_times = commune_within_powiat_importer.import_data()
    with open("/home/arcadedb/logs/commune_within_powiat_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {commune_within_powiat_times[0]}\n")
        f.write(f"SQL creation time: {commune_within_powiat_times[1]}\n")
        f.write(f"Import time: {commune_within_powiat_times[2]}\n")
    del commune_within_powiat_importer
    gc.collect()
elif relation_no == 3:
    powiat_within_voivodeship_importer = RelationImporter(loader_class=PowiatWithinVoivodeshipLoader,
                                                          csv_filenames=["ads24-voivodships.csv", "ads24-powiats.csv"],
                                                          relation_name="powiat_within_voivodeship")
    powiat_within_voivodeship_times = powiat_within_voivodeship_importer.import_data()
    with open("/home/arcadedb/logs/powiat_within_voivodeship_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {powiat_within_voivodeship_times[0]}\n")
        f.write(f"SQL creation time: {powiat_within_voivodeship_times[1]}\n")
        f.write(f"Import time: {powiat_within_voivodeship_times[2]}\n")
    del powiat_within_voivodeship_importer
    gc.collect()
elif relation_no == 4:
    voivodeship_within_country_importer = RelationImporter(loader_class=VoivodeshipWithinCountryLoader,
                                                           csv_filenames=["ads24-countries.csv",
                                                                          "ads24-voivodships.csv"],
                                                           relation_name="voivodeship_within_country")
    voivodeship_within_country_times = voivodeship_within_country_importer.import_data()
    with open("/home/arcadedb/logs/voivodeship_within_country_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {voivodeship_within_country_times[0]}\n")
        f.write(f"SQL creation time: {voivodeship_within_country_times[1]}\n")
        f.write(f"Import time: {voivodeship_within_country_times[2]}\n")
    del voivodeship_within_country_importer
    gc.collect()
elif relation_no == 5:
    neighbouring_communes_importer = RelationImporter(loader_class=NeighbouringCommunesLoader,
                                                      csv_filenames=["ads24-communes.csv"],
                                                      relation_name="neighbouring_communes")
    neighbouring_communes_times = neighbouring_communes_importer.import_data()
    with open("/home/arcadedb/logs/neighbouring_communes_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {neighbouring_communes_times[0]}\n")
        f.write(f"SQL creation time: {neighbouring_communes_times[1]}\n")
        f.write(f"Import time: {neighbouring_communes_times[2]}\n")
    del neighbouring_communes_importer
    gc.collect()
elif relation_no == 6:
    pass
elif relation_no == 7:
    trees_near_trees_importer = RelationImporter(loader_class=TreesNearTreesLoader,
                                                 csv_filenames=["ads24-trees.csv"],
                                                 relation_name="trees_near_trees")
    trees_near_trees_times = trees_near_trees_importer.import_data()
    with open("/home/arcadedb/logs/trees_near_trees_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {trees_near_trees_times[0]}\n")
        f.write(f"SQL creation time: {trees_near_trees_times[1]}\n")
        f.write(f"Import time: {trees_near_trees_times[2]}\n")
    del trees_near_trees_importer
    gc.collect()
elif relation_no == 8:
    trees_near_road_importer = RelationImporter(loader_class=TreesNearRoadLoader,
                                                csv_filenames=["ads24-trees.csv", "ads24-roads.csv"],
                                                relation_name="trees_near_road")
    trees_near_road_times = trees_near_road_importer.import_data()
    with open("/home/arcadedb/logs/trees_near_road_times.txt", "w", encoding='utf-8') as f:
        f.write(f"DataFrame creation time: {trees_near_road_times[0]}\n")
        f.write(f"SQL creation time: {trees_near_road_times[1]}\n")
        f.write(f"Import time: {trees_near_road_times[2]}\n")
    del trees_near_road_importer
    gc.collect()
elif relation_no == 9:
    pass
elif relation_no == 10:
    pass
else:
    print("Invalid relation number")
    sys.exit(1)
