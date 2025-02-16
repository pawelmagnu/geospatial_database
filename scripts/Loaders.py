import numpy as np
import pandas as pd
import geopandas as gpd
import shapely
from shapely import wkt
from shapely.geometry import Point, Polygon, MultiPolygon, LineString
from tqdm import tqdm
from scipy.spatial import cKDTree


from abc import abstractmethod


class Loader:
    def __init__(self, filename, max_queries=None):
        self.df = pd.read_csv(filename, encoding="utf-8")

        # self.geometries = self.df.wkt.apply(lambda x: shapely.from_wkt(x))
        # self.geo_df = gpd.GeoDataFrame(index=self.df.index, crs='epsg:4326', geometry=self.geometries)
        # self.geo_df = self.geo_df.to_crs(epsg=2180)
        self.max_queries = max_queries
        self.max_file_size = 100000000

    def __del__(self):
        del self.df
        # del self.geometries
        # del self.geo_df

    def create_sql(self, filename):
        max_queries = self.max_queries if self.max_queries is not None else len(self.df)
        one_query_size = len(self.get_query(0))
        batch_size = one_query_size * 10000
        print(f"Max queries: {max_queries}, one query size: {one_query_size}, batch size: {batch_size}")
        # how many batches can we fit in one 100MB file
        max_batches = max(1, self.max_file_size // batch_size)
        print(f"Max batches: {max_batches}")
        how_many_files = int(np.ceil(max_queries / (max_batches * 10000)))
        print(f"How many files: {how_many_files}")
        for file_idx in range(how_many_files):
            with open(f"{filename[:-4]}_{file_idx}.sql", "w", encoding='utf-8') as f:
                f.write('connect remote:localhost/ads24_db root rootroot;\n')
                f.write('begin;\n')
                for i in tqdm(
                        range(file_idx * max_batches * 10000, min((file_idx + 1) * max_batches * 10000, max_queries))):
                    query = self.get_query(i)
                    f.write(query)
                    if i % 10000 == 9999:
                        f.write('commit;\n')
                        f.write('begin;\n')
                f.write('commit;\n')
                f.write('exit;\n')

    @abstractmethod
    def get_query(self, idx):
        pass


class RegionLoader(Loader):
    def __init__(self, filename, max_queries=None, region_type='country'):
        super().__init__(filename, max_queries)
        self.region_type = region_type

    def get_query(self, idx):
        # region_geom = self.geo_df.geometry[idx]
        # query = f'insert into region set name="{self.df["name"][idx]}", type="{self.region_type}", geom={shapely.to_geojson(region_geom)};'
        query = f'insert into region set name="{self.df["name"][idx]}", type="{self.region_type}";'
        return query


class TreeLoader(Loader):
    def __init__(self, filename, max_queries=None):
        super().__init__(filename, max_queries)

    def get_query(self, idx):
        # tree_geom = self.geo_df.geometry[idx]
        # query = f'insert into tree set type="tree", geom={shapely.to_geojson(tree_geom)};'
        query = f'insert into tree set type="tree", id="{self.df["id"][idx]}";'
        return query


class BuildingLoader(Loader):
    def __init__(self, filename, max_queries=None):
        super().__init__(filename, max_queries)
        for column in self.df.columns:
            self.df[column] = self.df[column].astype(str).str.replace('\"', '')
            self.df[column] = self.df[column].astype(str).str.replace(' ', '_')


    def get_query(self, idx):
        # building_geom = self.geo_df.geometry[idx]
        building_subtype = self.df["building"][idx] if self.df["building"][idx] is not None else "null"
        # query = f'insert into building set type="building", subtype={building_subtype},  geom={shapely.to_geojson(building_geom)};'
        query = f'insert into building set type="building", subtype="{building_subtype}";'
        return query


class RailwayLoader(Loader):
    def __init__(self, filename, max_queries=None):
        super().__init__(filename, max_queries)
        for column in self.df.columns:
            self.df[column] = self.df[column].astype(str).str.replace('\"', '')
            self.df[column] = self.df[column].astype(str).str.replace(' ', '_')

    def get_query(self, idx):
        # railway_geom = self.geo_df.geometry[idx]
        # query = f'insert into railway set type="railway", subtype={str(self.df["railway"][idx])},  geom={shapely.to_geojson(railway_geom)};'
        query = f'insert into railway set type="railway", subtype="{str(self.df["railway"][idx])}";'
        return query


class RoadLoader(Loader):
    def __init__(self, filename, max_queries=None):
        super().__init__(filename, max_queries)
        for column in self.df.columns:
            self.df[column] = self.df[column].astype(str).str.replace('\"', '')
            self.df[column] = self.df[column].astype(str).str.replace(' ', '_')

    def get_query(self, idx):
        # road_geom = self.geo_df.geometry[idx]
        road_name = self.df["name"][idx] if self.df["name"][idx] is not None else "null"
        # query = f'insert into road set type="road", name="{road_name}", class="{self.df["road_class"][idx]}", nodes="{self.df["nodes"][idx]}", geom={shapely.to_geojson(road_geom)};'
        query = f'insert into road set type="road", name="{road_name}", class="{self.df["road_class"][idx]}", nodes="{self.df["nodes"][idx]}";'
        return query


class RelationLoader:
    def __init__(self, filenames, relation_name, relation_type="within"):
        self.filenames = filenames
        self.relation_name = relation_name
        self.relation_type = relation_type
        self.relation_result_gdf = None
        self.max_file_size = 100000000

    def create_sql(self, full_sql_path):
        max_queries = len(self.relation_result_gdf)
        one_query_size = len(self.get_query(0))
        batch_size = one_query_size * 10000
        print(f"Max queries: {max_queries}, one query size: {one_query_size}, batch size: {batch_size}")
        # how many batches can we fit in one 100MB file
        max_batches = max(1, self.max_file_size // batch_size)
        print(f"Max batches: {max_batches}")
        how_many_files = int(np.ceil(max_queries / (max_batches * 10000)))
        print(f"How many files: {how_many_files}")
        for file_idx in range(how_many_files):
            with open(f"{full_sql_path[:-4]}_{file_idx}.sql", "w", encoding='utf-8') as f:
                f.write('connect remote:localhost/ads24_db root rootroot;\n')
                f.write('begin;\n')
                for i in tqdm(
                        range(file_idx * max_batches * 10000, min((file_idx + 1) * max_batches * 10000, max_queries))):
                    query = self.get_query(i)
                    f.write(query)
                    if i % 10000 == 9999:
                        f.write('commit;\n')
                        f.write('begin;\n')
                f.write('commit;\n')
                f.write('exit;\n')

    @abstractmethod
    def get_query(self, idx):
        pass


class CityWithinCommuneLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="within"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.city_df = gpd.read_file(self.filenames[0], encoding="utf-8")
        self.commune_df = gpd.read_file(self.filenames[1], encoding="utf-8")
        self.city_df['geometry'] = self.city_df['wkt'].apply(wkt.loads)
        self.commune_df['geometry'] = self.commune_df['wkt'].apply(wkt.loads)
        self.cities_gdf = gpd.GeoDataFrame(self.city_df, geometry='geometry', crs='EPSG:4326')
        self.communes_gdf = gpd.GeoDataFrame(self.commune_df, geometry='geometry', crs='EPSG:4326')
        self.cities_gdf['geometry'] = self.cities_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.communes_gdf['geometry'] = self.communes_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.relation_result_gdf = gpd.sjoin(self.cities_gdf, self.communes_gdf, predicate='within')
        self.relation_result_gdf = self.relation_result_gdf[['name_left', 'name_right']].reset_index(drop=True)

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM region WHERE name = '{self.relation_result_gdf['name_left'][idx]}' and type='city') TO(SELECT FROM region WHERE name='{self.relation_result_gdf['name_right'][idx]}' and type='commune') UNIDIRECTIONAL;"
        return query


class CommuneWithinPowiatLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="within"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.powiat_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.commune_df = pd.read_csv(filenames[1], encoding="utf-8")
        self.powiat_df['geometry'] = self.powiat_df['wkt'].apply(wkt.loads)
        self.commune_df['geometry'] = self.commune_df['wkt'].apply(wkt.loads)
        self.powiat_gdf = gpd.GeoDataFrame(self.powiat_df, geometry='geometry', crs='EPSG:4326')
        self.commune_gdf = gpd.GeoDataFrame(self.commune_df, geometry='geometry', crs='EPSG:4326')
        self.powiat_gdf['geometry'] = self.powiat_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.commune_gdf['geometry'] = self.commune_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.relation_result_gdf = gpd.sjoin(self.commune_gdf, self.powiat_gdf, predicate='within')
        self.relation_result_gdf = self.relation_result_gdf[['name_left', 'name_right']].reset_index(drop=True)

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM region WHERE name = '{self.relation_result_gdf['name_left'][idx]}' and type='commune') TO(SELECT FROM region WHERE name='{self.relation_result_gdf['name_right'][idx]}' and type='powiat') UNIDIRECTIONAL;"
        return query


class PowiatWithinVoivodeshipLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="within"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.voivodeship_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.powiat_df = pd.read_csv(filenames[1], encoding="utf-8")
        self.voivodeship_df['geometry'] = self.voivodeship_df['wkt'].apply(wkt.loads)
        self.powiat_df['geometry'] = self.powiat_df['wkt'].apply(wkt.loads)
        self.voivodeship_gdf = gpd.GeoDataFrame(self.voivodeship_df, geometry='geometry', crs='EPSG:4326')
        self.powiat_gdf = gpd.GeoDataFrame(self.powiat_df, geometry='geometry', crs='EPSG:4326')
        self.voivodeship_gdf['geometry'] = self.voivodeship_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.powiat_gdf['geometry'] = self.powiat_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.relation_result_gdf = gpd.sjoin(self.powiat_gdf, self.voivodeship_gdf, predicate='within')
        self.relation_result_gdf = self.relation_result_gdf[['name_left', 'name_right']].reset_index(drop=True)

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM region WHERE name = '{self.relation_result_gdf['name_left'][idx]}' and type='powiat') TO(SELECT FROM region WHERE name='{self.relation_result_gdf['name_right'][idx]}' and type='voivodeship') UNIDIRECTIONAL;"
        return query


class VoivodeshipWithinCountryLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="within"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.country_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.voivodeship_df = pd.read_csv(filenames[1], encoding="utf-8")
        self.country_df['geometry'] = self.country_df['wkt'].apply(wkt.loads)
        self.voivodeship_df['geometry'] = self.voivodeship_df['wkt'].apply(wkt.loads)
        self.country_gdf = gpd.GeoDataFrame(self.country_df, geometry='geometry', crs='EPSG:4326')
        self.voivodeship_gdf = gpd.GeoDataFrame(self.voivodeship_df, geometry='geometry', crs='EPSG:4326')
        self.country_gdf['geometry'] = self.country_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.voivodeship_gdf['geometry'] = self.voivodeship_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.relation_result_gdf = gpd.sjoin(self.voivodeship_gdf, self.country_gdf, predicate='within')
        self.relation_result_gdf = self.relation_result_gdf[['name_left', 'name_right']].reset_index(drop=True)

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM region WHERE name = '{self.relation_result_gdf['name_left'][idx]}' and type='voivodeship') TO(SELECT FROM region WHERE name='{self.relation_result_gdf['name_right'][idx]}' and type='country') UNIDIRECTIONAL;"
        return query


class NeighbouringCommunesLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="neighbour"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.relation_type = "neighbour"
        self.commune_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.commune_df['geometry'] = self.commune_df['wkt'].apply(wkt.loads)
        self.commune_gdf = gpd.GeoDataFrame(self.commune_df, geometry='geometry', crs='EPSG:4326')
        self.commune_gdf['geometry'] = self.commune_gdf['geometry'].apply(
            lambda geom: Polygon(list(geom.coords)) if isinstance(geom, LineString) else
            MultiPolygon([Polygon(list(line.coords)) for line in geom.geoms]) if isinstance(geom,
                                                                                            MultiPolygon) else geom
        )
        self.relation_result_gdf = gpd.sjoin(self.commune_gdf, self.commune_gdf, predicate='intersects')
        self.relation_result_gdf = self.relation_result_gdf[['name_left', 'name_right']].reset_index(drop=True)

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM region WHERE name = '{self.relation_result_gdf['name_left'][idx]}' and type='commune') TO(SELECT FROM region WHERE name='{self.relation_result_gdf['name_right'][idx]}' and type='commune');"
        return query


class TreesNearTreesLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="neighbour"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.tree_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.tree_df['geometry'] = self.tree_df['wkt'].apply(wkt.loads)
        self.tree_gdf = gpd.GeoDataFrame(self.tree_df, geometry='geometry', crs='EPSG:4326')
        self.tree_gdf = self.tree_gdf.to_crs("EPSG:3857")
        self.coords = self.tree_gdf.geometry.apply(lambda geom: (geom.x, geom.y)).to_list()
        self.tree = cKDTree(self.coords)
        self.pairs = self.tree.query_pairs(r=50, output_type='ndarray')
        self.relation_result_gdf = gpd.GeoDataFrame(
            {'id1': np.array((self.tree_gdf.iloc[self.pairs[:, 0]]['id'])),
             'id2': np.array((self.tree_gdf.iloc[self.pairs[:, 1]]['id'])),
             'geo1': np.array((self.tree_gdf.iloc[self.pairs[:, 0]]['geometry'])),
             'geo2': np.array((self.tree_gdf.iloc[self.pairs[:, 1]]['geometry']))})
        self.relation_result_gdf['distance'] = self.relation_result_gdf['geo1'].distance(self.relation_result_gdf['geo2'])

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM tree WHERE id = '{self.relation_result_gdf['id1'][idx]}' and type='tree') TO(SELECT FROM tree WHERE id='{self.relation_result_gdf['id2'][idx]}' and type='tree');"
        return query


class TreesNearRoadLoader(RelationLoader):
    def __init__(self, filenames, relation_name, relation_type="near"):
        super().__init__(filenames, relation_name=relation_name, relation_type=relation_type)
        self.tree_df = pd.read_csv(filenames[0], encoding="utf-8")
        self.road_df = pd.read_csv(filenames[1], encoding="utf-8")
        self.tree_df['geometry'] = self.tree_df['wkt'].apply(wkt.loads)
        self.road_df['geometry'] = self.road_df['wkt'].apply(wkt.loads)
        self.tree_gdf = gpd.GeoDataFrame(self.tree_df, geometry='geometry', crs='EPSG:4326')
        self.road_gdf = gpd.GeoDataFrame(self.road_df, geometry='geometry', crs='EPSG:4326')
        self.tree_gdf = self.tree_gdf.to_crs("EPSG:3857")
        self.road_gdf = self.road_gdf.to_crs("EPSG:3857")
        self.road_gdf['buffer'] = self.road_gdf.geometry.buffer(20)
        self.tree_gdf = self.tree_gdf.set_geometry('geometry')
        self.road_gdf = self.road_gdf.set_geometry('buffer')
        self.relation_result_gdf = gpd.sjoin(self.tree_gdf, self.road_gdf[['id', 'buffer']], how='inner',
                                             predicate='within')

    def get_query(self, idx):
        query = f"CREATE EDGE {self.relation_type} FROM (SELECT FROM road WHERE id = '{self.relation_result_gdf['id_left'][idx]}' and type='road') TO(SELECT FROM tree WHERE id='{self.relation_result_gdf['id_right'][idx]}' and type='tree');"
        return query


