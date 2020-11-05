import numpy as np
import pandas as pd
from py2neo import Graph
from connector import connection
from tqdm import tqdm
import math
import configparser

class DataTransfer:

    def __init__(self):
        self.driver = connection()
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")


    def createUnique(self, data, toCopy):

        for i in data:
            vals = i.split(",")

            for v in vals:
                v = v.strip()
                v = v.replace("\"", "")
                v = v.replace("#", "")
                toCopy.add(v)

        return toCopy


    def prepareData(self, data, *args, fieldName = None):
        
        ret = list()
        properties = list()

        if len(args)!=0:
            
            for a in args:
                if type(a) == str:
                    properties.append(a)


        with tqdm(total = len(data)) as pbar:
            for i, val in enumerate(data):
                val = val.strip()
                val = val.replace("\"", "")
                val = val.replace("#", "")

                _temp = dict()
                for p in properties:
                    if p == "name":
                        _temp[p] = val
                    
                    if p == "id":
                        _temp[p] = i

                    if p == "type":
                        showType = args[-1].loc[args[-1][fieldName] == val, p].tolist()

                        if len(showType) != 0:
                            showType = showType[0]
                            _temp[p] = showType
                        
                        else:
                            _temp[p] = ''

                    if p == "rating":
                        rating = args[-1].loc[args[-1][fieldName] == val, p].tolist()

                        if len(rating) != 0:
                            rating = rating[0]
                            _temp[p] = rating

                        else:
                            _temp[p] = ''

                
                ret.append(_temp)
                pbar.update(1)


        return ret


        
    def createNodes(self, data, label, create = False):
        
        if not create:
            return
        
        with tqdm(total=len(data)) as pbar:
            for i, data in enumerate(data):
                node_name = label[0] + str(i)
                keys = data.keys()

                _temp = 'SET '

                for k in keys:
                    if k == "name" : 
                        _temp += f' {node_name}.{k} = "{data[k]}" ,'
                    
                    if k == "type":
                        _temp += f' {node_name}.{k} = "{data[k]}" ,'

                    if k == "rating":
                        _temp += f' {node_name}.{k} = "{data[k]}" ,'

                    if k == "id":
                        _temp += f' {node_name}.{k} = {data[k]} ,'
                
                _temp = _temp.rstrip(",")

                # print(_temp)

                self.driver.run(f'CREATE ({node_name}: {label}) {_temp}')

                pbar.update(1)


    
    def createRelationship(self, dataframe, fieldName1, fieldName2, labelType1, labelType2, relationship, create=False, towards=True):

        if not create:
            return

        vals1 = dataframe[fieldName1].unique()
        
        with tqdm(total=len(vals1)) as pbar:
            for v in vals1:
                v = v.strip()
                v = v.replace("\"", "")
                v = v.replace("#", "")

                vals2 = dataframe.loc[dataframe[fieldName1] == v , fieldName2].unique()
                if len(vals2) == 0 or type(vals2[0]) == float:
                    continue

                vals2 = vals2[0]
                # print(vals2)
                vals2 = vals2.split(",")
                
                for _v in vals2:
                    _v = _v.strip()
                    _v = _v.replace("\"", "")
                    if towards:
                        _query = f'Match (a:{labelType1}), (b:{labelType2}) where a.name="{v}" and b.name="{_v}" Create (a)-[:{relationship}]->(b)'
                    else : 
                        _query = f'Match (a:{labelType1}), (b:{labelType2}) where a.name="{v}" and b.name="{_v}" Create (a)<-[:{relationship}]-(b)'
                    

                    self.driver.run(_query)

                
                pbar.update(1)



        




    def main(self):

        df = pd.read_csv("./DATA/netflix_titles.csv")
        df["date_added"] = pd.to_datetime(df["date_added"])
        df["year_added"] = df["date_added"].dt.year
        df["month_added"] = df["date_added"].dt.month

        _directors = df.loc[~df["director"].isna(), "director"].unique()
        
        directors = set()

        shows = df["title"].values

        _actors = df.loc[~df["cast"].isna(), "cast"].unique()
        actors = set()

        directors = self.createUnique(_directors, directors)
        actors = self.createUnique(_actors, actors)

        _country = df.loc[~df["country"].isna(), "country"].unique()
        country = set()
        country = self.createUnique(_country, country)

        nodeDirectors = self.prepareData(directors, "name", "id")
        
        nodeActors = self.prepareData(actors, "name", "id")

        nodeShows = self.prepareData(shows, "name", "id", "type", "rating", df, fieldName="title")

        nodeCountries = self.prepareData(country, "name", "id")



        # Create Nodes in GraphDB

        print("-"*20)
        print("CREATING NODES")
        createNodes = self.config["SETTINGS"]["CREATENODES"]

        if createNodes == "TRUE":
            createNodes = True
        else:
            createNodes = False

        self.createNodes(nodeDirectors, "Director", create=createNodes)
        self.createNodes(nodeActors, "Actor", create=createNodes)
        self.createNodes(nodeShows, "Shows", create=createNodes)
        self.createNodes(nodeCountries, "Country", create=createNodes)
        print("-"*20)



        # Create Relationships in GraphDB
        print("*"*20)
        print("CREATING REALTION")
        createRelationship = self.config["SETTINGS"]["CREATERELATIONSHIP"]
        
        if createRelationship == "TRUE":
            createRelationship = True
        else:
            createRelationship = False

        self.createRelationship(df, "title", "cast", "Shows", "Actor", "acted_in",create=createRelationship, towards=False)
        self.createRelationship(df, "title", "director", "Shows", "Director", "directed_by", create=createRelationship, towards=False)
        self.createRelationship(df, "title", "country", "Shows", "Country", "produced_in", create=createRelationship, towards=True)

        print("*"*20)



        





        






DataTransfer().main()