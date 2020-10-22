from tqdm import tqdm
import requests
import bz2
import datetime as dt
import io
import os
import re
import timeit
import urllib.request as urllib2
import collections
import numpy as np
import pandas as pd
import requests
import codecs

import pdb

import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark import SparkContext, SparkConf

class pgn_process:
    def __init__(self, file_url):

        file_basename = file_url.split(sep="/")[-1].split(sep='.pgn.bz2')[0]
        archive_date = file_basename.split(sep="rated_")[1]
        
        self.paths = dict()

        self.paths['ServerURL'] = file_url
        self.paths['LocalURL'] = "file://" + urllib2.quote(
            "./data/lichess_db_standard_rated_2013-06.pgn.bz2"
        )
        self.paths['DriveFolder'] = "./data/"
        self.paths['tmpFolder'] = "./tmp/"
        self.paths['FileCSV'] = file_basename + '.csv'
        self.paths['FileBZ2'] = file_basename + '.pgn.bz2'
        self.paths['ParquetPath'] = (
          self.paths['DriveFolder'] + archive_date + '/' + 'data.parquet/')
        self.tic = timeit.default_timer()
        self.toc = timeit.default_timer()
        self.chunk_size = 1024 * 1000 * 10
        self.data_rem = b""
        self.game_idx = 0
        self.decompressor = bz2.BZ2Decompressor()

        self.games_rem = ""
        self.games_list = list()
        self.chunk_num = 0
        self.first_save = True
        self.games_keys = [
            "White",
            "Black",
            "Result",
            "WhiteElo",
            "BlackElo",
            "ECO",
            "TimeControl",
            "Termination",
            "PlyCount",
            "TimeStamp",
        ]
        self.games_dict = collections.defaultdict(list)

        self.spark = SparkSession.builder.appName('pgnParser').getOrCreate()  


    def obtain(self):
        response = requests.get(self.paths['ServerURL'], stream=True)

        if os.path.isdir(self.paths['tmpFolder']) == False:
            os.mkdir(self.paths['tmpFolder'])

        with open(
          (self.paths['tmpFolder'] + self.paths['FileBZ2']), 
          "wb") as handle:
            for data in tqdm(response.iter_content()):
                handle.write(data)

    def process_archive(self):
        with urllib2.urlopen(self.paths["ServerURL"], timeout = 30) as f:
        # with urllib2.urlopen(self.localpath) as f:
            for chunk in iter(lambda: f.read(self.chunk_size), b""):

                self.chunk_num += 1

                # feed chunk to decompressor
                self.decompress(chunk)

                # handle case of concatenated bz2 streams
                while self.decompressor.eof:
                    remainder = self.decompressor.unused_data
                    self.decompressor = bz2.BZ2Decompressor()
                    self.decompress(remainder)

                if len(self.games_dict['White']) > 1e6:
                    self.toc = timeit.default_timer()
                    print('Processed {:d} games in {:.0f} s'.format(len(self.games_dict['White']), self.toc - self.tic))
                    self.tic = timeit.default_timer()
                    self.save_data()
                    self.games_dict = collections.defaultdict(list)

        self.save_data()

    def decodeBytesUtf8Safe(self, data):
        """
        decodes byte array in utf8 to string. It can handle case when end of byte array is
        not complete thus making utf8 error. in such case text is translated only up to error.
        Rest of byte array (from error to end) is returned as second parameter and can be
        combined with next byte array and decoded next time.
        :param toDec: bytes array to be decoded a(eg bytes("abc","utf8"))
        :return:
        1. decoded string
        2. rest of byte array which could not be encoded due to error
        """
        N_bytes = len(data)

        decoded = ""
        while(N_bytes>0):
            try:
                decoded = data[:N_bytes].decode("utf-8")
            except UnicodeDecodeError as ex:
                N_bytes -= 1
            else:
                break

        return decoded, data[N_bytes:]

    def decompress(self, chunk):
        """Decompress and process a piece of a compressed stream"""
        # print(self.decompressor.eof)
        data = self.data_rem + self.decompressor.decompress(chunk)
        if data != 0:  # 0 is common -- waiting for a bzip2 block
            # process dat here

            chunk_decoded, self.data_rem = self.decodeBytesUtf8Safe(data)
            try:
                ### A chunk might contain an incomplete game, which is difficult
                ### to produce later. Therefore incomplete strings are saved in
                ### remainder_string and concacated in the next run
                chunk_decoded = self.games_rem + chunk_decoded
            except:
                chunk_decoded = chunk_decoded

            ### Split chunks at the "Event" key as it is the indicator
            ### for a new game
            games = re.sub("\n\n\[Event", "\n\n;[Event", chunk_decoded).split("\n\n;")

            ### Extract the last chunk as it is typically incomplete and can be
            ### concacated in the next run. Additionally, it removes the last
            ### match from chunk and leaves chunk = [], in case no further data
            ### can be extracted from the archive.

            self.games_rem = games.pop()

            self.extract_keys(games)

    def extract_keys(self, data):
        ### Function extracts the features from the chunk

        ### Iterate over lines in the game and extract the features
        ### using RegEx
        for game in data[:]:

            game = game.split(sep="\n")
            game_dict = collections.defaultdict(list)

            for line in game:
                key = None
                val = None

                # matched = re.search('\[(\w+) "([\S\s]+)"\]', line.strip())
                matched = line.split(sep='\"')
                if len(matched) > 1:
                    # key = matched.group(1)
                    # val = matched.group(2)

                    key = matched[0][1:-1]
                    val = matched[1]

                    game_dict[key] = val

                elif len(line) > 1:
                    # matched = re.split("(\d+)[.]", line.strip())
                    matched = line.split(sep=' ')
                    for i in range(len(matched),0,-1):
                        try:
                            game_dict["PlyCount"] = int(float(matched[i]))
                            break
                        except:
                            game_dict["PlyCount"] = 0

            game_dict["Result"] = 1 if game_dict["Result"] == "1-0" else (-1 if game_dict["Result"] == "0-1" else 0)
            try:
                game_dict["WhiteElo"] = int(game_dict["WhiteElo"])
            except:
                game_dict["WhiteElo"] = (
                    1200
                    if game_dict["WhiteElo"][:-1:] == ""
                    else int(game_dict["WhiteElo"][:-1:])
                )
            try:
                game_dict["BlackElo"] = int(game_dict["BlackElo"])
            except:
                game_dict["BlackElo"] = (
                    1200
                    if game_dict["BlackElo"][:-1:] == ""
                    else int(game_dict["BlackElo"][:-1:])
                )

            game_dict["TimeControl"] = game_dict["TimeControl"].split(sep="+")
            ### TimeControl = fix + increment * 40 moves (average)
            try:
                game_dict["TimeControl"] = (
                    int(game_dict["TimeControl"][0])
                    + int(game_dict["TimeControl"][1]) * 40
                )
            except:
                ### Can be raised for games without TimeControl
                game_dict["TimeControl"] = np.nan

            game_dict["TimeStamp"] = game_dict["UTCDate"] + " " + game_dict["UTCTime"]

            if game_dict["Termination"] == "Time forfeit":
                game_dict["Termination"] = 1
            elif game_dict["Termination"] == "Abandoned":
                game_dict["Termination"] = 2
            elif game_dict["Termination"] == "Rules Infraction":
                game_dict["Termination"] = 3
            else:
                game_dict["Termination"] = 0

            ### Features are written in an array of lists, which can be
            ### easily transformed to DataFrames
            for key in self.games_keys:
                self.games_dict[key].append(game_dict[key])

            self.game_idx += 1

            ### Progress indication: Gives the number of games processed
            print(
                "Games processed: {:d}".format(self.game_idx)
            ) if self.game_idx % 1e5 == 0 else ...

    def save_data(self):

        if os.path.isdir(self.paths['tmpFolder']) == False:
            os.mkdir(self.paths['tmpFolder'])

        # Convert data to DataFrame and write it to file
        if self.first_save:
            if os.path.isfile(self.paths['tmpFolder'] + self.paths['FileCSV'] ):
                os.remove(self.paths['tmpFolder'] + self.paths['FileCSV'])

            df = pd.DataFrame(self.games_dict)

            df['TimeStamp'] = pd.to_datetime(
              df['TimeStamp'], 
              format='%Y.%m.%d %H:%M:%S')

            df = df[df['PlyCount'] > 8]

            df.to_csv(
                (self.paths['tmpFolder'] + self.paths['FileCSV']),
                mode="w",
                index=None,
            )

            self.first_save = False
        else:
            if len(self.games_dict) == 0:
                return
                
            df = pd.DataFrame(self.games_dict)

            df['TimeStamp'] = pd.to_datetime(
              df['TimeStamp'], 
              format='%Y.%m.%d %H:%M:%S')

            df = df[df['PlyCount'] > 8]
            
            df.to_csv(
                (self.paths['tmpFolder'] + self.paths['FileCSV']), 
                mode="a", header=False, index=None
            )

    def csv2parquet(self):
        schema = StructType() \
            .add("White",StringType(),True) \
            .add("Black",StringType(),True) \
            .add("Result",FloatType(),True) \
            .add("WhiteElo",IntegerType(),True) \
            .add("BlackElo",FloatType(),True) \
            .add("ECO",StringType(),True) \
            .add("TimeControl",FloatType(),True) \
            .add("Termination",StringType(),True) \
            .add("PlyCount",FloatType(),True) \
            .add("TimeStamp",StringType(),True) \


        sdf = self.spark.read.schema(schema) \
                      .options(delimiter=',') \
                      .options(header='True') \
                      .csv(self.paths['tmpFolder'] + self.paths['FileCSV'])

        sdf.write.parquet(self.paths['ParquetPath'], mode='overwrite')

        return sdf

    def delete_csv(self):
        os.remove(self.paths['tmpFolder'] + self.paths['FileCSV'])
        
