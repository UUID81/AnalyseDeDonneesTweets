from asyncio import open_connection
import mysql.connector as msql
import pandas as pd
import csv

class Connexion:
    # Variables de classe
    # --------------------------------------
    __USER = "root"
    __PWD = "root"
    __HOST = "localhost"
    __PORT = "8081"
    __DB = "tweet_fr_ukraine"
    __cursor = None
    __path1 = "table_user.csv"
    __path2 = "table_tweet.csv"

    # Méthodes de classe
    # -------------------------------------
    # OUVERTURE ET FERMETURE
    # -------------------------------------
    @classmethod
    def open_connexion(cls):
        if cls.__cursor == None:
            cls.__bdd = msql.connect(
                user=cls.__USER,
                password=cls.__PWD,
                host=cls.__HOST,
                port=cls.__PORT,
                database=cls.__DB,
            )
            cls.__cursor = cls.__bdd.cursor()

    @classmethod
    def close_connexion(cls):
        cls.__cursor.close()
        cls.__bdd.close()
        cls.__cursor = None

    # ----------------------------------------------
    # CREATION DES TABLES ET CHARGEMENT DES DONNEES
    # ----------------------------------------------
    @classmethod
    def create_table(cls):
        """Méthode qui crée les tables users et tweets dans la BDD"""
        cls.open_connexion()
        query1 = "CREATE TABLE if not exists users(userid INT, username VARCHAR(40), following INT, followers INT, PRIMARY KEY(userid))ENGINE=INNODB ;"
        query2 = "CREATE TABLE if not exists tweets(tweetid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT, hashtags VARCHAR(50) DEFAULT 'NONE', retweetcount INT, year INT, month INT, day INT, hour INT, minute INT, userid INT, PRIMARY KEY(tweetid), FOREIGN KEY(userid) REFERENCES users(userid))ENGINE=INNODB ;"

        cls.__cursor.execute(query1)
        cls.__cursor.execute(query2)
        cls.__bdd.commit()
        cls.close_connexion()

    @classmethod
    def load_infile_table(cls):
        """Méthode qui charge les données dans les tables users et tweets"""
        # Ouverture des fichiers .csv et création des dataframes pour alimenter les tables dans MySql
        df_users = pd.read_csv(cls.__path1)
        df_tweets = pd.read_csv(cls.__path2)

        # Ouverture connexion
        cls.open_connexion()

        # Ajout des lignes du dataframe df_users dans la table users
        for index, ligne in df_users.iterrows():
            query1 = f"INSERT INTO users (userid, username, following, followers) VALUES ({ligne['user_id']}, '{ligne['username']}', {ligne['following']}, {ligne['followers']});"
            # print(query1)
            cls.__cursor.execute(query1)
        cls.__bdd.commit()

        # Ajout des lignes du dataframe df_tweets dans la table tweets
        for index, ligne in df_tweets.iterrows():
            query2 = f"INSERT INTO tweets (hashtags, retweetcount, year, month, day, hour, minute, userid) VALUES ('{ligne['hashtags']}', {ligne['retweetcount']}, {ligne['year']}, {ligne['month']}, {ligne['day']}, {ligne['hour']}, {ligne['minute']}, {ligne['user_id']});"
            # print(query2)
            try:
                cls.__cursor.execute(query2)
            except:
                pass
        cls.__bdd.commit()

        cls.close_connexion()

    # -------------------------------------
    # VUE ET PROCEDURE
    # -------------------------------------
    # Procédure :
    @classmethod
    def procedure(cls, hashtag):
        """Génère une procédure stockée qui calcule le nombre de hashtags produits par heure et par minute"""

        cls.open_connexion()

        query_procedure = "CREATE PROCEDURE nb_hashtags_hour (IN hashtag VARCHAR(50)) BEGIN SELECT tweets.day, tweets.hour, COUNT(tweets.hashtags) AS 'Nb_hashtags' FROM tweets WHERE tweets.hashtags = hashtag GROUP BY tweets.day, tweets.hour ORDER BY tweets.day; END ;"
        # query_ok = "CREATE PROCEDURE nb_hashtags_hour_test (IN hashtag VARCHAR(50)) BEGIN SELECT tweets.day, tweets.hour, COUNT(tweets.hashtags) AS 'Nb_hashtags' FROM tweets WHERE tweets.hashtags = hashtag GROUP BY tweets.day, tweets.hour ORDER BY tweets.day; END"
        cls.__cursor.execute(query_procedure)

        query_call = f"CALL nb_hashtags_hour('{hashtag}');"
        result = cls.__cursor.execute(query_call)
        print(result)
        cls.close_connexion()

        # return result

    # View :


# Connexion.create_table()pypiip
Connexion.load_infile_table()