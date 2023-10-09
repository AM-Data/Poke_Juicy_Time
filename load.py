import pandas as pd
from sqlalchemy import create_engine, text, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import time
from my_logger import CustomLogger

logger = CustomLogger(__name__).get_logger()

##### DATABASE TABLES #####
Base = declarative_base()
class PlayerStats(Base):
    __tablename__ = 'player_stats'

    player = Column(String(50), primary_key=True)
    hands = Column(Integer)
    vpip = Column(Integer)
    pfr = Column(Integer)
    vpip_pfr_gap = Column(Integer)
    wwsf = Column(Integer)
    cohort = Column(String(50))


class PlayerTimestamps(Base):
    __tablename__ = 'player_timestamps'

    id = Column(Integer, primary_key=True, autoincrement=True)
    player = Column(String(50))
    hand_id = Column(Integer)
    start_date = Column(DateTime)
    big_blind = Column(String(50))
    n_active_players = Column(Integer)


class HandHistory(Base):
    __tablename__ = 'hand_history'

    hand_id = Column(Integer, primary_key=True)
    hand_history = Column(Text)


class HandInfo(Base):
    __tablename__ = 'hand_info'

    hand_id = Column(Integer, primary_key=True)
    start_date = Column(DateTime)
    game_type = Column(String(50))
    mode = Column(String(50))
    table_name = Column(String(50))
    table_currency = Column(String(50))
    small_blind = Column(String(50))
    big_blind = Column(String(50))
    n_active_players = Column(Integer)

###### DATABASE HANDLING #####
class DataBaseManagement():
    def __init__(self, url, create_all_tables=False):
        self.url = url
        self.engine = create_engine(url)
        self.SessionLocal = sessionmaker(bind=self.engine)

        if create_all_tables:
            self.create_all_tables()

    def create_all_tables(self):
        Base.metadata.create_all(self.engine, checkfirst=True)

    def mysql_query(self, query):
        session = self.SessionLocal()
        try:
            logger.info('Trying to create the table')
            session.execute(text(query))
            logger.info('mysql_query sent')
        except:
            logger.error(f'Query cannot be sent: {query}')
        finally:
            logger.info('Closing sql session')
            session.close()

    def my_sql_select_query(self, query, single_value=False):
        session = self.SessionLocal()
        try:
            result = session.execute(text(query))
            if single_value:
                return round(float(result.scalar()), 3)
            else:
                return result.fetchall()
        except Exception as e:
            print(f"An error occured in my_sql_select_query returning 0: {e}")
            return 0
        finally:
            session.close()


    def get_table_as_df(self, query, index_col=None):
        try:
            df = pd.read_sql(query, self.engine, index_col=index_col)
            return df
        except Exception as e:
            logger.error("Cannot get the table as dataframe. Error: ", e)

    def truncate_table(self, table_name):
        query = f"""TRUNCATE TABLE {table_name};"""
        self.mysql_query(query)

    def load_table_player_timestamps(self, df, index=False, if_exists='append'):
        # check if table exists
        table_name = 'player_timestamps'
        try:
            self.send_df_to_table(df=df, table=table_name, index=index, if_exists=if_exists)
        except Exception as e:
            print(f"Failed to load the {table_name}. \n Error: {e}")

    def load_hand_history_table(self, df, index=False, if_exists='append'):
        table_name = 'hand_history'
        print(f'Len of {table_name} table: {len(df)}')
        try:
            start_time = time.time()
            self.send_df_to_table(df, table_name, index=index, if_exists=if_exists, chunks=True)
            end_time = time.time()
            print(f'Duration of loading {table_name}: {end_time - start_time}')
        except Exception as e:
            print(f"Failed to load {table_name} table to the database. \n Error: {e}")

    def load_hand_info_table(self, df, index=False, if_exists='append'):
        table_name = 'hand_info'
        print(f'Len of {table_name} table: {len(df)}')
        try:
            start_time = time.time()
            self.send_df_to_table(df, table_name, index=index, if_exists=if_exists)
            end_time = time.time()
            print(f'Duration of loading {table_name}: {end_time - start_time}')
        except Exception as e:
            print(f"Failed to load the player_timestamp table to the database. \n Error: {e}")

    def load_stats_table(self, df, index=False, if_exists='replace'):
        table_name = 'player_stats'
        try:
            self.send_df_to_table(df, table_name, index=index, if_exists=if_exists)
        except Exception as e:
            logger.error(f"Failed to load the {table_name} to the database. \n Error: {e}")

    def send_df_to_table(self, df, table, index=False, if_exists='append', chunks=False, chunk_size=4500):
        session = self.SessionLocal()
        try:
            # sending only 5000 rows at once
            # if the table is too big to send
            if chunks:
                chunk_size = chunk_size
                num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
                for i in range(num_chunks):
                    start_idx = i * chunk_size
                    end_idx = start_idx + chunk_size
                    chunk = df.iloc[start_idx:end_idx]
                    chunk.to_sql(table, self.engine, index=index, if_exists=if_exists)
            else:
                df.to_sql(table, self.engine, index=index, if_exists=if_exists)
        except Exception as e:
            logger.error(f'Cannot load the table to the database. Error: {e}')
        finally:
            session.close()
