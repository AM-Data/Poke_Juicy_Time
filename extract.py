import os
import pandas as pd
from tqdm import tqdm
import transform
import load
import xml.etree.ElementTree as ET
import time
DATABASE_URL = 'mysql+mysqlconnector://root:BCfxCk0UyMpi5iJLp6tq@localhost/jucy_poker'

class ExtractHandhistories():
    def __init__(self):
        # tables
        self.hand_history = []
        self.hand_info = []
        self.player_timestamps = []
        self.DB_connection = load.DataBaseManagement(DATABASE_URL)


    def open_xml_file(self, path):
        with open(path, 'r', encoding='UTF-8') as xml_file:
            return xml_file.read()

    def get_each_hand(self, file) -> list:
        hands = []
        xml_chunks = file.split('</root>')
        for chunk in xml_chunks:
            if chunk == "":
                continue
            # filter out the tournament hands, we don't need them yet
            if 'tournamentcurrency' in chunk:
                continue
            if not chunk.strip():  # if there are no characters in it
                continue
            chunk += '</root>'
            hands.append(chunk)
        return hands

    def load_handhistory(self, path):
        try:
            return self.get_each_hand(self.open_xml_file(path))
        except Exception as e:
            raise ValueError(f"Not a valid path: {path}")

    def extract_folders(self, path):
        last_subdir_paths = []
        existing_hand_id = []
        duplicates = 0

        # load existing hand_id from database to avoid duplicates
        try:
            existing_hand_id = self.DB_connection.get_table_as_df('SELECT hand_id FROM hand_info')
            existing_hand_id = existing_hand_id['hand_id'].to_list()
        except:
            print('No hand_info to load existing hand_ids.')

        for root, dirs, files in os.walk(path):
            if not dirs:
                last_subdir_paths.append(root)
        # get the list of filenames in the directory
        for path in tqdm(last_subdir_paths, desc='Loading files', ascii=False):
            filenames = os.listdir(path)
            file_path = None
            try:
                for filename in filenames:
                    file_path = os.path.join(path, filename)
                    hands = self.load_handhistory(file_path)

                    # extract data
                    for hand in hands:
                        root = ET.fromstring(hand)
                        hand_id = int(root.find('.//game').attrib['gamecode'])
                        if existing_hand_id:
                            if hand_id in existing_hand_id:
                                duplicates += 1
                                continue
                        try:
                            self.hand_history.append({'hand_id': hand_id, 'hand_history': hand})
                            data = transform.PokerDataParser(hand)
                            self.hand_info.append(data.parse_hand_information())
                            self.player_timestamps.extend(data.parse_date_of_each_player())
                        except Exception as e:
                            print(f'Error by parsing the hand. \n Hand:{hand} \n Exception: {e}')
            except Exception as e:
                print(f'Error by loading the folder: {e}, \n Path: {path}, \n Filepaths: {file_path}, \n Filenames: {filenames} \n')
        print(f"Total hands: {len(self.hand_info)}")
        print(f'Total duplicates: {duplicates}')

        # load to database
        self.DB_connection.load_table_player_timestamps(pd.DataFrame(self.player_timestamps))
        self.DB_connection.load_hand_history_table(pd.DataFrame(self.hand_history))
        self.DB_connection.load_hand_info_table(pd.DataFrame(self.hand_info))



