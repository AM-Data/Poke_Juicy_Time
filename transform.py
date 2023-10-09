import xml.etree.ElementTree as ET
import pandas as pd

import load



class PokerDataParser:
    def __init__(self, handhistory):
        try:
            self.hand_history = handhistory
            self.root = ET.fromstring(handhistory)
            self.general_info = self.root.find('general')
            self.game_info = self.root.find('game')
        except Exception as e:
            print(f"""Error by loading the handhistory with ET.fromstring():
            Handhistory: {handhistory}
            Error: {e}
        """)

    def parse_hand_information(self):
        data = {
            'hand_id': self.game_info.attrib["gamecode"],
            'start_date': self.game_info.find('general/startdate').text,
            'mode': self.general_info.find('mode').text,
            'game_type': self.general_info.find('gametype').text,
            'table_name': self.general_info.find('tablename').text,
            'table_currency': self.general_info.find('tablecurrency').text,
            'small_blind': self.general_info.find('smallblind').text.replace('€', ""),
            'big_blind': self.general_info.find('bigblind').text.replace('€', ""),
            'n_active_players': len(self.root.find('.//players').findall('player'))
        }
        return data

    def parse_date_of_each_player(self):
        # extract player, start_date, hand_id, limit
        player_list = []
        hand_id = self.game_info.attrib["gamecode"]
        start_date = self.game_info.find('general/startdate').text
        big_blind = self.general_info.find('bigblind').text.replace('€', "")
        for player in self.root.findall('.//players/player'):
            player_list.append({
                'player_name': player.attrib['name'],
                'hand_id': hand_id,
                'start_date': start_date,
                'big_blind': big_blind,
                'n_active_players': len(self.root.find('.//players').findall('player'))
            })
        return player_list
