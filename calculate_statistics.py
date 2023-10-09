import pandas as pd
from pandas import DataFrame
import xml.etree.ElementTree as ET
from my_logger import CustomLogger
from poker_metrics import euro_to_float
import load



logger = CustomLogger(__name__).get_logger()

player_cohorts = {
            "fish_passiv": [("hands", ">=", 100), ("vpip", ">=", 40), ("wwsf", "<", 45)],
            "fish_aggro": [("hands", ">=", 100), ("vpip", ">=", 50), ("wwsf", ">=", 45)],
            "reg_passiv": [("hands", ">=", 500), ("vpip", "<=", 33), ("vpip_pfr_gap", "<=", 9), ("wwsf", "<", 43)],
            "reg_medium": [("hands", ">=", 500), ("vpip", "<=", 33), ("vpip_pfr_gap", "<=", 9), ("wwsf", "<", 47),
                           ("wwsf", ">=", 43)],
            "reg_aggro": [("hands", ">=", 500), ("vpip", "<=", 37), ("vpip_pfr_gap", "<=", 9), ("wwsf", ">=", 47)],
            "semireg_passiv": [("hands", ">=", 240), ("vpip", "<", 40), ("vpip_pfr_gap", ">=", 10), ("wwsf", "<", 45)],
            "semireg_aggro": [("hands", ">=", 200), ("vpip", "<", 50), ("vpip_pfr_gap", ">=", 10), ("wwsf", ">=", 45)],
            "fish_low_sample": [("hands", "<", 100), ("hands", ">", 15), ("vpip_pfr_gap", ">=", 15)],
            "reg_low_sample": [("hands", "<", 100), ("hands", ">", 15), ("vpip", "<=", 32), ("vpip_pfr_gap", "<=", 9)]
        }
def classify_player_type(row):
    for cohort, conditions in player_cohorts.items():
        matches_all_conditions = True
        for col, cond, value in conditions:
            if cond == '>=' and not row[col] >= value:
                matches_all_conditions = False
                break
            elif cond == '<=' and not row[col] <= value:
                matches_all_conditions = False
                break
            elif cond == '>' and not row[col] > value:
                matches_all_conditions = False
                break
            elif cond == '<' and not row[col] < value:
                matches_all_conditions = False
                break
            elif cond == '==' and not row[col] == value:
                matches_all_conditions = False
                break
        if matches_all_conditions:
            return cohort
    return "unknown"


class PokerStats:
    """Class to retrieve the stats about a player"""
    stats_table: DataFrame

    def __init__(self, db_connection,
                 min_bigblind=0,
                 max_bigblind=1000,
                 min_active_players=3,
                 max_active_players=10,
                 start_date=None,
                 end_date=None):
        self.min_bigblind = min_bigblind
        self.max_bigblind = max_bigblind
        self.min_active_players = min_active_players
        self.max_active_players = max_active_players
        self.start_date = start_date
        self.end_date = end_date

        self.DB_connection = db_connection

    # _____________________calc stats from xml files____________________________
    def calc_wwsf_from_hand(self, root):
        players_on_flop = set()

        # players who saw flop
        for action in root.findall(".//round[@no='2']/action"):
            players_on_flop.add(action.attrib['player'])

        wwsf = []
        # check if the player won who were on the flop
        for player in root.findall('.//players/player'):
            player_name = player.attrib['name']
            if player_name not in players_on_flop:
                continue
            amount_won = euro_to_float(player.attrib['win'])
            wwsf.append({
                'player': player_name,
                'saw_flop': True,
                'won_hand': amount_won > 0
            })
        return wwsf

    def calc_vpip_pfr_from_hand(self, root):
        stats = []
        players_first_action = set()
        round_1_actions = root.findall(".//round[@no='1']/action")

        for action in round_1_actions:
            action_type = int(action.get('type'))
            player_name = action.get('player')

            if player_name in players_first_action:
                continue
            players_first_action.add(player_name)

            flag_vpip = action_type in [3, 4, 5, 23]
            flag_pfr = action_type in [5, 23]

            stats.append({
                'player': player_name,
                'flag_vpip': flag_vpip,
                'flag_pfr': flag_pfr
            })
        return stats

    def parse_hand_history(self, hand_histories):
        vpip_raw = []
        wwsf_raw = []

        for hand in hand_histories:
            root = ET.fromstring(hand)

            # getting the raw stats:
            vpip_raw.extend(self.calc_vpip_pfr_from_hand(root))
            wwsf_raw.extend(self.calc_wwsf_from_hand(root))

        n_hands = len(hand_histories)
        logger.info(f"Calculated Stats for {n_hands} hands.")

        return vpip_raw, wwsf_raw

    def process_raw_data(self, vpip_raw, wwsf_raw):
        # process VPIP stats
        vpip = pd.DataFrame(vpip_raw)
        vpip = vpip.groupby('player').agg(
            hands=('player', 'count'),
            vpip=('flag_vpip', 'sum'),
            pfr=('flag_pfr', 'sum')
        )
        vpip['vpip'] = vpip['vpip'] / vpip['hands'] * 100
        vpip['pfr'] = vpip['pfr'] / vpip['hands'] * 100
        vpip['vpip_pfr_gap'] = vpip['vpip'] - vpip['pfr']

        # process WWSF stats
        wwsf = pd.DataFrame(wwsf_raw).groupby('player').agg(
            saw_flop=('saw_flop', 'sum'),
            won_hand=('won_hand', 'sum')
        )
        wwsf['wwsf'] = wwsf['won_hand'] / wwsf['saw_flop'] * 100

        # merge tables
        final_player_stats = vpip.reset_index().merge(wwsf.reset_index()[['player', 'wwsf']], on='player', how='left').fillna(0)

        numeric_cols = final_player_stats.select_dtypes(exclude='object').columns
        final_player_stats[numeric_cols] = final_player_stats[numeric_cols].round(0).astype(int)
        logger.info('Add cohort')
        final_player_stats['cohort'] = final_player_stats.apply(classify_player_type, axis=1)
        logger.info('Finish Add cohort')

        return final_player_stats

    def classify_player_type(self):
        return

    def update_stats_from_xml(self):
        hand_histories = self.DB_connection.get_table_as_df(f"""SELECT * FROM hand_history""")
        hand_histories = hand_histories['hand_history'].tolist()

        logger.info('Calculate stats')
        vpip_raw, wwsf_raw = self.parse_hand_history(hand_histories)
        logger.info('Finish  parse_hand_history')
        final_player_stats = self.process_raw_data(vpip_raw, wwsf_raw)
        logger.info('Finish calculate stats')

        self.DB_connection.load_stats_table(final_player_stats)