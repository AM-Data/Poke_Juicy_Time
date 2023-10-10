import plotly.graph_objects as go
import plotly.express as px
import load
import pandas as pd
import xml.etree.ElementTree as ET
import logging
import calendar
from my_logger import CustomLogger

pd.set_option('display.max_columns', None)

logger = CustomLogger(__name__).get_logger()


def euro_to_float(euro_string):
    if "€" in euro_string:
        return float(euro_string[1:].replace(".", "").replace(",", "."))
    else:
        return float(euro_string.replace(".", "").replace(",", "."))


class PokerMetrics:
    def __init__(self, db_connection, min_bigblind=0, max_bigblind=10000, min_active_players=3, max_active_players=6,
                 filter_table_name=None):
        self.min_bigblind = min_bigblind
        self.max_bigblind = max_bigblind
        self.min_active_players = min_active_players
        self.max_active_players = max_active_players
        self.filter_table_name = filter_table_name

        # Database connection for querying
        self.DB_CONNECTION = db_connection

        self.unknown_players = ["Player 1", "Player 2", "Player 3", "Player 4", "Player 5", "Player 6", "Player 7",
                                "Player 8", "Player 9", "Player 10"]
        self.hero = ["HulluPorro", "lllllllIlllllllI", "IIlllIlIIlllllII", "IiIllIlliliilliI", "lililillliillill"]
        # self.hero = ["GLADIATOREEEEEE"]

        self.analyse_hand_count = 0

    def load_player_data_from_DB(self, return_only_columns=None, exclude_hero=True):
        query = '''
        SELECT start_date,
            pt.player,
            hand_id,
            big_blind,
            n_active_players,
            cohort
        FROM player_timestamps AS pt LEFT JOIN player_stats as ps ON pt.player = ps.player
        '''
        logger.info('Querying the df')
        df = self.DB_CONNECTION.get_table_as_df(query, index_col='start_date')
        logger.info('Finish Querying the df')

        # filter the dataframe for the parameter
        excluding_players = self.unknown_players + (self.hero if exclude_hero else [])
        mask_exclude_regs_hu = (df['cohort'].isin(['reg_passiv', 'reg_medium', 'reg_aggro']))
        df = df[
            (~df['player'].isin(excluding_players)) &
            (df['big_blind'].apply(euro_to_float) >= self.min_bigblind) &
            (df['big_blind'].apply(euro_to_float) <= self.max_bigblind) &
            (~mask_exclude_regs_hu | (df['n_active_players'] >= self.min_active_players) & mask_exclude_regs_hu)
            ]

        if return_only_columns:
            return df[return_only_columns]

        return df


    def generate_SQL_query_from_criteria(self, criteria):
        where_conditions = []
        for column, operator, value in criteria:
            if column in ['n_active_players']:
                prefix = 'pt.'
            else:
                prefix = 'ps.'
            where_conditions.append(f"{prefix}{column} {operator} {value}")
        where_clause = " AND ".join(where_conditions)

        return f"SELECT pt.* FROM player_timestamps AS pt JOIN player_stats AS ps ON pt.player = ps.player " \
               f"WHERE {where_clause}"

    def grouping_date_index_data(self, df, time_unit='day'):
        # Kann man verbessern, wir haben bereits einen Dataframe mit big_winner und loooser gemeinsam und nichtmehr getrennt
        # Die Berechnungen kann man dann auch mit der neuen Spalte ['winner_loser] durchführen und
        # man muss nichtmehr getrennt die zwei DFs hier durchjagen.

        time_units = {'day': df.index.day, 'hour': df.index.hour, 'weekday': df.index.weekday,
                      'weekday_hour': [df.index.weekday, df.index.hour], 'monthly_hour': None}

        df = df.copy()
        if time_unit not in time_units:
            raise ValueError(
                f"{time_unit} is not a valid parameter for timeunit. \n Valid time units: {time_units.keys()}")

        if not df.index.dtype == 'datetime64[ns]':
            logger.warning("index is not datetime, set index to datetime...")
            df['start_date'] = pd.to_datetime(df['start_date'])
            df.set_index('start_date', inplace=True)

        # adding time unit columns
        df['weekday'] = df.index.day_name()
        df['day'] = df.index.day
        df['hour'] = df.index.hour
        df['month'] = df.index.month
        df['year'] = df.index.year

        df_ret = None

        if time_unit == 'monthly_hour':
            df_test = df.copy()

            # Function to calculate week number from a given date and the first Friday
            def calculate_week_number(row):
                first_friday_of_month = self._first_friday(row['year'], row['month'])
                if row['day'] < first_friday_of_month:
                    return 0  # This will be filtered out later
                day_order = row['day'] - first_friday_of_month + 1
                return (day_order - 1) // 7 + 1  # Week number starts from 1

            df_test['week_number'] = df_test.apply(calculate_week_number, axis=1)

            # Filter out the rows where the week_number is 0
            # df_test = df_test[df_test['week_number'] > 0]

            # Group by week number, weekday, and hour, then count the number of occurrences
            grouped_df = df_test.groupby(['week_number', 'weekday', 'hour']).agg(
                n_big_looser=('winner_looser', lambda x: (x == 'big_looser').sum()),
                n_big_winner=('winner_looser', lambda x: (x == 'big_winner').sum())
            ).reset_index()

            # Create a new column combining week number and weekday for the heatmap y-axis
            grouped_df['week_weekday'] = grouped_df['week_number'].astype(str) + ' ' + grouped_df['weekday']

            # Order the df for first Weekday to last weekday for later to plot the heatmap with an order
            ordered_weekdays = ["Friday", "Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
            grouped_df['weekday'] = pd.Categorical(grouped_df['weekday'], categories=ordered_weekdays, ordered=True)

            grouped_df = grouped_df.sort_values(by=['week_number', 'weekday'])  # order by weeknumber and days
            # grouped_df = grouped_df.sort_values(by=['weekday', 'week_number'])  # ordered by all weekdays and number

            return grouped_df.set_index(['week_weekday', 'hour'])[['n_big_looser', 'n_big_winner']]

        # grouping the data by time unit
        df_ret = df.groupby(time_units[time_unit]).agg(
            n_big_looser=('winner_looser', lambda x: (x == 'big_looser').sum()),
            n_big_winner=('winner_looser', lambda x: (x == 'big_winner').sum()),
        )

        return df_ret

    def _get_big_looser_big_winner(self, time_unit):
        # define big_looser and winner
        big_looser = ['fish_low_sample', 'fish_passiv', 'semireg_passiv', 'fish_aggro']  # 'reg_passiv', 'reg_medium'   - Winrate ist ca. 2bb/100
        big_winner = ['semireg_aggro', 'reg_aggro']

        # assign big_looser/big_winner to table
        df_cohort_timestamps = self.load_player_data_from_DB()
        df_cohort_timestamps['winner_looser'] = df_cohort_timestamps['cohort'].apply(
            lambda x: 'big_looser' if x in big_looser
            else ('big_winner' if x in big_winner else 'other'))


        # try new df
        df_new = self.grouping_date_index_data(df_cohort_timestamps, time_unit=time_unit)

        df = df_new

        df['sum_observations'] = df['n_big_looser'] + df['n_big_winner']
        df['ratio'] = df['n_big_looser'] / df['n_big_winner']  # the higher the ratio, the better

        return df

    def _week_and_weekday(self, date, first_friday_of_month):
        day = date.day
        day_order = day - first_friday_of_month + 1

        # Calculate week number and weekday
        week_number = (day_order - 1) // 7 + 1
        weekdays = ["Fri", "Sat", "Sun", "Mon", "Tue", "Wed", "Thu"]
        weekday = weekdays[(day_order - 1) % 7]
        return f"{week_number} {weekday}"

    def _first_friday(self, year, month):
        first_day_of_month = calendar.weekday(year, month, 1)  # 0 is Monday, 1 is Tuesday, ..., 6 is Sunday
        return (4 - first_day_of_month + 7) % 7 + 1  # 4 is Friday



    def plot_big_looser_big_winner_ratio(self, time_unit='day'):
        ####!!!!! in progress
        logger.info('Create big looser, big winner dataframe')
        df = self._get_big_looser_big_winner(time_unit=time_unit)
        logger.info('Finished: Create big looser, big winner dataframe')

        # plot the data
        fig = go.Figure()
        if time_unit == 'weekday_hour' or time_unit == 'monthly_hour':
            df = df.rename_axis(['weekday', 'hours'])
            heatmap_data = df['ratio'].reset_index().pivot(columns='hours', index='weekday', values='ratio').fillna(
                0).round(2)

            # reindex the mothly_hour
            preferred_index = df.reset_index()['weekday'].unique()
            if time_unit == 'monthly_hour':
                heatmap_data = heatmap_data.reindex(preferred_index)

            fig = px.imshow(
                heatmap_data,
                title=f'<b>Ratio of big winners and loosers by weekday and hour of the day</b> <br> Limit: > {self.min_bigblind}',
                color_continuous_scale='rdylgn',
                range_color=[0.2, 1.6]
            )

            # adding custom data to be show in the popup
            custom_data = df['sum_observations'].reset_index().pivot(columns='hours', index='weekday',
                                                                     values='sum_observations').fillna(0).round(2)
            if time_unit == 'monthly_hour':
                custom_data = custom_data.reindex(preferred_index)

            fig.update_traces(customdata=custom_data.values,
                              hovertemplate="Hour: %{x}<br>Weekday: %{y}<br>Ratio: %{z}<br>Observations: %{customdata}")

            if time_unit == 'weekday_hour':
                logger.info('Update the layout')
                fig.update_layout(
                    yaxis=dict(tickvals=[0, 1, 2, 3, 4, 5, 6],
                               ticktext=['Montag', 'Dienstag', 'Mittwoch', 'Donnerstag', 'Freitag', 'Samstag',
                                         'Sonntag'])
                )
        else:
            fig.add_trace(
                go.Bar(x=df.index, y=df['ratio'], name='big_looser', text=df['sum_observations'], textposition='auto'))

        fig.show()

    def get_combined_heatmap_data(self):
        # in progress
        heatmap_data_list = []
        df = pd.DataFrame()

        unique_months = df['Date'].dt.to_period("M").unique()
        for month_period in unique_months:
            year, month = month_period.year, month_period.month

            month_start = pd.Timestamp(year, month, 1)
            # find the first Friday of the month
            first_friday = month_start
            while first_friday.weekday() != 4:
                first_friday += pd.DateOffset(days=1)

            # Calculate the 4-week span
            end_date = first_friday + pd.DateOffset(days=27)

            data_for_heatmap = df[(df["Date"] >= first_friday) & (df["Date"] <= end_date)].copy()
            data_for_heatmap["Week_Number"] = (data_for_heatmap["Date"] - first_friday).dt.days // 7 + 1
            data_for_heatmap["Weekday"] = data_for_heatmap["Date"].dt.day_name()

            heatmap_data_list.append(data_for_heatmap)

        # Concatenate all the dataframes
        heatmap_df = pd.concat(heatmap_data_list)

        # Group by Weekday and Week_Number, and aggregate values (you can adjust the aggregation function)
        heatmap_df_agg = heatmap_df.groupby(['Weekday', 'Week_Number'].agg('mean')).reset_Index()

        heatmap_data = heatmap_df_agg.pivot("Weekday", "Week_number", "Value")

        # Reindex the rows in the desired order
        ordered_days = ["Friday", "Saturday", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday"]
        heatmap_data = heatmap_data.reindex(ordered_days)

        return heatmap_data

    def _calculate_pot_from_handhistory(self, root):
        pot = 0
        # bet amount of all players
        player_contribution_to_pot = []
        for player in root.findall(".//game/general/players/player"):
            bet = player.get('bet')
            player_contribution_to_pot.append(euro_to_float(bet))

        # adjust the pot size if a player covers another or made a bet and no one called
        values_sorted = sorted(player_contribution_to_pot, reverse=True)
        if values_sorted[0] == values_sorted[1]:
            pot = sum(values_sorted)
        else:
            pot = sum(values_sorted[1:]) + values_sorted[1]
        return pot

    def _calculate_rake(self, n_players, bblind, pot_in_eur, rake_in_pct=0.0303, return_in_bb=False):
        rake_cap = self._rake_cap_table(n_players, bblind)
        rake = pot_in_eur * rake_in_pct
        if return_in_bb:
            return rake / bblind if rake < rake_cap else rake_cap / bblind
        return rake if rake < rake_cap else rake_cap

    def _rake_cap_table(self, n_players, bblind):
        if n_players == 2:
            return 0.5
        rake_caps = [(0.29, 1), (0.99, 1.5), (1.99, 2), (3.99, 2.5), (5.99, 3)]
        for limit, cap in rake_caps:
            if bblind <= limit:
                return cap
        return 4

    def _calculate_rake_contribute_for_each_player(self, root, total_rake, hero_nick, bigblind):
        # bet amount of all players
        player_contribution_to_pot = {}
        for player in root.findall(".//game/general/players/player"):
            name = player.get('name')
            bet = player.get('bet')
            player_contribution_to_pot[name] = euro_to_float(bet)

        # adjust the pot size if a player covers another or made a bet and no one called
        sorted_items = sorted(player_contribution_to_pot.items(), key=lambda x: x[1], reverse=True)
        if sorted_items[0][1] > sorted_items[1][1]:
            player_contribution_to_pot[sorted_items[0][0]] = sorted_items[1][1]

        # remove hero
        if hero_nick in player_contribution_to_pot:
            del player_contribution_to_pot[hero_nick]

        # calculate percentage each one contributes the rake
        won = sum(player_contribution_to_pot.values())

        player_contribution_to_pot_pct = {key: value / won for key, value in player_contribution_to_pot.items()}
        player_rake = {key: round(value * total_rake / bigblind, 2) for key, value in
                       player_contribution_to_pot_pct.items()}
        return player_rake

    def _calculate_seat_position_related_to_hero(self, root, hero):
        players = []
        for player in root.findall(".//game/general/players/player"):
            name = player.get('name')
            seat = int(player.get('seat'))
            players.append((name, seat))

        # Sort the players based on their seat numbers
        players.sort(key=lambda x: x[1])

        # Find the index of the specified player
        index = next((i for i, (name, seat) in enumerate(players) if name in hero), None)

        positions = {}
        if index is not None:
            # Starting from the specified player, reverse count 1 to each player before
            count = 1
            while True:
                index -= 1
                # Stop when we reach back to the specified player
                if players[index][0] in hero:
                    break
                if index < 0:
                    index = len(players) - 1  # go to the last player in the list
                positions[players[index][0]] = count

                count += 1
        return positions

    def _calculate_net_result_vs_hero(self, xml_content, hero_nick, include_rake=True):
        """
        Calculate the net result of the hero player against other players from the given XML content representing game data.

        This method parses the provided XML content to determine the net result of the hero player compared to the other players.
        The net result is calculated based on the amount each player bet and won, normalized by the big blind value, and
        considering the rake contributed by each player. If the hero player is not found in the game data, or if the hand
        is from unknown players, the method returns an empty dictionary.

        :param xml_content: str
            The XML content representing the game data.

        :return: dict
            A dictionary where the keys are the names of the players and the values are the net results of the hero player
            against them. A positive value indicates the amount won by the hero player, and a negative value indicates the
            amount lost by the hero player, both in terms of the big blind value.

        :raises LookupError:
            If no winner is found in the hand.

        :example:
        ">>> xml_content = '<some xml content>'
        ">>> obj._calculate_net_result_vs_hero(xml_content)"
        {'player1': -2.5, 'player2': 1.0, 'player3': 0}

        Note:
            This is a private helper method indicated by the single underscore prefix, and it's intended to be used internally
            within the class. Additionally, some utility methods are used internally, like `_calculate_rake`,
            `_calculate_pot_from_handhistory`, and `_calculate_rake_contribute_for_each_player`, to assist with calculations.
        """
        logger.debug(f'Calculate net results vs hero, hero: {hero_nick}')
        root = ET.fromstring(xml_content)

        players_data = {}
        bigblind = euro_to_float(root.find(".//general/bigblind").text)
        bool_raked_hand = True if root.find(".//game/round[@no='2']") is not None else False
        start_date = root.find("./game/general/startdate").text

        # find out the winner of the hand
        winner_of_hand = None
        for player in root.findall(".//game/general/players/player"):
            win = euro_to_float(player.attrib['win'])
            if win > 0:
                winner_of_hand = player.attrib['name']
                logger.debug(f"Winner of the hand {winner_of_hand}")
        if winner_of_hand is None:
            logger.error('No winner in this hand')
            raise LookupError('No winner in this hand')
        # if the hand is from unknown players skip
        if winner_of_hand in self.unknown_players:
            logger.info(f"Winner of the hand is unknown. Skipping this hand. Nickname: {winner_of_hand}")
            return

        # get the hero in the handhistory
        logger.debug(f'Looking for the hero in the handhistory. Hero Nick: {hero_nick}')
        hero_name = None
        for player in root.findall(".//game/general/players/player"):
            if player.attrib['name'] == hero_nick:
                hero_name = player.attrib['name']
                logger.debug('Found hero')
                break
        if hero_name is None:
            logger.info(f"No Hero Nickname found! Skipping this hand. \n Handhistory: {xml_content}")
            return

        # if winner is not hero, bet amount from xml data gets to the winner of the hand.
        # we assign 0 to all other players, so we can count the number of hands they played later on.
        if winner_of_hand != hero_nick:
            player = root.find(f".//game/general/players/player[@name='{hero_name}']")
            amount_lost = euro_to_float(player.attrib['bet']) / bigblind
            for player in root.findall(".//game/general/players/player"):
                player_name = player.attrib['name']
                if player_name == hero_name:
                    continue
                if player_name == winner_of_hand:
                    winner_stacksize = euro_to_float(player.attrib['chips']) / bigblind
                    amount_lost = winner_stacksize if amount_lost > winner_stacksize else amount_lost
                    players_data[winner_of_hand] = -amount_lost
                else:
                    players_data[player_name] = 0
        else:
            # if winner is hero, calculate the loss of each player per round
            # find hero stack to check later on if a player covers us
            hero_stack = 0
            for player in root.find(".//game/general/players"):
                name = player.get('name')
                if name == hero_nick:
                    hero_stack = euro_to_float(player.get('chips')) / bigblind
                    break
            if hero_stack == 0:
                logger.warning(f"Hero is winner, but no Stack found.")

            # add the player name and won/loss to the dict
            for player in root.findall(".//game/general/players/player"):
                if player.attrib['name'] == hero_nick:
                    continue
                amount_won = euro_to_float(player.attrib['bet']) / bigblind
                if amount_won > hero_stack:
                    amount_won = hero_stack
                players_data[player.attrib['name']] = amount_won

            # substract the contributed rake from each player to get the net won.
            # only do it if we have round 2 in the handhistory, preflop is no rake
            if bool_raked_hand and include_rake:
                # calculate rake
                n_players = len(root.findall(".//players/player"))
                pot = self._calculate_pot_from_handhistory(root)
                rake = self._calculate_rake(pot_in_eur=pot,
                                            n_players=n_players,
                                            bblind=bigblind)
                # get the amount of rake each player paid
                player_rake = self._calculate_rake_contribute_for_each_player(root,
                                                                              total_rake=rake,
                                                                              hero_nick=hero_name,
                                                                              bigblind=bigblind)
                # adjust the amount of won vs each player
                players_data = {key: players_data[key] - player_rake.get(key, 0) for key in players_data}

        # add the share of the hand for each player
        n_players = len(players_data)
        players_data = {key: [value, 1 / n_players] for key, value in players_data.items()}

        # calculate distance to hero
        player_positions = self._calculate_seat_position_related_to_hero(root=root, hero=hero_nick)
        for key in player_positions:
            if key in players_data:
                players_data[key].append(player_positions[key])

        # # check if splitpot
        # winnings = []
        # for player in root.findall(".//game/general/players/player"):
        #     win = euro_to_float(player.get('win'))
        #     if win > 0:
        #         winnings.append(win)
        # if len(winnings) > 2:
        #     print(xml_content)
        #     print(players_data)

        self.analyse_hand_count += 1
        logger.debug(f"Return players_data: {players_data}")
        return players_data, start_date

    def _get_won_lost_vs_player(self,
                                hero,
                                min_bblind=None,
                                max_bblind=None,
                                min_active_players=3,
                                max_active_players=6,
                                add_cohort=True, include_rake=True):
        # set variables
        if min_bblind is None:
            min_bblind = self.min_bigblind
        if max_bblind is None:
            max_bblind = self.max_bigblind

        wins_sample = []
        n_hero_names = len(hero)
        count = 1
        for hero_nick in hero:
            logger.info(f"Looking for hands for hero: {hero_nick} Number {count} of {n_hero_names}.")
            count += 1

            # getting the hand_ids, where the player is involved
            query = f"""SELECT DISTINCT(hand_id) from player_timestamps 
                            WHERE player = '{hero_nick}'"""
            player_hand_ids = self.DB_CONNECTION.get_table_as_df(query)

            # filter the hand_ids by min/max bblind and n_active_players
            query = f"""SELECT hand_id, big_blind, n_active_players FROM hand_info WHERE hand_id in {tuple(player_hand_ids['hand_id'])}"""
            hand_ids = self.DB_CONNECTION.get_table_as_df(query)
            hand_ids['big_blind'] = hand_ids['big_blind'].apply(euro_to_float)
            hand_ids = hand_ids[(hand_ids['big_blind'].between(min_bblind, max_bblind)) &
                                (hand_ids['n_active_players'].between(min_active_players, max_active_players))]

            # get hand histories
            query = f"""SELECT * FROM hand_history WHERE hand_id in {tuple(hand_ids['hand_id'])} """
            hand_histories = self.DB_CONNECTION.get_table_as_df(query)

            # create the dataframe with the observations
            df = hand_histories.copy()
            logger.info(f"Number of hands to calculate the net_won: {len(df)}")

            for xml_str in df['hand_history']:
                try:
                    net_won_vs_hero, start_date = self._calculate_net_result_vs_hero(xml_str, hero_nick=hero_nick,
                                                                                     include_rake=include_rake)
                except Exception as e:
                    logger.warning(f'No data for the players, either players are unknown or no hero found. {e}')
                    continue
                # add net_won_vs_hero to a dataframe to get the changes over time
                wins_sample.extend(
                    (start_date, key, value[0], value[1], value[2]) for key, value in net_won_vs_hero.items())
                logger.debug(wins_sample)

        df = pd.DataFrame(wins_sample,
                          columns=['start_date', 'player', 'won_vs_hero', 'share_of_hand', 'seat_distance_to_hero'])

        # adding cohorts if set
        if add_cohort:
            # get cohort list
            cohorts = self.load_player_data_from_DB(return_only_columns=['player', 'cohort'])
            cohorts = cohorts.drop_duplicates()
            df = df.merge(cohorts[['player', 'cohort']], on='player', how='left')
        return df

    def calculate_won_lost_from_cohort(self,
                                       hero,
                                       min_active_players,
                                       max_active_players,
                                       include_rake=True,
                                       show_result_over_time=False,
                                       show_position_data=False,
                                       show_position_graph=False):

        if hero == 'hero':
            hero = self.hero
        else:
            cohorts = self.load_player_data_from_DB(return_only_columns=['player'])
            hero = cohorts['player'].tolist()   # have to be done

        df = self._get_won_lost_vs_player(hero=hero,
                                          min_active_players=min_active_players,
                                          max_active_players=max_active_players,
                                          add_cohort=True, include_rake=include_rake)

        print("Total sum of won_loss: ", df['won_vs_hero'].sum())
        print()

        df_won_loss = df.groupby('cohort', as_index=False).agg(
            hero_won=('won_vs_hero', 'sum'),
            hand_count=('share_of_hand', 'sum')
        )

        df_won_loss['bb_100'] = df_won_loss['hero_won'] / df_won_loss['hand_count'] * 100
        print(df_won_loss.sort_values(by='bb_100', ascending=False))

        if show_result_over_time:
            df_plot = df.sort_values(by='start_date').reset_index()

            fig = go.Figure()
            for cohort in df_plot['cohort'].unique():
                df_result = df_plot[df_plot['cohort'] == cohort][['won_vs_hero']].reset_index()
                df_result['cum_sum_won_vs_hero'] = df_result['won_vs_hero'].cumsum()
                fig.add_trace(go.Scatter(x=df_result.index, y=df_result['cum_sum_won_vs_hero'], name=cohort))
            fig.update_layout(title=f"Won from Cohort over hands. Limit > {self.min_bigblind}")
            fig.show()

        if show_position_data:
            vs_cohort = ['reg_aggro']
            df = df[df['cohort'].isin(vs_cohort)]

            df_distance = df.groupby('seat_distance_to_hero', as_index=False).agg(
                hero_won=('won_vs_hero', 'sum'),
                hand_count=('share_of_hand', 'sum')
            )
            df_distance['bb_100'] = df_distance['hero_won'] / df_distance['hand_count'] * 100

            if show_position_graph and show_position_data:
                # plot the positions
                fig = go.Figure()
                for distance in df['seat_distance_to_hero'].unique():
                    df_plot = df[df['seat_distance_to_hero'] == distance][['won_vs_hero']].reset_index()
                    df_plot['cumsum_won'] = df_plot['won_vs_hero'].cumsum()
                    fig.add_trace(go.Scatter(x=df_plot.index, y=df_plot['cumsum_won'], name=str(distance)))
                fig.show()

            print()
            print(f"Hero vs {vs_cohort}. Tablesize: {min_active_players}-{max_active_players}. Blinds: {self.min_bigblind, self.max_bigblind}")
            print(df_distance)
