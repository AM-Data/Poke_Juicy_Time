import time
import load
import poker_metrics
import extract
import calculate_statistics
from my_logger import CustomLogger

##### Logger #####
logger = CustomLogger(__name__).get_logger()

##### Database Connection #####
DATABASE_URL = 'mysql+mysqlconnector://root:BCfxCk0UyMpi5iJLp6tq@localhost/jucy_poker'
db_conn = load.DataBaseManagement(DATABASE_URL, create_all_tables=True)

path = r"C:\MyHandsArchive_H2N\IPoker"
# extract.ExtractHandhistories().extract_folders(path)
# update_stats = calculate_statistics.PokerStats(db_conn).update_stats_from_xml()


# Testing won vs her
start_time = time.time()
test = poker_metrics.PokerMetrics(db_conn, min_bigblind=1, max_bigblind=10)
# test.calculate_won_lost_from_cohort(min_active_players=6, max_active_players=6, show_result_over_time=True, hero="hero", show_position_data=True)
test.plot_big_looser_big_winner_ratio(time_unit='weekday_hour')
end_time = time.time()
print(end_time-start_time)
