import duckdb
# from pprint import pprint
# %load_ext sql
# %sql duckdb:///mtga_local.duckdb

def show_tables(db_dir):
    conn = duckdb.connect(database=db_dir)
    tables = conn.execute(
        """
        SELECT 
            (SELECT count(*) FROM matches) as matches_rows,
            (SELECT count(*) FROM decks) as decks_rows,
            (SELECT count(*) FROM turn1_hands) as t1hand_rows,
            (SELECT count(*) FROM dim_cards) as cards_rows,
            (SELECT count(*) FROM players) as players_rows
            ;
        """
    ).fetchall()
    conn.close()

    print('\nRow Counts')
    print('players: ' + str(tables[0][4]))
    print('matches: ' + str(tables[0][0]))
    print('decks:   ' + str(tables[0][1]))
    print('t1hand:  ' + str(tables[0][2]))
    print('cards:   ' + str(tables[0][3]))

if __name__ == "__main__":
    show_tables("/home/r3gal/develop/mtga_pipeline/cloud/mtga_local.duckdb")