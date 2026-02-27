import duckdb
# from pprint import pprint
# %load_ext sql
# %sql duckdb:///mtga_local.duckdb

conn = duckdb.connect(database='mtga_local.duckdb')
tables = conn.execute(
    """
    SELECT 
        (SELECT count(*) FROM matches) as matches_rows,
        (SELECT count(*) FROM decks) as decks_rows,
        (SELECT count(*) FROM turn1_hands) as t1hand_rows,
        (SELECT count(*) FROM dim_cards) as cards_rows
        ;
    """
).fetchall()

print(tables)
