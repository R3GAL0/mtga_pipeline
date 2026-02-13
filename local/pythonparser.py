# Read a .log file

# Do some basic bulk cleaning

# convert to flat csv

import csv

filenm = "Filtered_Player_20260212_233143_test"

path = rf'/home/r3gal/develop/mtga_pipeline/data/{filenm}.log'
output_path = rf'/home/r3gal/develop/mtga_pipeline/data/Filtered_{filenm}.csv'

recording = False
game_num = 1

with open(path, 'r') as log_data, open(output_path, 'w') as csvfile:
    out_file = csv.writer(csvfile)
    out_file.writerow(["game_num", "metadata", "payload"])

    for line in log_data:

        if recording & line.startswith('[UnityCrossThreadLogger]'):    
            line = line[24:]

            index = line.find('{')
            if index != -1:
                metadata = line[:index]
            else:
                metadata = line
            line = line[len(metadata):]


            payload = line
            # currently doesnt account for depth of brackets
            # ie finishes on finding the first } which is not the full end of the payload
            # need to track the depth of brackets {}
            depth = 1
            while '}' not in payload:
                payload += next(log_data)


            row = [game_num, metadata, payload]
            out_file.writerow(row)

        if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectedToGRE_Waiting","new":"Playing"}'):
            recording = True
        elif line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}'):
            recording = False


# Columns
# game number, PlayerID, Timestamp, Meta data, payload