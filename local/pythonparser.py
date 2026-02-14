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

        if recording and line.startswith('[UnityCrossThreadLogger]'):    
            # Dropping '[UnityCrossThreadLogger]'
            line = line[24:]

            # Grabbing the metadata. Checking if the payload is part of the line
            index = line.find('{')
            if index != -1:
                metadata = line[:index]
            else:
                metadata = line
            line = line[len(metadata):]

#           incase metadata eats the whole string
            if len(line) == 0:
                payload = next(log_data)
            else:
                payload = line
            
            # The payload is a nested json, need to track open and close paren    
            depth = payload.count('{') - payload.count('}')
            while depth > 0:
                next_line = next(log_data)
                payload += next_line
                depth += next_line.count('{') - next_line.count('}')


            row = [game_num, metadata, payload]
            out_file.writerow(row)

        # Looking for the start and stop of the match
        if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectedToGRE_Waiting","new":"Playing"}'):
            recording = True
        elif line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}'):
            recording = False
            game_num += 1

