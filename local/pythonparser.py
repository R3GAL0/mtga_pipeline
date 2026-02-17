
# parses Player.log files and creates a .csv for each .log
# .csv has columns="game_num", "metadata", "payload"

# setup to bulk run over the whole of ./data/raw while ignoring already parsed files


import csv
import os

# raw and output file paths
raw_path = '/home/r3gal/develop/mtga_pipeline/data/raw'
output_path = '/home/r3gal/develop/mtga_pipeline/data/upload'

# filtering out the logs that were already processed/are present in the output_path
raw_logs = os.listdir(path=raw_path)
output_logs = os.listdir(path=output_path)

output_files = [x[10:] for x in output_logs]

if 'placeholder.md' in raw_logs:
    raw_logs.remove('placeholder.md')
    
difference = list(set(raw_logs) - set(output_files))



def parse_logs(input_path_file, output_path_file):
    recording = False
    game_num = 1

    with open(input_path_file, 'r') as log_data, open(output_path_file, 'w') as csvfile:
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


for raw_file in difference:
    raw_path_file       = raw_path + r'/' + raw_file
    output_path_file    = output_path + r'/Filtered_' + raw_file[:-4] + '.csv'

    parse_logs(raw_path_file, output_path_file)