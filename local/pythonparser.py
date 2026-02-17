
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

# removing the Filtered_ prefix and .csv suffix, adding .log suffix
output_files = [x[9:-4]+'.log' for x in output_logs]

if 'placeholder.md' in raw_logs:
    raw_logs.remove('placeholder.md')

# removing the already run log files from the list of raw_logs to run
for item in output_files:
    if item in raw_logs:
        raw_logs.remove(item)


def parse_logs(input_path_file, output_path_file):
    recording = False
    game_num = 1
    print(input_path_file)

    with open(input_path_file, 'r') as log_data, open(output_path_file, 'w') as csvfile:
        out_file = csv.writer(csvfile)
        out_file.writerow(["game_num", "metadata", "payload"])

        for line in log_data:
            # skip these system messages
            unwanted = ['ClientToGreuimessage', 'ClientToGremessage', '==>', 'Client.TcpConnection.Close']
            payload = ''
            if recording and line.startswith('[UnityCrossThreadLogger]') and not any(u in line for u in unwanted):    
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
                print('recording on')
            elif line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}' or payload == '{"old":"Playing","new":"MatchCompleted"}'):
                recording = False
                print('recording off')
                game_num += 1


for file in raw_logs:
    raw_path_file       = raw_path + r'/' + file
    output_path_file    = output_path + r'/Filtered_' + file[:-4] + '.csv'

    parse_logs(raw_path_file, output_path_file)