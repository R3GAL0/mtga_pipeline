
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

#       unwanted metadata/response types
        unwanted = ['ClientToGreuimessage', 'ClientToGremessage', '==>', 'Client.TcpConnection.Close']

        # Convert file to an iterator to look ahead safely
        log_iter = iter(log_data)
        buffered_line = None

        while True:
            # grabbing the buffered line, and handling end of file
            if buffered_line:
                line = buffered_line
                buffered_line = None
            else:
                try:
                    line = next(log_iter)
                except StopIteration:
                    break
                line = line.strip()

            # Looking for the start and stop of the match
            if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectedToGRE_Waiting","new":"Playing"}'):
                recording = True
                print('recording on')
                continue

            if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}'):
                recording = False
                print('recording off')
                game_num += 1
                continue

            if not recording:
                continue

            # Skip unwanted lines
            if not line.startswith('[UnityCrossThreadLogger]') or any(u in line for u in unwanted):
                continue

            # Droping [UnityCrossThreadLogger] prefix
            line = line[24:]

            # Extract metadata
            index = line.find('{')
            if index != -1:
                metadata = line[:index]
                payload = line[index:]
            else:
                metadata = line
                try:
                    next_line = next(log_iter).strip()
                    # Stop if the next line is a state change
                    if next_line.startswith('[UnityCrossThreadLogger]STATE CHANGED'):
                        continue
                    payload = next_line
                except StopIteration:
                    payload = ''

            # The payload is a nested json, need to track open and close paren    
            depth = payload.count('{') - payload.count('}')
            while depth > 0:
                try:
                    next_line = next(log_iter).strip()
                except StopIteration:
                    break

                # Stop if the next line is another response
                if next_line.startswith('[UnityCrossThreadLogger]'):
                    buffered_line = next_line
                    break

                payload += next_line
                depth += next_line.count('{') - next_line.count('}')

            out_file.writerow([game_num, metadata, payload])


for file in raw_logs:
    raw_path_file       = raw_path + r'/' + file
    output_path_file    = output_path + r'/Filtered_' + file[:-4] + '.csv'

    parse_logs(raw_path_file, output_path_file)