"""
MTGA Data Pipeline: Bronze Layer Ingestion

Parse the Player_log.log and dump each session of games into a Player.csv 
Tracking new games, Player_id, and game start time

GENERAL PROCESS:
    Parse the Player.log line by line
    Record last deck found
    When a game start is detected write the deck list to the csv and start recording game event payloads
    When game end is detected stop recording and continue looking line by line waiting for the next game start event

    If no games were found in Player.log delete the raw file

    The program is made to processes any files in /data/raw, 
        it will ignore the raw file if the parsed version is in /data/parsed_csv

    The parsed_csv files will then be moved to the cloud for insertion into the silver layer

Notes:
    .csv has columns="game_num", "player_id", "timestamp", "event", "payload"
"""

import csv
import os
import json
import re

# raw and output file paths
raw_path = '/home/r3gal/develop/mtga_pipeline/data/raw'
output_path = '/home/r3gal/develop/mtga_pipeline/data/parsed_csv'

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

match_start_pattern = re.compile(
    r'^\[UnityCrossThreadLogger\]STATE CHANGED '
    r'\{"old":"ConnectedToMatchDoor_(?:ConnectedToGRE_Waiting|ConnectingToGRE)","new":"Playing"\}'
)

def parse_logs(input_path_file, output_path_file):
    recording = False
    game_num = 0
    rows_written = 0
    print(input_path_file)

    with open(input_path_file, 'r') as log_data, open(output_path_file, 'w') as csvfile:
        out_file = csv.writer(csvfile)
        out_file.writerow(["game_num", "player_id", "timestamp", "event", "payload"])

#       unwanted metadata/response types
        unwanted = ['ClientToGreuimessage', 'ClientToGremessage', '==>', 'Client.TcpConnection.Close']

        # Convert file to an iterator to read ahead on lines without consuming them
        log_iter = iter(log_data)
        buffered_line = None
        deck_list = ''


        while True:
            # grabbing the buffered line, and handling end of file
            if buffered_line:
                print(buffered_line)
                line = buffered_line
                buffered_line = None
            else:
                try:
                    line = next(log_iter)
                except StopIteration:
                    break
                line = line.strip()

            # I want to grab this line prior to recording, it contains the deck list
            if '[UnityCrossThreadLogger]==> DeckUpsertDeckV2' in line:
                deck_list = line 
                continue

            # Looking for the start and stop of the match
            # the state doesnt always switch to 'ConnectedToMatchDoor_ConnectedToGRE_Waiting', 
            # in cases of low latency/server load state will switch immediately from 'ConnectedToMatchDoor_ConnectingToGRE' to 'Playing'
            if match_start_pattern.match(line):
                recording = True
                # print('recording on')
                game_num += 1
                continue

            if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}'):
                recording = False
                continue

            if not recording:
                continue

            # Skip unwanted lines
            if not line.startswith('[UnityCrossThreadLogger]') or any(u in line for u in unwanted):
                continue

            # A match has started, now parsing the lines and recording

            # Droping [UnityCrossThreadLogger] prefix
            line = line[24:]

            # Extract metadata
            index = line.find('{')
            if index != -1:
                metadata = line[:index]
                payload = line[index:]
            else:
                metadata = line
                next_line = next(log_iter).strip()
                payload = next_line


            # splitting metadata into timestamp, player_id and event columns
            metadata_split = metadata.split(': ')
            if len(metadata_split[1].split(' ')) == 3:
                metadata_split[1] = metadata_split[1].split(' ')[2]

            # a match started, writing the last found deck list as the first line of the game
            if len(deck_list) > 1:
                # trim to only the payload, wrap in a 'list' to explode payload column later
                deck_list = '[' + deck_list[deck_list.find('{'):] + ']'

                out_file.writerow([game_num, metadata_split[1], metadata_split[0], 'deck_list', deck_list])
                deck_list = ''

            # check payload for valid requestid, if missing then drop row 
            # (signals it is just a timer event aka a player went on the rope and a timer was displayed )
            try:
                payload_json = json.loads(payload)

                if metadata_split[2] == 'GreToClientEvent':
                    payload_str = json.dumps(payload_json['greToClientEvent']['greToClientMessages'])
                elif metadata_split[2] == 'MatchGameRoomStateChangedEvent':
                    # wrapping in a list to explode later
                    payload_str = '[' + json.dumps(payload_json['matchGameRoomStateChangedEvent']['gameRoomInfo']) + ']'
                else:
                    payload_str = json.dumps(payload_json)

#           If a player causes > 50 events in one turn a JSON object will not be returned
            except json.JSONDecodeError:
                # payload_str = payload
                payload_str = json.dumps(payload)

            out_file.writerow([game_num, metadata_split[1], metadata_split[0], metadata_split[2], payload_str])
            rows_written += 1
    # removing files that have no games, both .log and .csv are deleted
    if rows_written == 0:
        os.remove(output_path_file)
        os.remove(input_path_file)



for file in raw_logs:
    raw_path_file       = raw_path + r'/' + file
    output_path_file    = output_path + r'/Filtered_' + file[:-4] + '.csv'

    parse_logs(raw_path_file, output_path_file)