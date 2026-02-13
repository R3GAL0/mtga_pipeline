# Read a .log file

# Do some basic bulk cleaning

# convert to flat csv

filenm = "Player_20260212_221356_2games"

path = rf'/home/r3gal/develop/mtga_pipeline/data/{filenm}.log'
output_path = rf'/home/r3gal/develop/mtga_pipeline/data/Filtered_{filenm}.log'

recording = False
with open(path, 'r') as log_data, open(output_path, 'w') as out_file:
    for line in log_data:
        if line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"ConnectedToMatchDoor_ConnectedToGRE_Waiting","new":"Playing"}'):
            recording = True
        elif line.startswith('[UnityCrossThreadLogger]STATE CHANGED {"old":"Playing","new":"MatchCompleted"}'):
            recording = False
            out_file.write(line)

        if recording:    
            out_file.write(line)

