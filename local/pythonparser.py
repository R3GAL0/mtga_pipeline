# Read a .log file

# Do some basic bulk cleaning

# convert to flat csv



path = r'/home/r3gal/Desktop/Player-prev.log'

output_path = r'/home/r3gal/Desktop/Filtered_Player_prev.log'

with open(path, 'r') as log_data, open(output_path, 'w') as out_file:
    for line in log_data:
        if not line.startswith("There is no"):
            out_file.write(line)