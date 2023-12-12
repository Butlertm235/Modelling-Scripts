"""
Script to convert SATURN network into TAsK format. The script parses through a SATURN .dat network file and cuts the relevant data for use
in the TAsK software: https://github.com/olga-perederieieva/TAsK.

TAsK Format is as follows:
Initial node | Terminal node | Link capacity | Link length | free_flow_time | b | power | speed | toll | link_type | ;

IMPORTANT: This script was developed as part of a research project. The methodology of the conversion process was changed after this script
was developed, therefore the code is not commented nor comes with any guarantee of accuracy. Though, every care was taken during the
development of the script. Some columns of the output network dataframe were not completed, though calculations for such network parameters
can be found by examining the BPR curve.

I hope this can be used as a starting point for analysing SATURN networks, or reverse engineered to speed up network coding in SATURN.


For any queries, please contact Shane Elliot: shane.elliot@wsp.com


"""

import numpy as np
import pandas as pd



network_filename = "FILENAME.dat"

network_lines = []
sim_cc_lines = []
node_coords_lines = []
section = "empty"

with open (network_filename, 'rt') as network_filename:
    for line in network_filename:
        line = line.rstrip("\n")
        if line == "1111111111":
            section = "links"       
        if line == "2222222222":
            section = "sim_cc"     
        if line == "5555555555":
            section = "nodes"
        if line == "9999999999":
            section = "empty"
        
        if section == "links":
            network_lines.append(line)
        if section == "sim_cc":
            sim_cc_lines.append(line)
        if section == "nodes":
            node_coords_lines.append(line)

network_lines = network_lines[1:]
sim_cc_lines = sim_cc_lines[2:]
node_coords_lines = node_coords_lines[1:]

nodes_list = []

network_columns = ["A_Node","B_Node","Link_ID","Speed","Length","Capacity","Free_flow_time","b","power","Toll","Link_type",";"]
network_df = pd.DataFrame(columns=network_columns)

i=0
skip_line = 0

while i < len(network_lines):
    print("i=",i)
    if network_lines[i][0].isdigit() == True:
        A_node = network_lines[i][0:5]
        nodes_list.append(A_node)
        num_arms = int(network_lines[i][9])
        skip_line = 0
        for j in range(num_arms):
            index = i+1+skip_line
            print("index=",index)
            if network_lines[index][10] == "*":
                skip_line += 1

            num_lanes = int((network_lines[index][13:15].strip()).translate({ord(k): None for k in 'FB'}))      #Removed any turn modifiers such as Flare etc

            if num_lanes == 0:
                pass
            else:
                B_node = network_lines[index][5:10]
                nodes_list.append(B_node)
                Link_ID = A_node+"_"+B_node

                for k in range(num_arms-1):
                    capacity = network_lines[index][25+k*10:30+k*10].strip()
                    lane_from = network_lines[index][32+k*10]
                    lane_to = network_lines[index][34+k*10]
                    if capacity == "":
                        pass
                    else:
                        break
                approx_capacity = int(capacity)/((int(lane_from)+int(lane_to))/2)
                link_speed = int(network_lines[index][15:20].strip())
                link_length = int(network_lines[index][20:25].strip())

                network_df.loc[index,"A_Node"] = A_node
                network_df.loc[index,"B_Node"] = B_node
                network_df.loc[index,"Link_ID"] = Link_ID
                network_df.loc[index,"Speed"] = link_speed
                network_df.loc[index,"Length"] = link_length
                network_df.loc[index,"Capacity"] = approx_capacity
                network_df.loc[index,"Free_flow_time"] = link_length/link_speed
            i += 1
    i += 1

network_df.loc[:,"Toll"] = 0
network_df.loc[:,"Link_type"] = 1
network_df.loc[:,";"] = ";"

sim_cc_columns = ["Zone_ID","Node_ID"]
sim_cc_df = pd.DataFrame(columns = sim_cc_columns)
sim_cc_list = []

for i in range(len(sim_cc_lines)):
    Zone_ID = sim_cc_lines[i][0:5]
    sim_cc_df.loc[i,"Zone_ID"] = Zone_ID
    sim_cc_df.loc[i,"Node_ID"] = sim_cc_lines[i][5:10]
    sim_cc_list.append(Zone_ID)

node_coords_columns = ["Node_ID","x","y"]
node_coords_df = pd.DataFrame(columns=node_coords_columns)

for i in range(len(node_coords_lines)):
    if node_coords_lines[i][0] == "*":
        pass
    else:
        s = node_coords_lines[i][10:].partition(",")
        node_coords_df.loc[i,"Node_ID"] = int(node_coords_lines[i][5:10])
        node_coords_df.loc[i,"x"] = float(s[0].strip())
        node_coords_df.loc[i,"y"] = float(s[2].strip())


nodes_list = list(dict.fromkeys(nodes_list))    #Remove duplicates from nodes list. This list will be used to check that we have coords for all nodes

print(network_df)
print(sim_cc_df)
print(sim_cc_list)
print(node_coords_df)
print(nodes_list)