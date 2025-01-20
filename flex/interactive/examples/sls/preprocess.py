#!/bin/python3

import sys
import os
import csv


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 preprocess.py <output_dir>")
        sys.exit(1)
        
    output_dir = sys.argv[1]

    files=["test1.csv", "test2_1.csv", "test2_2.csv", "test3.csv", "test4.csv", "test5.csv", "test6.csv"]
    vertex_map = {}
    edge_map = {}
    # vertex_map = {vertex_name: vertex_data_type}
    # edge_map = {<src_vertex_name, dst_vertex_name, edge_label_name>: edge_data_type}
    
    for f in files:
        # read csv
        with open(f, mode='r') as infile:
            reader = csv.reader(infile)
            for row in reader:
                # columns are edge_label_name, src_vertex_name, dst_vertex_name, src_vertexId, dst_vertexId
                edge_label_tuple = (row[1], row[2], row[0])
                if edge_label_tuple not in edge_map:
                    edge_map[edge_label_tuple] = []
                edge_map[edge_label_tuple].append((row[3], row[4]))
                if row[1] not in vertex_map:
                    vertex_map[row[1]] = []
                if row[2] not in vertex_map:
                    vertex_map[row[2]] = []
                vertex_map[row[1]].append(row[3])
                vertex_map[row[2]].append(row[4])
    
    for vertex in vertex_map:
        vertex_map[vertex] = list(set(vertex_map[vertex]))
        print(f"vertex label: {vertex}, vertices: {vertex_map[vertex]}")
        # write to file ${output_dir}/vertex_${vertex}.csv
        # replace @ with _
        vertex_replaced = vertex.replace('@', '_')
        with open(f"{output_dir}/vertex_{vertex_replaced}.csv", mode='w') as outfile:
            writer = csv.writer(outfile)
            for vertex_id in vertex_map[vertex]:
                writer.writerow([vertex_id])
        
    for edge in edge_map:
        print(f"edge label: {edge}, edges: {edge_map[edge]}")
        # write to file ${output_dir}/edge_${src_vertex}_${dst_vertex}_${edge_label}.csv
        edge_triplet= f"{edge[0]}_{edge[1]}_{edge[2]}"
        edge_triplet = edge_triplet.replace('@', '_')
        with open(f"{output_dir}/edge_{edge_triplet}.csv", mode='w') as outfile:
            writer = csv.writer(outfile)
            for edge_tuple in edge_map[edge]:
                writer.writerow(edge_tuple)
    
                