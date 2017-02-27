#pragma once

#include "mapreduce_spec.h"
#include <math.h>


/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */
struct FileShard {
	std::vector<string> files;
	std::vector<int> starts;
	std::vector<int> ends;
};


/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */ 
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	ifstream input;
	std::vector<int> files_size;
	int sum = 0;
	for(int i = 0; i < (mr_spec.input_files).size(); i++){
		input.open(mr_spec.input_files[i].c_str());
		if(input.is_open()){
			input.seekg(0, ios_base::end);
			files_size.push_back(input.tellg());
			sum += files_size[i];
			input.close();
		} else {
			cout<<"Unable to open input file"<<endl;
		}
	}

	int M = (int)ceil(sum / 1000.0 / mr_spec.map_kilobytes);
	//cout<<"M: "<<M<<endl;

	int start = 0, end = 0, num = 0, map = mr_spec.map_kilobytes * 1000;
	for(int i = 0; i < M; i++){
		FileShard shard;
		for(int j = num; j < (mr_spec.input_files).size(); j++) {
			if((start + map) < files_size[j]){
				input.open(mr_spec.input_files[j]);
				shard.files.push_back(mr_spec.input_files[j]);
				shard.starts.push_back(start);
				end = start + map;
				input.seekg(end);
				char c;
				int add = 0;
				while(input.get(c)){
					add++;
					if(c == '\n'){
						break;
					}
				}
				end += add;
				start = end;
				shard.ends.push_back(end);
				input.close();
				num = j;
				map = mr_spec.map_kilobytes * 1000;
				break;
			} else {
				shard.files.push_back(mr_spec.input_files[j]);
				shard.starts.push_back(start);
				shard.ends.push_back(files_size[j]);
				map -= files_size[j] - start;
				start = 0;
			}
		}

		fileShards.push_back(shard);
	}
	
	return true;
}
