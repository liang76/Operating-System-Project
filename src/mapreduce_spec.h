#pragma once

#include <string>
#include <fstream>
#include <vector>
#include <iostream>

using namespace std;

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	int n_workers;
	vector<string> worker_ipaddr;
	vector<string> input_files;
	string output_dir;
	int n_output_files;
	int map_kilobytes;
	string user_id;
};


/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	vector<string> lines(8);
	//cout<<"filename: "<<config_filename<<endl;
	ifstream config(config_filename.c_str());
	if(config.is_open()){
		int index = 0;
		while(getline(config, lines[index])){
			index++;
		}
		config.close();
		
		mr_spec.n_workers = stoi(lines[0].substr(lines[0].find("=")+1, lines[0].size()-lines[0].find("=")-1));
		//cout << "n_workers: "<<mr_spec.n_workers<<endl;
		mr_spec.n_output_files = stoi(lines[4].substr(lines[4].find("=")+1, lines[4].size()-lines[4].find("=")-1));
		//cout << "n_output_files: "<<mr_spec.n_output_files<<endl;
		mr_spec.map_kilobytes = stoi(lines[5].substr(lines[5].find("=")+1, lines[5].size()-lines[5].find("=")-1));
		//cout << "map_kilobytes: "<<mr_spec.map_kilobytes<<endl;
		mr_spec.user_id = lines[6].substr(lines[6].find("=")+1, lines[6].size()-lines[6].find("=")-1);
		//cout << "n_user_id: "<<mr_spec.user_id<<endl;
		
		int start = lines[1].find("=")+1;
		for(int i = lines[1].find("="); i < lines[1].size(); i++){
			if(lines[1][i] == ','){
				//cout<<lines[1].substr(start, i-start)<<endl;
				mr_spec.worker_ipaddr.push_back(lines[1].substr(start, i-start));
				start = i+1;
			}
		}
		//cout<<lines[1].substr(start, lines[1].size()-start)<<endl;
		mr_spec.worker_ipaddr.push_back(lines[1].substr(start, lines[1].size()-start));


		const string path = config_filename.substr(0, config_filename.size()-10);
		start = lines[2].find("=")+1;
		for(int i = lines[2].find("="); i < lines[2].size(); i++){
			if(lines[2][i] == ','){
				//cout<<path+lines[2].substr(start, i-start)<<endl;
				mr_spec.input_files.push_back(path+lines[2].substr(start, i-start));
				start = i+1;
			}
		}
		mr_spec.input_files.push_back(path+lines[2].substr(start, lines[2].size()-start));
		//cout<<path+lines[2].substr(start, lines[2].size()-start)<<endl;

		mr_spec.output_dir = path+lines[3].substr(lines[3].find("=")+1, lines[3].size()-lines[3].find("=")-1);
		//cout << "output_dir: "<<mr_spec.output_dir<<endl;
		
	} else {
		cout<< "Unable to open config file"<<endl;
		return false;
	}

	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	bool check = true;
	if(mr_spec.n_workers != mr_spec.worker_ipaddr.size()){
		check = false;
	}

	for(int i = 0; i < mr_spec.input_files.size(); i++){
		ifstream input(mr_spec.input_files[i].c_str());
		if(!input.is_open()){
			check = false;
		}
	}

	if(check){
		return true;
	} else {
		return false;
	}
}
