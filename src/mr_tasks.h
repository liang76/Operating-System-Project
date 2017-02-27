#pragma once

#include <string>
#include <iostream>
#include <fstream>
#include <vector>

/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the map task*/
struct BaseMapperInternal {

		/* DON'T change this function's signature */
		BaseMapperInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void set(const std::string dir, const int n_r);
		std::vector<std::string>& get();

		std::string output_dir;
		int R;
		std::vector<std::string> inter_files;
};


/* CS6210_TASK Implement this function */
inline BaseMapperInternal::BaseMapperInternal() {
	output_dir = "";
	R = 0;
}

inline void BaseMapperInternal::set(const std::string dir, const int n_r) {
	output_dir = dir;
	R = n_r;
	inter_files.resize(R);
}

inline std::vector<std::string>& BaseMapperInternal::get() {
	return inter_files;
}

/* CS6210_TASK Implement this function */
inline void BaseMapperInternal::emit(const std::string& key, const std::string& val) {
	std::hash<std::string> str_hash;
	int num = str_hash(key) % R;
	std::ofstream inter_file;
	inter_files[num] = output_dir + "/intermediate_file_" + std::to_string(num) + ".txt";
  	inter_file.open (output_dir + "/intermediate_file_" + std::to_string(num) + ".txt", std::ofstream::out | std::ofstream::app);
  	inter_file << key << ","<<val<< "\n";
  	inter_file.close();
	//std::cout << "Emit by BaseMapperInternal: " << key << ", " << val << std::endl;
}


/*-----------------------------------------------------------------------------------------------*/


/* CS6210_TASK Implement this data structureas per your implementation.
		You will need this when your worker is running the reduce task*/
struct BaseReducerInternal {

		/* DON'T change this function's signature */
		BaseReducerInternal();

		/* DON'T change this function's signature */
		void emit(const std::string& key, const std::string& val);

		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void set(const std::string dir, const int n_r, const int id);
		std::vector<std::string>& get();

		std::string output_dir;
		std::vector<std::string> output_files;
		int id_;
};


/* CS6210_TASK Implement this function */
inline BaseReducerInternal::BaseReducerInternal() {
	output_dir = "";
	id_ = -1;
}

inline void BaseReducerInternal::set(const std::string dir, const int R, const int id) {
	output_dir = dir;
	id_ = id;
	output_files.resize(R);
}

inline std::vector<std::string>& BaseReducerInternal::get() {
	return output_files;
}

/* CS6210_TASK Implement this function */
inline void BaseReducerInternal::emit(const std::string& key, const std::string& val) {
	std::ofstream output_file;
	output_files[id_] = output_dir + "/output_file_" + std::to_string(id_) + ".txt";
  	output_file.open (output_dir + "/output_file_" + std::to_string(id_) + ".txt", std::ofstream::out | std::ofstream::app);
  	output_file << key << ","<<val<< "\n";
  	output_file.close();
	//std::cout << "Emit by BaseReducerInternal: " << key << ", " << val << std::endl;
}
