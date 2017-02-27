#pragma once

#include "mapreduce_spec.h"
#include "file_shard.h"
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <chrono>

#include <grpc++/grpc++.h>

#include "masterworker.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using masterworker::TaskRequest;
using masterworker::TaskReply;
using masterworker::MapReduce;

class MapReduceClient {
public:
	MapReduceClient(std::shared_ptr<Channel> channel)
		: stub_(MapReduce::NewStub(channel)) {}

  	// Assambles the client's payload, sends it and presents the response back
  	// from the server.
  	bool AssignTasks(const TaskRequest& t, vector<string>& map_results) {
    		// Data we are sending to the server.
    		TaskRequest request;
    		request.set_file_name(t.file_name());
		request.set_start(t.start());
		request.set_end(t.end());
		request.set_n_r(t.n_r());
		request.set_type(t.type());
		request.set_output_dir(t.output_dir());
		request.set_user_id(t.user_id());
		request.set_file_id(t.file_id());

    		// Container for the data we expect from the server.
    		TaskReply reply;

    		// Context for the client. It could be used to convey extra information to
    		// the server and/or tweak certain RPC behaviors.
    		ClientContext context;

    		// The actual RPC.
    		Status status = stub_->AssignTasks(&context, request, &reply);

    		// Act upon its status.
		if (!status.ok()) {
			std::cout << status.error_code() << ": " << status.error_message() << std::endl;
			return false;
		}

		int i = 0;
		for (const auto result : reply.return_files()) {
			map_results[i] = result;
			i++;
		}
		return true;
 	 }

private:
  	std::unique_ptr<MapReduce::Stub> stub_;
};

enum t_states{ to_do, in_process, done };

enum w_states{ available, busy };

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		void barrier(){
			int local = !sense;

			mt_sense.lock();
			count--;
			mt_sense.unlock();

			if(count == 0){
				count = n_workers;
				sense = local;
			} else {
				while(sense != local);
			}
		}

		static void map_reduce_task(Master *m, int w_id){
			MapReduceClient mr(grpc::CreateChannel(m->ipaddr[w_id], grpc::InsecureChannelCredentials()));
			//assign map tasks
			while(true){
				for(int i = 0; i < m->M; i++){
					m->mt.lock();
					if(m->m_flags[i] == to_do){
						m->m_flags[i] = in_process;
						m->mt.unlock();
						for(int j = 0; j < m->shards[i].files.size(); j++){
							TaskRequest t;
							t.set_file_name(m->shards[i].files[j]);
							t.set_start(m->shards[i].starts[j]);
							t.set_end(m->shards[i].ends[j]);
							t.set_n_r(m->R);
							t.set_type("m");
							t.set_output_dir(m->output_dir);
							t.set_user_id(m->user_id);
							if(mr.AssignTasks(t, m->inter_files)){
								cout<<"Map task "<<i<<" are done by worker "<<w_id<<endl;
								m->mt.lock();
								m->m_flags[i] = done;
								m->mt.unlock();
							} else {
								cout<<" Map tasks "<<i<<" failed."<<endl;
								this_thread::sleep_for(chrono::milliseconds(5));
							}
						}
					} else {
						m->mt.unlock();
					}
				}
				bool check = true;
				for(int i = 0; i < m->M; i++){
					if(m->m_flags[i] != done){
						check = false;
					}
				}
				if(check){
					break;
				}
			}

			m->barrier();

			//assign reduce tasks
			while(true){
				for(int i = 0; i < m->R; i++){
					m->mt.lock();
					if(m->r_flags[i] == to_do){
						m->r_flags[i] = in_process;
						m->mt.unlock();
						TaskRequest t;
						t.set_file_name(m->inter_files[i]);
						t.set_start(0);
						t.set_end(0);
						t.set_n_r(m->R);
						t.set_type("r");
						t.set_output_dir(m->output_dir);
						t.set_user_id(m->user_id);
						t.set_file_id(i);
						if(mr.AssignTasks(t, m->output_files)){
							cout<<"Reduce task "<<i<<" are done by worker "<<w_id<<endl;
							m->mt.lock();
							m->r_flags[i] = done;
							m->mt.unlock();
						} else {
							cout<<" Reduce tasks "<<i<<" failed."<<endl;
							this_thread::sleep_for(chrono::milliseconds(5));
						}
					} else {
						m->mt.unlock();
					}
				}
				bool check = true;
				for(int i = 0; i < m->R; i++){
					if(m->r_flags[i] != done){
						check = false;
					}
				}
				if(check){
					break;
				}
			}
		}

		int M, n_workers, R, count;
		vector<string> ipaddr;
		vector<string> inter_files, output_files;
		vector<FileShard> shards;
		mutex mt, mt_sense;
		string output_dir, user_id;
		vector<t_states> m_flags, r_flags;
		vector<w_states> w_flags;
		bool sense;
};

/*bool map_task(int job_n, string addr, FileShard shard, int R, string t_type, string output_dir, string user_id, vector<string>& return_files, vector<bool>& flags) {
	MapReduceClient mr(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
	for(int i = 0; i < shard.files.size(); i++){
		//cout<<"checking 1==="<<endl;
		TaskRequest t;
		//cout<<shard.files[i]<<endl;
		t.set_file_name(shard.files[i]);
		t.set_start(shard.starts[i]);
		t.set_end(shard.ends[i]);
		t.set_n_r(R);
		t.set_type(t_type);
		t.set_output_dir(output_dir);
		t.set_user_id(user_id);
		if(mr.AssignTasks(t, return_files)){
			cout<<job_n<<" Map tasks are done!"<<endl;
			flags[job_n] = true;
			return true;
		} else {
			cout<<job_n<<" Map tasks failed."<<endl;
			return false;
		}
	}
}

bool reduce_task(int job_n, string addr, string input_file, int R, string t_type, string output_dir, string user_id, vector<string>& return_files, vector<bool>& flags) {
	MapReduceClient mr(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials()));
	TaskRequest t;
	t.set_file_name(input_file);
	t.set_start(0);
	t.set_end(0);
	t.set_n_r(job_n);
	t.set_type(t_type);
	t.set_output_dir(output_dir);
	t.set_user_id(user_id);
	if(mr.AssignTasks(t, return_files)){
		cout<<job_n<<" Reduce tasks are done!"<<endl;
		flags[job_n] = true;
		return true;
	} else {
		cout<<job_n<<" Reduce tasks failed."<<endl;
		return false;
	}
}*/

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) {
	M = file_shards.size();
	n_workers = mr_spec.n_workers;
	for(int i = 0; i < mr_spec.worker_ipaddr.size(); i++){
		ipaddr.push_back(mr_spec.worker_ipaddr[i]);
	}

	for(int i = 0; i < M; i++){
		FileShard shard;
		for(int j = 0; j < file_shards[i].files.size(); j++){
			shard.files.push_back(file_shards[i].files[j]);
			shard.starts.push_back(file_shards[i].starts[j]);
			shard.ends.push_back(file_shards[i].ends[j]);
			//cout<<"shard:"<<i<<" File:"<<file_shards[i].files[j]<<" start:"<<file_shards[i].starts[j]<<" end:"<<file_shards[i].ends[j]<<endl;
		}
		shards.push_back(shard);
	}

	R = mr_spec.n_output_files;
	output_dir = mr_spec.output_dir;
	user_id = mr_spec.user_id;
	inter_files.resize(R);
	output_files.resize(R);
	w_flags.resize(n_workers);
	for(int i = 0; i < n_workers; i++){
		w_flags[i] = available;
	}
	m_flags.resize(M); 
	for(int i = 0; i < M; i++){
		m_flags[i] = to_do;
	}
	r_flags.resize(R); 
	for(int i = 0; i < R; i++){
		r_flags[i] = to_do;
	}
	sense = true;
	count = n_workers;
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	vector<thread> threads(n_workers);

	for(int i = 0; i < n_workers; i++){
		threads[i] = thread(map_reduce_task, this, i);
	}

	for(int i = 0; i < n_workers; i++){
		threads[i].join();
	}

	return true;
}
