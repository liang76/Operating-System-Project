#pragma once

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include <map>

#include <grpc++/grpc++.h>

#include "masterworker.grpc.pb.h"

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using masterworker::TaskRequest;
using masterworker::TaskReply;
using masterworker::MapReduce;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		std::string ip_addr;
		
		// Logic and data behind the server's behavior.
		class MapReduceServiceImpl final : public MapReduce::Service {
			Status AssignTasks(ServerContext* context, const TaskRequest* request, TaskReply* reply) override {
				//std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
				if(request->type() == "m"){
					std::cout<<"Run Map task"<<std::endl;
					auto mapper = get_mapper_from_task_factory(request->user_id());
					mapper->impl_->set(request->output_dir(), request->n_r());
					std::ifstream file(request->file_name().c_str());
					if(file.is_open()){
						file.seekg(request->start());
						std::string line;
						int position = 0;
						while(getline(file, line) && position < request->end()){
							position = file.tellg();
							mapper->map(line);
						}
						file.close();
					} else {
						std::cout<<"worker: Unable to open input file"<<std::endl;
						return Status::CANCELLED;
					}
					std::vector<std::string> inter_files = mapper->impl_->get();
					for(int i = 0; i < inter_files.size(); i++){
						reply->add_return_files(inter_files[i]);
					}
				} else if(request->type() == "r"){
					std::cout<<"Run Reduce task"<<std::endl;
					auto reducer = get_reducer_from_task_factory(request->user_id());
					reducer->impl_->set(request->output_dir(), request->n_r(), request->file_id());
					std::map<std::string, std::vector<std::string>> mymap;
  					std::map<std::string, std::vector<std::string>>::iterator it;
					std::ifstream file(request->file_name().c_str());
					if(file.is_open()){
						std::string line;
						while(getline(file, line)){			
							std::string key = line.substr(0, line.find(","));
							std::string val = line.substr(line.find(",")+1, line.length()-line.find(",")-1);
							it = mymap.find(key);
							if(it != mymap.end()){
								it->second.push_back(val);
							} else {
								std::vector<std::string> value;
								value.push_back(val);
								mymap.insert(std::pair<std::string, std::vector<std::string>>(key, value));
							}
						}
						file.close();
						
						for(it = mymap.begin(); it != mymap.end(); ++it){
							reducer->reduce(it->first, it->second);
						}
					} else {
						std::cout<<"worker: Unable to open intermediate file"<<std::endl;
						return Status::CANCELLED;
					}
					std::vector<std::string> output_files = reducer->impl_->get();
					for(int i = 0; i < output_files.size(); i++){
						reply->add_return_files(output_files[i]);
					}
				} else {
					return Status::CANCELLED;
				}

				return Status::OK;
			}
		};
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port) {
	ip_addr = ip_addr_port;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks 
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and 
	BaseReduer's member BaseReducerInternal impl_ directly, 
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*MapReduceServiceImpl server;
  	server.Run(ip_addr);*/

	std::string server_address(ip_addr);
	MapReduceServiceImpl service;

	ServerBuilder builder;
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	// Register "service" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *synchronous* service.
	builder.RegisterService(&service);
	// Finally assemble the server.
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;

	// Wait for the server to shutdown. Note that some other thread must be
	// responsible for shutting down the server for this call to ever return.
	server->Wait();
	
	return true;
}
