#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <deque>
#include <map>
#include <string>
#include <iostream>
#include <vector>

#include "server/messages.h"
#include "server/master.h"


using namespace std;

const int pthread_num = 18;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int max_num_workers;
  int num_pending_client_requests;
  int next_tag;
  
  deque<Request_msg> waitlist;
  map<int, Client_handle> clientMap;
  map<int, Request_msg> requestMap;

  // structure for countprime
  map<int, int> respNumMap;
  map<int, vector<int> > countListMap;

  map<string, Response_msg> cache;

  Worker_handle my_worker;
  Client_handle waiting_client;

} mstate;



void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  mstate.next_tag = 0;
  mstate.max_num_workers = max_workers;
  mstate.num_pending_client_requests = 0;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  // fire off a request for a new worker

  int tag = random();
  Request_msg req(tag);
  req.set_arg("name", "my worker 0");
  request_new_worker_node(req);

}

void handle_new_worker_online(Worker_handle worker_handle, int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.my_worker = worker_handle;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

string get_key(Request_msg & req) {
    string cmd = req.get_arg("cmd");
    if(!cmd.compare("countprimes")) {
        return string("N#") + req.get_arg("n");
    }

    if(!cmd.compare("418wisdom")) {
        return string("4#") + req.get_arg("x");
    } 

    if(!cmd.compare("compareprimes")) {
        return string("M#") + req.get_arg("n1") + ',' +
                              req.get_arg("n2") + ',' +
                              req.get_arg("n3") + ',' +
                              req.get_arg("n4");
    }

    if(!cmd.compare("tellmenow")) {
        return string("T#") + req.get_arg("x");
    }

    return "";
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.

  DLOG(INFO) << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;
  //cout << "Master received a response from a worker: [" << resp.get_tag() << ":" << resp.get_response() << "]" << std::endl;

  int tag = resp.get_tag();
  Client_handle client = mstate.clientMap[tag];
  Request_msg req = mstate.requestMap[tag];


  if(!req.get_arg("cmd").compare("compareprimes")) {
      string result = resp.get_response();
      int index = result[0]-'0';
      int count = atoi(result.substr(2).c_str());
      ++mstate.respNumMap[tag];
      mstate.countListMap[tag][index] = count;
      if(mstate.respNumMap[tag] == 4) {
          Response_msg new_resp;
          int c1 = mstate.countListMap[tag][1] - mstate.countListMap[tag][0];
          int c2 = mstate.countListMap[tag][3] - mstate.countListMap[tag][2];
          if (c1 > c2) {
              new_resp.set_response("There are more primes in first range.");
          } else {
              new_resp.set_response("There are more primes in second range.");
          }
          send_client_response(client, new_resp);
          mstate.countListMap.erase(tag);
          mstate.respNumMap.erase(tag);
          mstate.clientMap.erase(tag);
          mstate.requestMap.erase(tag);
      }

  } else {
      mstate.clientMap.erase(tag);
      mstate.requestMap.erase(tag);
      send_client_response(client, resp);
      // update cache
      string key = get_key(req);
      mstate.cache[key] = resp;
  }


  if(mstate.waitlist.size() == 0) {
      mstate.num_pending_client_requests--;
  } else {
      Request_msg new_worker_request = mstate.waitlist.front();
      mstate.waitlist.pop_front();
      send_request_to_worker(worker_handle, new_worker_request);
  }
}



void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  DLOG(INFO) << "Received request: " << client_req.get_request_string() << std::endl;

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (client_req.get_arg("cmd") == "lastrequest") {
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  }
  
  // The provided starter code cannot handle multiple pending client
  // requests.  The server returns an error message, and the checker
  // will mark the response as "incorrect"
  int tag = mstate.next_tag++;
  Request_msg worker_req(tag, client_req);
  mstate.clientMap[tag] = client_handle;
  mstate.requestMap[tag] = worker_req;

  string key = get_key(worker_req);
  //cout << tag << " has " << key << endl;

  if (mstate.cache.find(key) != mstate.cache.end()) {

      //cout << tag << " hit!" << endl;

      Response_msg resp = mstate.cache.find(key)->second;
      send_client_response(client_handle, resp);
      return;
  }


  string cmd = client_req.get_arg("cmd");
  if (!cmd.compare("compareprimes")) {
      int n[4];
      n[0] = atoi(worker_req.get_arg("n1").c_str());
      n[1] = atoi(worker_req.get_arg("n2").c_str());
      n[2] = atoi(worker_req.get_arg("n3").c_str());
      n[3] = atoi(worker_req.get_arg("n4").c_str());
      // initialize the sub-requests that have been completed
      //mstate.requestsMap[tag]->counts[4] = 0;
      for(int i = 0; i < 4; i++) {
          Request_msg new_req(tag);
          new_req.set_arg("cmd", "compareprimes");
          new_req.set_arg("n", to_string(n[i]));
          new_req.set_arg("index", to_string(i));
          mstate.waitlist.push_back(new_req);
      }
      mstate.respNumMap[tag] = 0;
      mstate.countListMap[tag] = vector<int>(4);

  } else if (!cmd.compare("tellmenow")) {
    //cout << "get tellmenow" << endl;
    send_request_to_worker(mstate.my_worker, worker_req);
    return;
  } else {
    mstate.waitlist.push_back(worker_req);
  }


  if (mstate.num_pending_client_requests != pthread_num) {
    Request_msg new_worker_request = mstate.waitlist.front();
    mstate.waitlist.pop_front();
    send_request_to_worker(mstate.my_worker, new_worker_request);
    mstate.num_pending_client_requests++;
    return;
  }
      
  return;

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.


  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.

}


void handle_tick() {

  // TODO: you may wish to take action here.  This method is called at
  // fixed time intervals, according to how you set 'tick_period' in
  // 'master_node_init'.

}

