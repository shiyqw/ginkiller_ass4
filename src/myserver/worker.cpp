
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <iostream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

using namespace std;

const int pthread_num = 25;

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  std::ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

WorkQueue<Request_msg> work_queue;
WorkQueue<Request_msg> tellmenow_queue;
WorkQueue<Request_msg> projectidea_queue;

// Implements logic required by compareprimes command via multiple
// calls to execute_work.  This function fills in the appropriate
// response.
static void execute_compareprimes(const Request_msg& req, Response_msg& resp) {

    int n, index;
    // grab the four arguments defining the two ranges
    n = atoi(req.get_arg("n").c_str());
    index = atoi(req.get_arg("index").c_str());

    Request_msg dummy_req(0);
    Response_msg dummy_resp(0);
    create_computeprimes_req(dummy_req, n);
    execute_work(dummy_req, dummy_resp);
    string count = dummy_resp.get_response();
    resp.set_response(to_string(index) + " " + count);
}

void * mytellmenow(void * dump) {
  while(true) {
      Request_msg req = tellmenow_queue.get_work();
      Response_msg resp(req.get_tag());

      DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

      double startTime = CycleTimer::currentSeconds();

      execute_work(req, resp);

      double dt = CycleTimer::currentSeconds() - startTime;

      DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

      worker_send_response(resp);
  }
  return NULL;
}

void * myprojectidea(void * dump) {
  while(true) {
      Request_msg req = projectidea_queue.get_work();
      Response_msg resp(req.get_tag());

      DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

      double startTime = CycleTimer::currentSeconds();

      execute_work(req, resp);

      double dt = CycleTimer::currentSeconds() - startTime;

      DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

      worker_send_response(resp);
  }
  return NULL;
}

void * myexecute(void * dump) {
  while(true) {
      Request_msg req = work_queue.get_work();
      Response_msg resp(req.get_tag());

      DLOG(INFO) << "Worker got request: [" << req.get_tag() << ":" << req.get_request_string() << "]\n";

      double startTime = CycleTimer::currentSeconds();

      if (req.get_arg("cmd").compare("compareprimes") == 0) {
        execute_compareprimes(req, resp);
      } else {
        execute_work(req, resp);
      }

      double dt = CycleTimer::currentSeconds() - startTime;

      DLOG(INFO) << "Worker completed work in " << (1000.f * dt) << " ms (" << req.get_tag()  << ")\n";

      worker_send_response(resp);
  }
  return NULL;
}


void worker_node_init(const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  DLOG(INFO) << "**** Initializing worker: " << params.get_arg("name") << " ****\n";

  pthread_t threads[pthread_num];
  for(int i = 0; i < pthread_num; ++i) {
      pthread_create(&threads[i], NULL, myexecute, NULL);
  }
  pthread_t tellmenow_thread;
  pthread_t projectidea_thread;
  pthread_create(&projectidea_thread, NULL, myprojectidea, NULL);
  pthread_create(&tellmenow_thread, NULL, mytellmenow, NULL);

}

void worker_handle_request(const Request_msg& req) {
    if (req.get_arg("cmd").compare("tellmenow") == 0) {
        tellmenow_queue.put_work(req);
    } else if (req.get_arg("cmd").compare("projectidea") == 0) {
        projectidea_queue.put_work(req);
    } else {
        work_queue.put_work(req);
    }
}
