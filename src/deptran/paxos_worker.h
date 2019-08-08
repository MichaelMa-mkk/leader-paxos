#pragma once

#include "__dep__.h"
#include "coordinator.h"
#include "benchmark_control_rpc.h"
#include "frame.h"
#include "scheduler.h"
#include "communicator.h"
#include "config.h"

namespace janus {

class PaxosWorker {
public:
  rrr::PollMgr* svr_poll_mgr_ = nullptr;
  vector<rrr::Service*> services_ = {};
  rrr::Server* rpc_server_ = nullptr;
  base::ThreadPool* thread_pool_g = nullptr;

  rrr::PollMgr* svr_hb_poll_mgr_g = nullptr;
  ServerControlServiceImpl* scsi_ = nullptr;
  rrr::Server* hb_rpc_server_ = nullptr;
  base::ThreadPool* hb_thread_pool_g = nullptr;

  Config::SiteInfo* site_info_ = nullptr;
  Frame* rep_frame_ = nullptr;
  Scheduler* rep_sched_ = nullptr;
  Communicator* rep_commo_ = nullptr;
  vector<Coordinator*> created_coordinators_{};

  rrr::Mutex finish_mutex{};
  rrr::CondVar finish_cond{};
  uint32_t n_current = 0;

  // PaxosWorker(int n_current_) : n_current(n_current_) {
  //   Log_debug("PaxosWorker constructor.");
  // }

  // PaxosWorker() = delete;

  ~PaxosWorker() {
    for (auto c : created_coordinators_) {
      delete c;
    }
  }

  void SetupHeartbeat();
  void SetupBase();
  void SetupService();
  void SetupCommo();
  void ShutDown();
  void Next(Marshallable&);

  static const uint32_t CtrlPortDelta = 10000;
  void WaitForShutdown();

  void SubmitExample();
  void Submit(shared_ptr<Marshallable> sp_m);
};

} // namespace janus