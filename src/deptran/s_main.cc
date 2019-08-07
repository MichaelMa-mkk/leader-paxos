#include "__dep__.h"
#include "benchmark_control_rpc.h"
#include "client_worker.h"
#include "frame.h"
#include "scheduler.h"
// #include "server_worker.h"
#include "config.h"
#include "coordinator.h"
#include "service.h"

#include "classic/tpc_command.h"
#include "classic/tx.h"
#include "occ/tx.h"
#include "multi_paxos/scheduler.h"

using namespace janus;

// static vector<ServerWorker> svr_workers_g = {};
vector<unique_ptr<ClientWorker>> client_workers_g = {};

rrr::PollMgr* svr_poll_mgr_ = nullptr;
vector<rrr::Service*> services_ = {};
rrr::Server* rpc_server_ = nullptr;
base::ThreadPool* thread_pool_g = nullptr;

rrr::PollMgr* svr_hb_poll_mgr_g = nullptr;
ServerControlServiceImpl* scsi_ = nullptr;
rrr::Server* hb_rpc_server_ = nullptr;
base::ThreadPool* hb_thread_pool_g = nullptr;

Frame* rep_frame_ = nullptr;
Config::SiteInfo* site_info_ = nullptr;
Scheduler* rep_sched_ = nullptr;

rrr::Mutex finish_mutex{};
rrr::CondVar finish_cond{};
uint32_t n_current = 0;

Communicator* rep_commo_ = nullptr;
static const uint32_t CtrlPortDelta = 10000;

static int volatile xx =
    MarshallDeputy::RegInitializer(MarshallDeputy::CONTAINER_CMD,
                                     [] () -> Marshallable* {
                                       return new CmdData;
                                     });

void check_current_path() {
  auto path = boost::filesystem::current_path();
  Log_info("PWD : %s", path.string().c_str());
}

Coordinator* CreateRepCoord() {
  Coordinator* coord;
  static cooid_t cid = 0;
  int32_t benchmark = 0;
  static id_t id = 0;
  verify(rep_frame_ != nullptr);
  coord = rep_frame_->CreateCoordinator(cid++,
                                        Config::GetConfig(),
                                        benchmark,
                                        nullptr,
                                        id++,
                                        nullptr);
  coord->par_id_ = site_info_->partition_id_;
  coord->loc_id_ = site_info_->locale_id;
  return coord;
}

void Next(Marshallable& cmd) {
  finish_mutex.lock();
  n_current--;
  if (n_current == 0){
    finish_cond.signal();
  }
  finish_mutex.unlock();
}

void server_launch_worker(vector<Config::SiteInfo>& server_sites) {
  auto config = Config::GetConfig();
  Log_info("server enabled, number of sites: %d", server_sites.size());

  int i = 0;
  vector<std::thread> setup_ths;
  for (auto& site_info : server_sites) {
    setup_ths.push_back(std::thread([&site_info, &i, &config]() {
      Log_info("launching site: %x, bind address %s",
               site_info.id,
               site_info.GetBindAddress().c_str());
      site_info_ = const_cast<Config::SiteInfo*>(&config->SiteById(site_info.id));

      rep_frame_ = Frame::GetFrame(config->replica_proto_);
      Log_debug("################# replica_proto_: %d", config->replica_proto_);
      rep_frame_->site_info_ = site_info_;
      rep_sched_ = rep_frame_->CreateScheduler();
      // rep_sched_->txn_reg_ = tx_reg_;
      rep_sched_->loc_id_ = site_info_->locale_id;
      // ----------------------callback------------------
      rep_sched_->RegLearnerAction(std::bind(Next,
                                             std::placeholders::_1));

      // start server service
      std::string bind_addr = site_info_->GetBindAddress();
      int n_io_threads = 1;
      svr_poll_mgr_ = new rrr::PollMgr(n_io_threads);
      if (rep_frame_ != nullptr) {
        services_ = rep_frame_->CreateRpcServices(site_info_->id,
                                                rep_sched_,
                                                svr_poll_mgr_,
                                                scsi_);
      }
      uint32_t num_threads = 1;
      thread_pool_g = new base::ThreadPool(num_threads);

      // init rrr::Server
      rpc_server_ = new rrr::Server(svr_poll_mgr_, thread_pool_g);

      // reg services
      for (auto service : services_) {
        rpc_server_->reg(service);
      }

      // start rpc server
      Log_debug("starting server at %s", bind_addr.c_str());
      int ret = rpc_server_->start(bind_addr.c_str());
      if (ret != 0) {
        Log_fatal("server launch failed.");
      }

      Log_info("Server %s ready at %s",
               site_info_->name.c_str(),
               bind_addr.c_str());

      verify(svr_poll_mgr_ != nullptr);
      if (rep_frame_) {
        rep_commo_ = rep_frame_->CreateCommo();
        if (rep_commo_) {
          rep_commo_->loc_id_ = site_info_->locale_id;
        }
        rep_sched_->commo_ = rep_commo_;
      }

      Log_info("site %d launched!", (int)site_info.id);
    }));
  }
  Log_info("waiting for server setup threads.");
  for (auto& th : setup_ths) {
    th.join();
  }
  Log_info("done waiting for server setup threads.");

  // for (ServerWorker& worker : svr_workers_g) {
  // start communicator after all servers are running
  // setup communication between controller script
  bool hb = Config::GetConfig()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::GetConfig()->get_ctrl_timeout();
  scsi_ = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
  svr_hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  hb_thread_pool_g = new rrr::ThreadPool(1);
  hb_rpc_server_ = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  hb_rpc_server_->reg(scsi_);

  auto port = site_info_->port + CtrlPortDelta;
  std::string addr_port = std::string("0.0.0.0:") +
                          std::to_string(port);
  hb_rpc_server_->start(addr_port.c_str());
  if (hb_rpc_server_ != nullptr) {
    // Log_info("notify ready to control script for %s", bind_addr.c_str());
    scsi_->set_ready();
  }
  Log_info("heartbeat setup for %s on %s",
           site_info_->name.c_str(), addr_port.c_str());
  // }
  Log_info("server workers' communicators setup");
}

void submit(shared_ptr<Marshallable> sp_m) {
  finish_mutex.lock();
  n_current++;
  finish_mutex.unlock();
  CreateRepCoord()->Submit(sp_m);
}

int main(int argc, char* argv[]) {
  check_current_path();
  Log_info("starting process %ld", getpid());

  int ret = Config::CreateConfig(argc, argv);
  if (ret != SUCCESS) {
    Log_fatal("Read config failed");
    return ret;
  }

  auto server_infos = Config::GetConfig()->GetMyServers();
  if (server_infos.size() > 0) {
    server_launch_worker(server_infos);
  }

  // TODO
  // auto sp_tx = shared_ptr<TxClassic>(new TxOcc(100, 1, rep_sched_));
  // sp_tx->cmd_ = shared_ptr<CmdData>(new CmdData());
  auto sp_tx = make_shared<TxClassic>(100, 1, rep_sched_);
  sp_tx->cmd_ = make_shared<CmdData>();

  auto sp_prepare_cmd = std::make_shared<TpcPrepareCommand>();
  sp_prepare_cmd->tx_id_ = 0;
  sp_prepare_cmd->cmd_ = sp_tx->cmd_;
  auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_cmd);
  // Coroutine::CreateRun([=]() { submit(sp_m); });
  submit(sp_m);
  finish_mutex.lock();
  while (n_current > 0) {
    Log_debug("wait for prepare command replicated %d", n_current);
    finish_cond.wait(finish_mutex);
  }
  finish_mutex.unlock();
  // Log_debug("wait for prepare command replicated");
  // sp_tx->ev_prepare_.Wait();
  // Log_debug("finished prepare command replication");

  // for (auto& worker : svr_workers_g) {
  if (hb_rpc_server_ != nullptr) {
    scsi_->server_shutdown();
    delete hb_rpc_server_;
    delete scsi_;
    svr_hb_poll_mgr_g->release();
    hb_thread_pool_g->release();

    for (auto service : services_) {
      if (DepTranServiceImpl* s = dynamic_cast<DepTranServiceImpl*>(service)) {
        auto& recorder = s->recorder_;
        if (recorder) {
          auto n_flush_avg_ = recorder->stat_cnt_.peek().avg_;
          auto sz_flush_avg_ = recorder->stat_sz_.peek().avg_;
          Log::info("Log to disk, average log per flush: %lld,"
                    " average size per flush: %lld",
                    n_flush_avg_, sz_flush_avg_);
        }
      }
    }
  }
  // }
  Log_info("all server workers have shut down.");

  fflush(stderr);
  fflush(stdout);

  // for (auto& worker : svr_workers_g) {
  Log_debug("deleting services, num: %d", services_.size());
  delete rpc_server_;
  for (auto service : services_) {
    delete service;
  }
  thread_pool_g->release();
  svr_poll_mgr_->release();
  // }

  RandomGenerator::destroy();
  Config::DestroyConfig();

  Log_debug("exit process.");

  return 0;
}
