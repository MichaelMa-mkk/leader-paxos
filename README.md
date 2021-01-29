# Paxos  

## Quick start (with Ubuntu 16.04 or newer)

Install dependencies:

```
sudo apt-get update
sudo apt-get install -y \
    git \
    pkg-config \
    build-essential \
    clang \
    libapr1-dev libaprutil1-dev \
    libboost-all-dev \
    libyaml-cpp-dev \
    python-dev \
    python-pip \
    libgoogle-perftools-dev
sudo pip install -r requirements.txt
```

Get source code:
```
git clone --recursive https://github.com/NYU-NEWS/janus.git
```

Build:

```
./waf-2.0.18 configure build
```

## More
Check out the doc directory to find more about how to build the system on older or newer distros, how to run the system in a distributed setup, and how to generate figures in the paper, etc.
<!-- 
## Do some actual good
For every star collected on this project, I will make a $25 charity loan via [Kiva] (https://www.kiva.org/invitedby/gzcdm3147?utm_campaign=permurl-share-invite-normal&utm_medium=referral&utm_content=gzcdm3147&utm_source=mpaxos.com).
-->

### For Paxos

#### Potential Environment Problems
* `error while loading shared libraries: libdeptran_server.so: cannot open shared object file: No such file or directory`
    ```
    export LD_LIBRARY_PATH=$(pwd)/build
    ```

#### Multi-process paxos

```
make
python3 paxos-microbench.py -d 60 -f 'config/1c1s3r3p.yml' -f 'config/occ_paxos.yml' -t 30 -T 100000 -n 32 -P p3 -P p2 -P localhost
```

#### Multi-thread paxos

```
make
python3 paxos-microbench.py -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 30 -T 100000 -n 32 -P localhost
```

#### Others

run `python3 paxos-microbench.py -h` for help  
**MUST** add all process names in the config files  
see the contents in config files for more information about paxos partition and replication
for multi-process paxos, if some site fails to start, change the order of your `-P` options and try again

#### To See CPU Profiling

PS: There are 32 outstanding requests.

```
./waf-2.0.18 configure -p build
./build/microbench -d 60 -f 'config/1c1s3r1p.yml' -f 'config/occ_paxos.yml' -t 10 -T 5000 -n 32 -P localhost > ./log/proc-localhost.log
./pprof --pdf ./build/deptran_server process-localhost.prof > cpu.pdf
```
