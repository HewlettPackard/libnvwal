import os
import re
import sh

BIN="/tmp/build/experiments/epoch_microbench"
DISK_DIR="/tmp/libnvwal/disk_root"
#DISK_DIR="/dev/shm/libnvwal/disk_root"
NVM_DIR="/dev/shm/libnvwal/nv_root"
#BIN="/dev/shm/volos/libnvwal/build/experiments/lsn_microbench"
#DISK_DIR="/dev/shm/libnvwal/build/microbench/disk_root"
#NVM_DIR="/mnt/nvm/pmem0/libnvwal/microbench/nv_root"
#DISK_DIR="/dev/shm/libnvwal/build/microbench/disk_root"
#NVM_DIR="/dev/shm/libnvwal/build/microbench/nv_root"

def run_lsn_workload(nwriters, nthreads, nops, segment_size, mds_page_size, writer_buffer_size, nlogrec, logrec_size):
  run = sh.Command(BIN)
  nv_quota = segment_size * 8
  out = run("-disk_folder", DISK_DIR, 
            "-nvm_folder", NVM_DIR, 
            "-nwriters", nwriters, 
            "-nthreads", nthreads, 
            "-nops", nops, 
            "-segment_size", segment_size, 
            "-mds_page_size", mds_page_size,
            "-writer_buffer_size", writer_buffer_size,
            "-nv_quota", nv_quota, 
            "-nlogrec", nlogrec, 
            "-logrec_size", logrec_size)

  for l in out.split('\n'):
    if re.search("workload_duration", l):
      m = re.match("workload_duration (.+) us", l)
      duration = float(m.group(1))
      return duration

if not os.path.exists(DISK_DIR):
  os.makedirs(DISK_DIR)
if not os.path.exists(NVM_DIR):
  os.makedirs(NVM_DIR)
  
nops = 10000
nwriters = [1,8,32]
nlogrec_range = [1]
#nlogrec_range = [1,2,4,8,16,32,64]
logrec_size = 128
segment_size_range = map(lambda x:2**x, range(20,21))
mds_page_size = 1024*1024

for segment_size in segment_size_range:
  print "\"segment size = %d\"" % segment_size
  for nlogrec in nlogrec_range:
    for nwriters_val in nwriters:
      writer_buffer_size = 128*1024
      duration_us = run_lsn_workload(nwriters_val, 1, nops, segment_size, mds_page_size, writer_buffer_size, nlogrec, logrec_size)
      total_bytes = nops * nlogrec * logrec_size
      throughput = total_bytes * 1000000 / duration_us 
      latency = duration_us / nops
      print nlogrec, throughput, latency
      print 
      print
