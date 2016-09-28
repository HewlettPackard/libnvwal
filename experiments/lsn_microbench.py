import os
import re
import sh

BIN="/dev/shm/volos/libnvwal/build/experiments/lsn_microbench"
DISK_DIR="/dev/shm/libnvwal/build/microbench/disk_root"
NVM_DIR="/mnt/nvm/pmem0/libnvwal/microbench/nv_root"
#DISK_DIR="/dev/shm/libnvwal/build/microbench/disk_root"
#NVM_DIR="/dev/shm/libnvwal/build/microbench/nv_root"

def run_lsn_workload(nthreads, nops, segment_size, writer_buffer_size, nlogrec, logrec_size):
  run = sh.Command(BIN)
  nv_quota = segment_size * 8
  out = run("-disk_folder", DISK_DIR, 
            "-nvm_folder", NVM_DIR, 
            "-nthreads", nthreads, 
            "-nops", nops, 
            "-segment_size", segment_size, 
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
  

for segment_size_pow in range(17, 20):
  segment_size = 2 ** segment_size_pow
  print "\"segment size = %d\"" % segment_size
  for nlogrec in [1,2,4,8,16,32,64]:
    nops = 1000000
    writer_buffer_size = 128*1024
    logrec_size = 128
    duration_us = run_lsn_workload(1, nops, segment_size, writer_buffer_size, nlogrec, logrec_size)
    total_bytes = nops * nlogrec * logrec_size
    throughput = total_bytes * 1000000 / duration_us 
    print nlogrec, throughput
    print 
    print
