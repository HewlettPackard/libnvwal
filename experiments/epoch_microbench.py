import os
import re
import sh
import shutil

BIN="/home/volos/workspace/build/libnvwal/experiments/epoch_microbench"
DISK_DIR="/tmp/libnvwal/microbench/disk_root"
#DISK_DIR="/dev/shm/libnvwal/microbench/disk_root"
NVM_DIR="/mnt/nvm/pmem1/libnvwal/microbench/nv_root"

def cleanup():
  if os.path.exists(DISK_DIR):
    shutil.rmtree(DISK_DIR)
  if os.path.exists(NVM_DIR):
    shutil.rmtree(NVM_DIR)

  os.makedirs(DISK_DIR)
  os.makedirs(NVM_DIR)
  sh.sync()
 
def run_lsn_workload(nwriters, nthreads, nops, segment_size, mds_page_size, writer_buffer_size, nlogrec, logrec_size):
  # run command
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

  cleanup()

  for l in out.split('\n'):
    if re.search("workload_duration", l):
      m = re.match("workload_duration (.+) us", l)
      duration = float(m.group(1))
      return duration

sep = ','
runs = 5 
nops = 1000000
nwriters_range = [1,2,4,8,16,24,32,40,48]
#nwriters_range = [40,48]
nthreads = 1
nlogrec_range = [1,2,4,8]
#nlogrec_range = [4,8]
logrec_size = 128
segment_size_range = map(lambda x:2**x, range(20,21))

print sep.join(['segment_size', 'logrec_size', 'nwriters', 'latency_us', 'throughput_bytes'])
for r in range(runs):
  for segment_size in segment_size_range:
    mds_page_size = segment_size
    for nlogrec in nlogrec_range:
      for nwriters in nwriters_range:
        nops_per_thread = nops / nwriters/nthreads
        writer_buffer_size = 128*1024
        duration_us = run_lsn_workload(nwriters, nthreads, nops_per_thread, segment_size, mds_page_size, writer_buffer_size, nlogrec, logrec_size)
        total_bytes = nops_per_thread * nwriters * nthreads * nlogrec * logrec_size
        throughput = total_bytes * 1000000 / duration_us 
        latency = duration_us / nops_per_thread
        print sep.join([str(segment_size), str(nlogrec*logrec_size), str(nwriters), str(latency), str(throughput)])
