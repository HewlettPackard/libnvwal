To measure disk fsync latency:

 ```
 cd $DISK_PATH
 sysbench --test=fileio --file-test-mode=rndwr --file-fsync-all=on --file-block-size=512 --max-time=10 prepare
 sysbench --test=fileio --file-test-mode=rndwr --file-fsync-all=on --file-block-size=512 --max-time=10 run
 ```

To measure disk bandwidth:

 ```
 cd $DISK_PATH
 sysbench --test=fileio --max-time=10 --file-test-mode=seqwr prepare
 sysbench --test=fileio --max-time=10 --file-test-mode=seqwr run
 ```
