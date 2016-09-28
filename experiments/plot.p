set xlabel "number of log records (log_record_size == 128 bytes)"
set ylabel "throughput (MB/s)"

set terminal pdf
set output 'out.pdf'

#plot  "out.dat" using 1:2 w lines ls 1
plot "out.dat" index 0 u 1:2 w lines ls 1 title columnheader(1),\
     "out.dat" index 1 u 1:2 w lines ls 2 title columnheader(1),\
     "out.dat" index 2 u 1:2 w lines ls 3 title columnheader(1)
