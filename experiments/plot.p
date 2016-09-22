set xlabel "segment size (bytes)"
set ylabel "throughput (MB/s)"

set terminal pdf
set output 'out.pdf'

plot  "out.dat" using 1:2 w lines ls 1
