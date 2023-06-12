echo "this is node $NODE_ID out of $NODE_NUM"
cd mamaserving && ./server
# cd mamaserving && env LD_PRELOAD="/gperftools/build/lib/libtcmalloc.so" HEAPCHECK=normal  ./server

# sleep 10
# apt update && apt install libc6-dbg
# export PATH=$PATH:/valgrind-3.21.0/bin/bin
# cd mamaserving && valgrind --leak-check=yes ./server

echo 'Exited'
# while [[ 1 ]]; do
#     sleep 10000
# done
