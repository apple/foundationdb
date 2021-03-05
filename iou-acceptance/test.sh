pkill -9 -f fdbserver
sleep 1
sudo -v

# Keep-alive: update existing sudo time stamp if set, otherwise do nothing.
while true; do
sudo -n true
sleep 60
kill -0 "$$" || exit
done 2>/dev/null &

bash asyncfile.sh > file.out 2>&1

bash rw.sh > rw.out 2>&1
