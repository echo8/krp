# this is run in the github.com/techknowlogick/xgo container
# which allows us to easily do cross compilation

# install docker
curl -sfL https://get.docker.com | sh
echo "ulimits: $(ulimit -Sn):$(ulimit -Hn)"
sed -i 's/ulimit -Hn/# ulimit -Hn/g' /etc/init.d/docker
service docker start

# go to krp dir and run goreleaser
cd /tmp/krp/src
curl -sfL https://goreleaser.com/static/run | bash -s -- release --snapshot --clean
