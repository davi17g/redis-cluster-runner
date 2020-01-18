import os
import signal
import os.path
import argparse
import threading
import subprocess

ROOT_DIR = os.path.basename(__file__).split(".")[0]


def create_folders(start_port, cluster_size):

    for port in range(start_port, start_port + cluster_size):
        os.makedirs(os.path.join(ROOT_DIR, str(port)), exist_ok=True)


def create_config_file(port):
    filename = "nodes-%s.conf" % port
    cc = "port {port}\ncluster-enabled yes\n" \
         "cluster-config-file nodes-{port}.conf\n" \
         "cluster-node-timeout 5000\n" \
         "appendonly yes".format(port=port)

    with open(os.path.join(ROOT_DIR, filename), 'w') as f:
        f.write(cc)


def create_config_files(start_port, cluster_size):

    for port in range(start_port, start_port + cluster_size):
        create_config_file(str(port))


class RedisRunner(threading.Thread):
    def __init__(self, port):
        self._port = port
        self._p = None
        super().__init__(name='executor-{}'.format(self._port), daemon=False)

    def run(self) -> None:
        cmd = 'cd {port} && redis-server ../nodes-{port}.conf'.format(port=self._port)
        self._p = subprocess.Popen([cmd], shell=True, stdout=subprocess.DEVNULL, preexec_fn=os.setsid)
        self._p.communicate()

    def close(self):
        os.killpg(os.getpgid(self._p.pid), signal.SIGTERM)


def main():
    parser = argparse.ArgumentParser(description="Script for running Redis Cluster")
    parser.add_argument('--size', help="Specify cluster size", type=int, default=6)
    parser.add_argument('--starting-port', help="Specify Starting port of the cluster", type=int, default=6379)
    args = parser.parse_args()

    threads = []
    addrs = ""

    try:
        print("Cluster is starting...")
        create_folders(args.starting_port, args.size)
        create_config_files(args.starting_port, args.size)
        os.chdir(ROOT_DIR)
        for port in range(args.starting_port, args.starting_port + args.size):
            addrs += "127.0.0.1:{} ".format(port)
            thread = RedisRunner(port)
            thread.start()
            threads.append(thread)
        print("Cluster is ready!")
        cmd = "redis-cli --replicas 1 {}".format(addrs)
        print(subprocess.check_output([cmd], shell=True))

        print("Cluster is Running...")
        [thread.join() for thread in threads]
    except KeyboardInterrupt:
        print("Cluster is Stopping...")
        [thread.close() for thread in threads]
        print("Canning up")


if __name__ == '__main__':
    main()