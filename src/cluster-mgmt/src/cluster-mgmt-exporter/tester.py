from typing import Any

import sshtunnel
from absl import app, flags, logging
from system_client import system_client

FLAGS = flags.FLAGS
flags.DEFINE_string('ssh_host', 'systemf310', 'SSH hostname and username')
flags.DEFINE_integer('local_port', 8080, 'Local port for SSH tunnel')
flags.DEFINE_integer('remote_port', 8080, 'Remote port for SSH tunnel')
flags.DEFINE_string('remote_host', 'localhost', 'Remote host for SSH tunnel')
flags.DEFINE_string('ssh_username', 'admin', 'SSH username')
flags.DEFINE_string('ssh_password', 'admin', 'SSH password')


def main(unused_argv: Any) -> None:
    with sshtunnel.open_tunnel(
            (FLAGS.ssh_host, 22),
            ssh_username=FLAGS.ssh_username,
            ssh_password=FLAGS.ssh_password,
            remote_bind_address=(FLAGS.remote_host, FLAGS.remote_port),
            local_bind_address=("localhost", FLAGS.local_port)
    ):
        logging.info("SSH Tunnel established")

        grpc_client = system_client.SystemClient(
            "localhost", FLAGS.local_port)

        try:
            # response = grpc_client.dev_stats_list()
            # logging.info(f"Received response: {response}")
            counter = 0
            logging.info("Starting stream")
            for response in grpc_client.dev_stats_show(
                    "2024-05-23T00:00:00",
                    end_time="2024-05-23T00:00:30",
                    counters=["*:NONE:*"],
                    max_data_points=None):
                logging.info(f"{counter}: Received response. "
                             f"Number of datapoints: {len(response.datapoints)}")
                counter += 1
            logging.info(f"DONE, {counter}")
        except system_client.SystemClientGrpcError as e:
            logging.error(f"SystemClient gRPC call failed: {str(e)}")


if __name__ == "__main__":
    logging.set_verbosity(logging.INFO)
    app.run(main)
