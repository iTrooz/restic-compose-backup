import logging
import os
import signal
import sys

from restic_compose_backup import utils

logger = logging.getLogger(__name__)

should_exit = False

def handle_signal(signum, frame):
    global should_exit
    logger.info(f"Received signal {signum}, shutting down gracefully...")
    should_exit = True

signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)


def run(image: str = None, command: str = None, volumes: dict = None,
        environment: dict = None, labels: dict = None, source_container_id: str = None):
    logger.info("Starting backup container")
    client = utils.docker_client()

    container = client.containers.run(
        image,
        command,
        labels=labels,
        # auto_remove=True,  # We remove the container further down
        detach=True,
        environment=environment + ['BACKUP_PROCESS_CONTAINER=true'],
        volumes=volumes,
        network_mode=f'container:{source_container_id}',  # Reuse original container's network stack.
        working_dir=os.getcwd(),
        tty=True,
    )

    logger.info("Backup process container: %s", container.name)
    log_generator = container.logs(stdout=True, stderr=True, stream=True, follow=True)

    def readlines(stream):
        """Read stream line by line, exit early if signal received"""
        while not should_exit:
            line = ""
            while not should_exit:
                try:
                    data = next(stream)
                    if isinstance(data, bytes):
                        line += data.decode()
                    elif isinstance(data, str):
                        line += data
                    if line.endswith('\n'):
                        break
                except StopIteration:
                    break
                except Exception as e:
                    logger.error(f"Exception reading log stream: {e}")
                    break
            if line:
                yield line.rstrip()
            else:
                break

    with open('backup.log', 'w') as fd:
        for line in readlines(log_generator):
            if should_exit:
                logger.info("Exiting log reading loop due to signal.")
                break
            fd.write(line)
            fd.write('\n')
            logger.info(line)

    if should_exit:
        try:
            logger.info("Stopping backup container due to signal...")
            container.stop(timeout=5)
        except Exception as e:
            logger.error(f"Error stopping container: {e}")

    container.reload()
    logger.debug("Container ExitCode %s", container.attrs['State']['ExitCode'])
    container.remove()

    if should_exit:
        logger.info("Exiting due to signal.")
        sys.exit(143)  # 128 + 15 (SIGTERM)

    return container.attrs['State']['ExitCode']
