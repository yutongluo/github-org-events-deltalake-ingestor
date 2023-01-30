from prefect import flow, task
from os import remove
from os.path import splitext
import fcntl
import asyncio


@task(name="spark-submit-ingest", task_run_name="ingest-for-{org}", retries=1, retry_delay_seconds=30)
async def ingest_org(org):
    proc = await asyncio.create_subprocess_exec(
        '/home/azadmin/spark-3.3.1-bin-hadoop3/bin/spark-submit', '--packages', 'io.delta:delta-core_2.12:2.2.0',
        '--class', 'SparkIngestMain', '--master', 'local[2]', '/home/azadmin/github-events-ingest-1.0-SNAPSHOT.jar',
        '-e', 'prod', '-o', org,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        raise IOError(f"Spark submit failed!\nSTDOUT\n{stdout.decode()}\nERR:\n{stderr.decode()}")


@flow(name="ingest-org-events", timeout_seconds=1800)
async def ingest():
    orgs = ["microsoft", "google", "apple", "twitter", "facebook", "alibaba"]
    for org in orgs:
        await ingest_org(org)


async def main():
    lock_file = '{}.lock'.format(splitext(__file__)[0])
    with open(lock_file, 'w') as lock_fp:
        try:
            fcntl.flock(lock_fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except IOError:
            print('Script has been running.')
            return
        else:
            await ingest()
    remove(lock_file)


asyncio.run(main())
