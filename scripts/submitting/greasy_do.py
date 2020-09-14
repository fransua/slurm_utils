#! /usr/bin/env python
# -*- coding: UTF-8 -*-
"""
List of taskfiles, where every taskfile contains all jobs to be ru
The script is ready to be run in local, so it is only needed to change this
FILES line, and the taskfile path would still be OK!

The number of tasks is max 2400; when using >48 cpus, better to use multiples
of 48 to avoid loosing cores in every node!

The maximum running time is 48 hours, in the form HH:MM:SS
"""

__author__ = "François Serra"
__license__ = "GPL3"
__email__ = "serra.francois@gmail.com"
__date__ = "28/07/2020"

import os
import sys
from time import sleep
from argparse import ArgumentParser
from getpass import getuser


GREASY_CMD = """#!/bin/bash
#SBATCH --job-name={job_name}.{ngreasy}
#SBATCH --chdir=/gpfs/scratch/bsc08/{whoami}/tmp/
#SBATCH --output={rep}/greasy_{ngreasy:03}/{job_name}.{ngreasy}_%j.out
#SBATCH --error={rep}/greasy_{ngreasy:03}/{job_name}.{ngreasy}_%j.err
#SBATCH --cpus-per-task={cpus}
#SBATCH --ntasks={ntasks}
#SBATCH --time={time}
#SBATCH --qos={qos}

module load greasy
module load singularity

export GREASY_LOGFILE={rep}/greasy_{ngreasy:03}/lm_interfaces_{ngreasy}.log

/apps/GREASY/latest/INTEL/IMPI/bin/greasy {job_list}

"""

WHOAMI = getuser()


def main():
    """

    """
    opts = get_options()

    # Generating a greasy file to run every taskfile
    qpath = "{}/queue/{}".format(os.path.expanduser('~'), opts.job_name)
    total = sum(1 for _ in open(opts.fname))
    fh_jobs = open(opts.fname)
    for ngreasy in range(1, total // (opts.ntasks * opts.jobs_per_task) + 1 +
                         (total % (opts.ntasks * opts.jobs_per_task) != 0)):
        gpath = os.path.join(qpath, "greasy_{:03}".format(ngreasy))
        if not os.path.exists(gpath):
            os.mkdir(gpath)
        greasy_fname = os.path.join(gpath, "greasy_cmds_{}.txt".format(ngreasy))
        greasy_list = open(greasy_fname, "w")
        for _ in range(opts.ntasks * opts.jobs_per_task):
            try:
                job = next(fh_jobs)
            except StopIteration:
                break
            if job.strip() is '':
                break
            try:
                greasy_list.write(job.split("] ")[1])
            except IndexError:
                greasy_list.write(job)
        greasy_list.close()

        greasy_cmdf = open(os.path.join(gpath, "greasy_{}.cmd".format(ngreasy)), "w")
        greasy_cmdf.write(
            GREASY_CMD.format(
                job_list=greasy_fname,
                rep=qpath,
                job_name=opts.job_name,
                ntasks=opts.ntasks,
                ngreasy=ngreasy,
                cpus=opts.cpus,
                time=opts.time,
                qos=opts.qos,
                whoami=WHOAMI
            )
        )
        greasy_cmdf.close()
        os.system(
            "sbatch {}".format(os.path.join(gpath, "greasy_{}.cmd".format(ngreasy)))
        )
        if not ngreasy % 10:
            sleep(2)


def get_options():

    parser = ArgumentParser(
        description="""prepare and run greasy jobs from file with a list of
        jobs"""
    )

    parser.add_argument(
        "-i",
        type=str,
        required=True,
        dest="fname",
        help="Input file with one job per line",
    )

    parser.add_argument(
        "--name",
        type=str,
        dest="job_name",
        required=True,
        metavar="STR",
        help="""Root name of the greasy jobs, will use this to create
        log/commands folder""",
    )

    parser.add_argument(
        "-C",
        "--cpus",
        type=int,
        default=1,
        help="[%(default)s] number of cpus to be used for each task",
    )

    parser.add_argument(
        "-t",
        "--ntasks",
        type=int,
        default=144,
        metavar="INT",
        help="[%(default)s] number of tasks per greasy job",
    )

    parser.add_argument(
        "-j",
        "--jobs_per_task",
        type=int,
        default=4,
        metavar="INT",
        help="""[%(default)s] number of cpus allocated per task (and thus per
        job, as jobs run one after the other in the same task)""",
    )

    parser.add_argument(
        "-T", "--time", type=str, default="48:00:00", help="[%(default)s] time limit"
    )

    parser.add_argument(
        '--qos', type=str, default='bsc_ls',
        help='[%(default)s] QOS to use (choices: %(choices)s)', choices=['bsc_ls', 'debug'])
        
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    sys.exit(main())
