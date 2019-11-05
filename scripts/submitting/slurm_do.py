#! /usr/bin/env python

from random     import random
try:
    from string import lowercase
except ImportError:
    from string import ascii_lowercase as lowercase
from time       import sleep
from argparse   import ArgumentParser, RawTextHelpFormatter
from subprocess import Popen, PIPE
from getpass    import getuser
from sys        import stdout, stderr
from os.path    import exists, expanduser, join
from os         import mkdir

################################################################################
# GLOBALS
LOGPATH = expanduser('~') + '/queue/%s/'
WHO     = getuser()

OUT = '{path}/{job_name}{job_num}.out'
ERR = '{path}/{job_name}{job_num}.err'

SCRIPT  = """\
#!/bin/bash

#SBATCH --job-name="{{array}}__{{job_name}}{{job_num}}"
#SBATCH --output={out}
#SBATCH --error={err}
#SBATCH --ntasks=1
{{extra}}

{{cmd}}

"""
################################################################################

def count_jobs(monitor_var='PENDING'):
    """
    Counts the number of pending jobs
    """
    monitor_var = 'PENDING,RUNNING' if monitor_var =='RUNNING' else monitor_var
    cmd = 'squeue -o "%%u " -u %s -h -t %s | wc -l'
    return int(Popen(cmd % (WHO, monitor_var),
                     shell=True, stdout=PIPE).communicate()[0])


def big_sleep(intime, time, monitor_var, wait_jobs):
    """
    checks if there is not more than 900 pending jobs
    otherwise, waits.
    """
    firstpause = True
    while count_jobs(monitor_var) > wait_jobs:
        if firstpause:
            stderr.write('-' * 37)
            stderr.write('> Big pause')
            firstpause = False
        stderr.write('.')
        stderr.flush()
        if intime:
            # wait 10 minutes
            sleep(600)
        else:
            # wait one 20th of the maxtime set.
            sleep(sum([60**(2-i)*int(j) for i, j in
                       enumerate(time.split(':'))]) / 20)
    if not firstpause:
        stdout.write('\n')


def main():
    opts = get_options()
    if not opts.name:
        job_list = opts.infile.split('/')[-1].split('.')[0] + '_'
        job_list = job_list + ''.join([lowercase[int(random()*26)]
                                       for _ in range(10)])
    else:
        job_list = opts.name

    srt, end = int(opts.beg), int(opts.end)

    PATH = LOGPATH % (job_list)
    if not exists(PATH):
        mkdir(PATH)
    PATH = join(PATH, '%04d' % (0))
    if not exists(PATH):
        mkdir(PATH)

    stdout.write('JOB LIST NAME: %s\n' % job_list)
    stdout.write(' Log stored in: %s\n\n' % PATH)

    jobids = {}
    jobnum = 1
    total_jobs = len(open(opts.infile).readlines())

    for cmd in open(opts.infile):
        if end:
            if not srt <= jobnum <= end:
                jobnum += 1
                continue

        kwargs = {
            'cpus-per-task': opts.cpus,
            'chdir'        : opts.chdir if opts.chdir else PATH,
            'time'         : opts.time,
            'qos'          : opts.qos
        }

        # parse command
        name = ''
        depe = ''
        inargs = {}
        # and inside arguments
        if cmd.startswith('['):
            inargs =  dict(c.split(' ') for c in cmd[1:].split('] ')[0].strip().split(';'))
            if 'depe' in inargs:
                depe = map(int, inargs['depe'].split(','))
                del inargs['depe']
            if 'name' in inargs:
                name = inargs.get('name', '') + '_'
                del inargs['name']
            cmd  = cmd.split(']')[1].strip()
            kwargs.update(inargs)

        # if multiple of 100, checks if there is not more than 900 pending jobs
        # otherwise, waits
        if not jobnum % 100:
            PATH = LOGPATH % (job_list)
            big_sleep('time' in inargs, kwargs['time'], opts.monitor_var, opts.wait_jobs)
            PATH = join(PATH, '%04d' % (jobnum/100))
            if not exists(PATH):
                mkdir(PATH)

        # define priority class
        if kwargs['qos'] == 'debug' and int(kwargs['time'].split(':')[0]) > 2:
            raise Exception('ERROR: changed to bsc_ls, too long job')

        # define memory
        if opts.highmem:
            kwargs['constraint'] = 'highmem'

        # write job script
        out = open(join(PATH, 'jobscript_'+str(jobnum)+'.cmd'), 'w')

        # create extra options from command line and internal
        extra = ''.join('#SBATCH --{}={}\n'.format(k, kwargs[k]) for k in kwargs)

        out.write(SCRIPT.format(path=PATH, array=job_list, job_name=name,
                                job_num=jobnum, extra=extra, cmd=cmd))
        out.close()

        # define dependencies (this is passed outside job script)
        if depe != '':
            depe = ' -d afterok:' + ':'.join(str(jobids[str(dep + jobnum * (dep < 0))])
                                             for dep in depe)

        # in case we just want to write jobs cripts
        if opts.norun:
            stdout.write('wrote cmd file {1:<4} {0:27} {1:5}/{2:<5}\n'.format(depe, jobnum, total_jobs))
            jobids[str(jobnum)] = jobnum
            jobnum += 1
            continue

        # pause each 10 jobs launched
        if not jobnum % 10:
            stderr.write('pause...\n')
            sleep(1)

        # submit
        out, err = Popen('sbatch' + depe + ' ' +
                         join(PATH, 'jobscript_'+ str(jobnum)+'.cmd'),
                         shell=True, stdout=PIPE, stderr=PIPE).communicate()
        # writes submission info
        if err:
            stderr.write(err + '\n')
        if 'Submitted batch job' in out:
            stdout.write('{:27} {:11} {:5}/{:<5}\n'.format(out.strip(), depe,
                                                           jobnum, total_jobs))
            jobids[str(jobnum)] = out.split()[-1]
            if opts.no_cmd:
                Popen('rm -f ' + join(PATH, 'jobscript_'+ str(jobnum)+'.cmd'),
                      shell=True, stdout=PIPE, stderr=PIPE).communicate()
        jobnum += 1


def get_options():
    '''
    parse option from call
    '''
    parser = ArgumentParser(
        usage="%(prog)s -i file [options]",
        formatter_class=RawTextHelpFormatter,
        description="""
: :+-------------------------------------------------------------------------+
: :| Reads a list of jobs from input file and launch each in SLURM.          |`+
: :|                                                                         | |
: :|  * this script will not allow more than 1000 jobs to be PENDING, it     | |
: :|    will wait until this number falls bellow 900 to continue launching   | |
: :|                                                                         | |
: :|  * information about dependencies, job names or execution time can be   | |
: :|    specified inside the input file, at the startbeginning of each line, | |
: :|    as:                                                                  | |
: :|      [name joe_73;time 2:00:00;cpus-per-task 8;depe -1,23] echo hola    | |
: :|                                                                         | |
: :+-------------------------------------------------------------------------+ |
: : `-------/   /-------------------------------------------------------------`+
: :        /   /
: :       /   /
: :      /   /
: :     /   /
: :    /   /
: :   /   /
: :  /   /
: : /   /
: :/   /
: :   /
: :  /
: : /
: :/
: :"""
        )

    inp_args = parser.add_argument_group('Inputs')
    job_args = parser.add_argument_group('Job features')
    exe_args = parser.add_argument_group('Execution')
    log_args = parser.add_argument_group('Logging')

    inp_args.add_argument('-i', dest='infile', metavar="PATH",
                          help='path to command list file', required=True)
    inp_args.add_argument('--start', action='store',
                          dest='beg', default=0,
                          help=('[first] line number (from command list file)'
                                ' at which to start'''))
    inp_args.add_argument('--stop', action='store',
                          dest='end', default=0,
                          help=('[last] line number (from command list file)'
                                ' at which to stop'''))

    job_args.add_argument('--name', action='store',
                          dest='name', default=None,
                          help=('[Random string] Name of the array job based on'
                                ' input file name'))

    choices = ['bsc_ls', 'debug']
    job_args.add_argument('--qos', dest='qos',
                          default='bsc_ls', choices=choices,
                          help=("[%(default)s] " +
                                "Use a given QOS. Can be any of: %s" % choices))

    job_args.add_argument('--requeue', action='store_false', dest='requeue',
                          default=True,
                          help=('[%(default)s] slurm will requeue the job if it '
                              'died due to a node fail'))

    job_args.add_argument('--time', action='store',
                          dest='time', default='2:00:00',
                          help='''[%(default)s] Maximum run time allowed''')

    log_args.add_argument('--no_out', action='store_true', dest='no_out',
                          default=False, help='[%(default)s] Do not store '
                          'standard output')

    job_args.add_argument('--chdir', dest='chdir', metavar="PATH",
                          help='path from where to execute commands')

    log_args.add_argument('--no_err', action='store_true', dest='no_err',
                          default=False, help='[%(default)s] Do not store '
                          'standard error')

    log_args.add_argument('--no_cmd', action='store_true', dest='no_cmd',
                          default=False, help='[%(default)s] Remove cmd files '
                          'once job is launched')

    exe_args.add_argument('--norun', action='store_true', dest='norun',
                          default=False, help='[%(default)s] Do not run '
                          'sbatch')

    job_args.add_argument('--cpus', action='store',
                          dest='cpus', default='1', type=int,
                          help='''[%(default)s] Number of CPUs per tasks''')

    exe_args.add_argument('--monitor', action='store',
                          dest='monitor_var', default='PENDING',
                          choices=['PENDING', 'RUNNING'],
                          help='[%(default)s] monitor running or pending jobs '
                          'to limit launches')

    exe_args.add_argument('--wait_jobs', action='store', metavar='INT',
                          dest='wait_jobs', default=900, type=int,
                          help='[%(default)s] set the limit in number of jobs')

    job_args.add_argument('--high_memory', action='store_true',
                          dest='highmem', default=False,
                          help='''By default heach CPU has 2Gb of RAM memory, with this, it's 8Gb!''')

    opts = parser.parse_args()
    global SCRIPT
    SCRIPT = SCRIPT.format(out=('/dev/null' if opts.no_out else OUT),
                           err=('/dev/null' if opts.no_err else ERR))
    return opts


if __name__ == "__main__":
    exit(main())
