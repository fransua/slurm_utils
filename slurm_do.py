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
GROUP   = Popen('sacctmgr list user', shell=True,
              stdout=PIPE).communicate()[0].split()[-2]
GROUP   = GROUP[0].upper() + GROUP[1:-1] + GROUP[-1].upper()
OUT = '{path}/{job_name}_{job_num}.out'
ERR = '{path}/{job_name}_{job_num}.err'

SCRIPT  = """\
#!/bin/bash

#SBATCH --job-name="{{array}}__{{job_name}}_{{job_num}}"
#SBATCH --chdir={{path}}
#SBATCH --output={out}
#SBATCH --error={err}
#SBATCH --ntasks=1
#SBATCH --cpus-per-task={{cpus}}
#SBATCH --time={{time}}
#SBATCH --qos={{qos}}
{{requeuing}}

{{group_info}}

{{cmd}}

"""
WHERE   = """# @ features         = {0}{2}
# @ account          = {2}
# @ partition        = {1}
"""
################################################################################

def count_jobs(how='PENDING'):
    """
    Counts the number of pending jobs
    """
    how = 'PENDING,RUNNING' if how =='RUNNING' else how
    cmd = 'squeue -o "%%u " -u %s -h -t %s | wc -l'
    return int(Popen(cmd % (WHO, how),
                     shell=True, stdout=PIPE).communicate()[0])

def big_sleep(intime, time, how, wait_jobs):
    """
    checks if there is not more than 900 pending jobs
    otherwise, waits.
    """
    firstpause = True
    while count_jobs(how) > wait_jobs:
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
            if not (srt <= jobnum <= end):
                jobnum += 1
                continue
        # if multiple of 100, checks if there is not more than 900 pending jobs
        # otherwise, waits
        if not jobnum % 100:
            PATH = LOGPATH % (job_list)
            big_sleep(opts.intime, opts.time, opts.how, opts.wait_jobs)
            PATH = join(PATH, '%04d' % (jobnum/100))
            if not exists(PATH):
                mkdir(PATH)

        # parse command
        cmds = cmd.split()
        if cmds[0].startswith('['):
            cmds[0] = cmds[0].replace(' ', '')[1:-1]
            inagrs =  dict(c.split(':') for c in cmds[0].split(','))
            time = inagrs.get('time', opts.time)
            cpus = inagrs.get('cpus', opts.cpus)
            depe = inagrs.get('depe', -1)
            name = inagrs.get('name', '')
            cmd  = ' '.join(cmds[1:])
        else:
            name = ''
            cmd  = ' '.join(cmds)
            time = opts.time
            cpus = opts.cpus

        name = name.replace('/', '_')
        if name.startswith('-') or name.startswith('.') or name.startswith('+'):
            name = '_' + name[1:]
        # define priority class
        qos = opts.qos
        if qos == 'debug' and int(time.split(':')[0]) > 2:
            raise Exception('ERROR: changed to lowprio, too long job')

        # # requeueing
        # req = '# @ requeue          = 1' if opts.requeue else ''
        # define memory
        req = ''
        # mem = ('# @ memory           = ' +
        #        '{}'.format(opts.memory) if opts.memory else '')
        mem = ''
        # define group
        # if opts.dedicated or opts.exclusive:
        #     where = WHERE.format('Ex' if opts.exclusive else '',
        #                          'debug' if opts.exclusive else 'development',
        #                          GROUP)
        # elif opts.partition:
        #     where = '# @ partition        = %s\n' % (opts.partition)
        # else:
        where = ''
        # write job script
        out = open(join(PATH, 'jobscript_'+str(jobnum)+'.cmd'), 'w')
        out.write(SCRIPT.format(array=job_list, job_name=name, job_num=jobnum,
                                time=time, path=PATH, qos=qos, requeuing=req,
                                memory=mem, group_info=where, cpus=cpus,
                                cmd=cmd))
        out.close()

        # in case we just want to write jobs cripts
        if opts.norun:
            stdout.write('wrote cmd file %5s/%-5s\n' % (jobnum, total_jobs))
            jobnum += 1
            continue

        # pause each 10 jobs launched
        if not jobnum % 10:
            stderr.write('pause...\n')
            sleep(1)

        # define dependencies (this is passed outside job script)
        if depe > -1:
            depe = ' -d ' + jobids[str(depe)]
        else:
            depe = ''

        # submit
        out, err = Popen('sbatch' + depe + ' ' +
                         join(PATH, 'jobscript_'+ str(jobnum)+'.cmd'),
                         shell=True, stdout=PIPE, stderr=PIPE).communicate()
        # writes submission info
        if err:
            stderr.write(err + '\n')
        if 'Submitted batch job' in out:
            stdout.write('%s %5s/%-5s\n' % (out.strip(), jobnum, total_jobs))
            jobids[str(jobnum - 1)] = out.split()[-1]
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
    :+-------------------------------------------------------------------------+
    :| Reads a list of jobs from input file and launch each in SLURM.          |`+
    :|                                                                         | |
    :|  * this script will not allow more than 1000 jobs to be PENDING, it     | |
    :|    will wait until this number falls bellow 900 to continue launching   | |
    :|                                                                         | |
    :|  * information about dependencies, job names or execution time can be   | |
    :|    specified inside the input file, at the startbeginning of each line, | |
    :|    as: [name:joe_73,time:2:00:00,cpus:8,depe:23]                        | |
    :|                                                                         | |
    :+-------------------------------------------------------------------------+ |
    : `-------/   /-------------------------------------------------------------`+
    :        /   /
    :       /   /
    :      /   /
    :     /   /
    :    /   /
    :   /   /
    :  /   /
    : /   /
    :/   /
    :   /
    :  /
    : /
    :/
    :"""
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

    # parser.add_argument('--dedicated', action='store_true', dest='dedicated',
    #                   default=False, help=('[%(default)s] Use only dedicated '
    #                                        'nodes of group %s' % GROUP))

    # parser.add_argument('--exclusive', action='store_true', dest='exclusive',
    #                   default=False,
    #                   help=("[%(default)s] "
    #                         "Use %s's exclusive or debug node" % GROUP))

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
                          dest='time', default='20:00:00',
                          help='''[%(default)s] Maximum run time allowed''')

    log_args.add_argument('--no_out', action='store_true', dest='no_out',
                          default=False, help='[%(default)s] Do not store '
                          'standard output')

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
                          dest='how', default='PENDING',
                          choices=['PENDING', 'RUNNING'],
                          help='[%(default)s] monitor running or pending jobs '
                          'to limit launches')

    exe_args.add_argument('--wait_jobs', action='store', metavar='INT',
                          dest='wait_jobs', default=900, type=int,
                          help='[%(default)s] set the limit in number of jobs')

    # parser.add_argument('--memory', action='store',
    #                   dest='memory', default=None,
    #                   help='''[5600] Amount of RAM required in Mb''')

    opts = parser.parse_args()
    global SCRIPT
    SCRIPT = SCRIPT.format(out=('/dev/null' if opts.no_out else OUT),
                           err=('/dev/null' if opts.no_err else ERR))
    return opts


if __name__ == "__main__":
    exit(main())
