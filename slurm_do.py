#!/usr/bin/python
"""
INFILE:


"""


from random     import random
from string     import lowercase
from time       import sleep
from optparse   import OptionParser
from subprocess import Popen, PIPE
from getpass    import getuser
from sys        import stdout, stderr
from os.path    import exists, expanduser
from os         import mkdir

################################################################################
# GLOBALS
LOGPATH = expanduser('~') + '/queue/%s/'
WHO     = getuser()
GROUP   = Popen('/opt/perf/bin/sacctmgr list user', shell=True,
              stdout=PIPE).communicate()[0].split()[-2]
GROUP   = GROUP[0].upper() + GROUP[1:-1] + GROUP[-1].upper()
SCRIPT  = """\
# @ job_name         = {9}__{0}_{1}
# @ initialdir       = {4}
# @ output           = {4}{0}_{1}.out
# @ error            = {4}{0}_{1}.err
# @ total_tasks      = 1
# @ cpus_per_task    = {8}
# @ wall_clock_limit = {3}
# @ class            = {5}
{7}
{6}

{2}

"""
WHERE   = """\
# @ features         = {0}{2}
# @ account          = {2}
# @ partition        = {1}
"""
################################################################################

def count_jobs():
    """
    Counts the number of pending jobs
    """
    cmd = '/opt/perf/bin/squeue -o "%%u " -u %s -h -t PENDING | wc -l'
    return int(Popen(cmd % WHO,
                     shell=True, stdout=PIPE).communicate()[0])

def big_sleep(intime, time):
    """
    checks if there is not more than 900 pending jobs
    otherwise, waits.
    """
    firstpause = True
    while count_jobs() > 900:
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
        name = ''.join([lowercase[int(random()*26)] for _ in xrange(10)])
        stdout.write('ARRAY NAME: %s\n' % name)
    else:
        name = opts.name

    srt, end = int(opts.beg), int(opts.end)

    PATH = LOGPATH % (name)
    if not exists(PATH):
        mkdir(PATH)

    dep = -1
    jobids = {}
    jobnum = 1
    for cmd in open(opts.infile):
        if end:
            if not (srt <= jobnum <= end):
                jobnum += 1
                continue
        # if multiple of 100, checks if there is not more than 900 pending jobs
        # otherwise, waits
        if not jobnum % 100:
            big_sleep(opts.intime, opts.time)
        # parse command
        cmds = cmd.split()
        if opts.intime and opts.inname:
            cmd, time, name  = ' '.join(cmds[:-2]), cmds[-2], cmds[-1]
        elif opts.inname:
            cmd, name  = ' '.join(cmds[:-1]), cmds[-1]
            time = opts.time
        elif opts.intime:
            cmd, time  = ' '.join(cmds[:-1]), cmds[-1]
            name = str(jobnum)
        else:
            if opts.dependencies:
                dep = int(cmds[-1])
                cmds = cmds[:-1]
            try:
                name = '_'.join(cmds[2:])[-25:]
            except:
                name = opts.name
            cmd  = ' '.join(cmds)
            time = opts.time
        name = name.replace('/', '_')
        if name.startswith('-') or name.startswith('.') or name.startswith('+'):
            name = '_' + name[1:]
        # define priority class
        cls = 'lowprio' if int(time.split(':')[0]) > 24 else 'normal'
        # define memory
        mem = ('# @ memory           = ' + 
               '{}'.format(opts.memory) if opts.memory else '')
        # define group
        if opts.group:
            where = WHERE.format('Ex' if opts.exclusive else '',
                                 'debug' if opts.debug else 'development',
                                 GROUP)
        else:
            where = ''
        # write job script
        out = open(PATH + 'jobscript_'+str(jobnum)+'.cmd', 'w')
        out.write(SCRIPT.format(name, jobnum, cmd, time, PATH, cls, mem, where,
                                opts.cpus, name))
        out.close()
        # in case we just want to write jobs cripts
        if opts.norun:
            jobnum += 1
            continue
        # pause each 10 jobs launched
        if not jobnum % 10:
            stderr.write('pause...\n')
            sleep(1)
        # define dependencies (this is passed outside job script)
        if opts.dependencies and dep > -1:
            dep = ' -dep ' + jobids[str(dep)]
        else:
            dep = ''
        # submit
        out, err = Popen('mnsubmit' + dep + ' ' +
                         PATH + 'jobscript_'+ str(jobnum)+'.cmd',
                         shell=True, stdout=PIPE, stderr=PIPE).communicate()
        # writes submission info
        if err:
            stderr.write(err + '\n')
        if 'Submitted batch job' in out:
            stdout.write('%5s ok %s\n' % (jobnum, out.strip()))
            jobids[str(jobnum - 1)] = out.split()[-1]
        jobnum += 1


def get_options():
    '''
    parse option from call
    '''
    parser = OptionParser(
        usage="%prog [options] file [options [file ...]]",
        description="""
    ____________________________________________________________________________
    |                                                                           |
    |   Reads a list of jobs from input file and launch each in SLURM.          |  
    |                                                                           |
    |    * this script will not allow more than 1000 jobs to be PENDING, it     |
    |      will wait until this number falls bellow 900 to continue launching   |
    |                                                                           |
    |    * information about dependencies, job names or execution time can be   |
    |      specified inside the input file, at the end of each line, see help   |
    |      for intime, injobname and independencies.                            | 
    |                                                                           |
    |    * each line should be built as follow:                                 | 
    |        executable [args_for_executable]+ [maxtime] [jobname] [dependency] |
    |___________________________________________________________________________|
        """
        )
    parser.add_option('-i', dest='infile', metavar="PATH", 
                      help='path to command list file')
    parser.add_option('--start', action='store', 
                      dest='beg', default=0, 
                      help=('[first] line number (from command list file)' +
                            ' at which to start.'''))
    parser.add_option('--stop', action='store', 
                      dest='end', default=0, 
                      help=('[last] line number (from command list file)' +
                            ' at which to stop.'''))
    parser.add_option('--name', action='store',
                      dest='name', default=None,
                      help='''[Random string] Name of the array job.''')
    parser.add_option('--exclusive', action='store_true', dest='exclusive',
                      default=False, help=('[%default] Use only exclusive ' +
                                           'nodes (ONLY with debug!!!).'))
    parser.add_option('--groupnodes', action='store_true', dest='group',
                      default=False, help='[%default] Use only group nodes.')
    parser.add_option('--norun', action='store_true', dest='norun',
                      default=False, help='[%default] Do not run mnsubmit.')
    parser.add_option('--intime', action='store_true', dest='intime',
                      default=False, 
                      help=('[%default] Time specification in input file ' +
                            '(first extra parameter)'))
    parser.add_option('--injobname', action='store_true', dest='inname',
                      default=False, 
                      help=('[%default] Name of the job in the input file ' +
                            '(second extra parameter).'))
    parser.add_option('--independencies', action='store_true',
                      dest='dependencies', default=False, 
                      help=('[%default] if called, the script will expect to ' +
                            'find at each line, the line number corresponding' +
                            ' to the job that the current job will have to ' +
                            'wait for (-1 if not dependent) ' +
                            '(last extra parameter)'))
    parser.add_option('--time', action='store',
                      dest='time', default='20:00:00',
                      help='''[%default] Maximum run time alowed.''')
    parser.add_option('--cpus', action='store',
                      dest='cpus', default='1',
                      help='''[%default] Number of CPUs per tasks''')
    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, 
                      help='[%default] Use debug queue, for testing.')
    parser.add_option('--memory', action='store',
                      dest='memory', default=None,
                      help='''[5600] Amount of RAM required in Mb.''')

    opts = parser.parse_args()[0]
    # complete
    if opts.exclusive:
        opts.debug = True
    if opts.debug:
        opts.exclusive = True
    if opts.exclusive:
        opts.group = True
    if not opts.infile:
        exit(parser.print_help())
    opts.cpus = int(opts.cpus)
    return opts


if __name__ == "__main__":
    exit(main())
