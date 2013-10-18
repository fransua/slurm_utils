#!/usr/bin/python
"""
INFILE:


"""


from random   import random
from string   import lowercase
from time import sleep
import os
from optparse import OptionParser
from subprocess import Popen, PIPE

def main():
    opts = get_options()
    if not opts.array:
        array = ''.join([lowercase[int(random()*26)] for _ in xrange(10)])
        print 'ARRAY NAME:', array
    else:
        array = opts.array

    srt, end = int(opts.beg), int(opts.end)

    PATH = '/home/devel/fjserra/queue/jobs/'+array+'/'
    os.system('mkdir -p ' + PATH)

    dep = -1
    jobids = {}
    previd = ''
    for i, cmd in enumerate(open(opts.infile)):
        i += 1
        if end:
            if not (srt <= i <= end):
                continue
        cmds = cmd.split()
        # if os.path.isfile(('/home/devel/fjserra/Projects/tad-ledily/results/'+
        #                    '{}.pik').format(cmds[-1])):
        #     print 'EXISTS: {}'.format(cmds[-1])
        #     continue
        out = open(PATH + 'lala_'+str(i)+'.cmd', 'w')
        if opts.intime and opts.inname:
            cmd, time, name  = ' '.join(cmds[:-2]), cmds[-2], cmds[-1]
        elif opts.inname:
            cmd, name  = ' '.join(cmds[:-1]), cmds[-1]
            time = opts.time
        elif opts.intime:
            cmd, time  = ' '.join(cmds[:-1]), cmds[-1]
            name = str(i)
        else:
            if opts.dependencies:
                dep = int(cmds[-1])
                cmds = cmds[:-1]
            try:
                name = '_'.join(cmds[2:])[-25:]
            except:
                name = opts.array
            cmd  = ' '.join(cmds)
            time = opts.time
        if name.startswith('-') or name.startswith('.') or name.startswith('+'):
            name = '_' + name[1:]
        cls = 'lowprio' if int(time.split(':')[0]) > 24 else 'normal'
        mem = ('# @ memory           = ' + 
               '{}'.format(opts.memory) if opts.memory else '')
        if opts.group:
            where = WHERE.format('Ex' if opts.exclusive else '',
                                 'debug' if opts.debug else 'development')
        else:
            where = ''
        out.write(SCRIPT.format(name, i, cmd, time, PATH, cls, mem, where,
                                opts.cpus, array))
        out.close()
        if opts.norun:
            return
        if not i % 10:
            print 'pause...'
            sleep(1)
        if opts.dependencies and dep > -1:
            dep = ' -dep ' + jobids[str(dep)]
        else:
            dep = ''
        out, err = Popen('mnsubmit' + dep + ' ' + PATH + 'lala_'+str(i)+'.cmd',
                         shell=True, stdout=PIPE, stderr=PIPE).communicate()
        print err
        if 'Submitted batch job' in out:
            print 'ok', out.strip()
            jobids[str(i-1)] = out.split()[-1]


def get_options():
    '''
    parse option from call
    '''
    parser = OptionParser(
        usage="%prog [options] file [options [file ...]]",
        description="""\
        for slurm
        """
        )
    parser.add_option('-i', dest='infile', metavar="PATH", 
                      help='path to command list file')
    parser.add_option('--start', action='store', 
                      dest='beg', default=0, 
                      help='''
                      [%default] line number (from command list file)
                      at which to start.''')
    parser.add_option('--stop', action='store', 
                      dest='end', default=0, 
                      help='''
                      [%default] line number (from command list file)
                      at which to stop.''')
    parser.add_option('--name', action='store',
                      dest='array', default=None,
                      help='''[Random string] Name of the array job.''')
    parser.add_option('--exclusive', action='store_true', dest='exclusive',
                      default=False, help='Use only exclusive nodes (ONLY with debug!!!).')
    parser.add_option('--groupnodes', action='store_true', dest='group',
                      default=False, help='Use only group nodes.')
    parser.add_option('--norun', action='store_true', dest='norun',
                      default=False, help='Do not run mnsubmit.')
    parser.add_option('--intime', action='store_true', dest='intime',
                      default=False, 
                      help='Time specification in input file (prev-last arg).')
    parser.add_option('--injobname', action='store_true', dest='inname',
                      default=False, 
                      help='Name of the job in the input file (last arg).')
    parser.add_option('--independencies', action='store_true', dest='dependencies',
                      default=False, 
                      help='line number of the job that current job will have to wait for (-1 if not dependent) (last arg).')
    parser.add_option('--time', action='store',
                      dest='time', default='20:00:00',
                      help='''[%default] Maximum run time alowed.''')
    parser.add_option('--cpus', action='store',
                      dest='cpus', default='1',
                      help='''Number of CPUs per tasks''')
    parser.add_option('--debug', action='store_true', dest='debug',
                      default=False, 
                      help='Use debug queue, for testing.')
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


SCRIPT = """#!/bin/bash
# Script to run Ecolopy in CNAG cluster.
# It is used 8 nodes and 1 cpu per task.

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

WHERE = """\
# @ features         = {0}StruG
# @ account          = StruG
# @ partition        = {1}
"""

if __name__ == "__main__":
    exit(main())
