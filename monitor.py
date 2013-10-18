#!/usr/bin/python
"""
22 Mar 2013


"""

from subprocess import Popen, PIPE
from optparse   import OptionParser
from sys        import stdout, stdin
from getpass    import getuser
from time       import time, sleep, mktime
from select     import select
from datetime   import datetime

import os

CMD = '/opt/perf/bin/squeue -o "%a %u %i %j %T %M %l %C %D %q %P %p %R"'
CMDSACCT  = '/usr/local/bin/sacct -j %s --format JobId,State,Start,End -X -n'

BEGTIME = time() # in the begining of time...
AUTOWIDTH = True

TIME_ROUND = 15000 # used to group jobs by time (600 corresponds to +- 5 min)

def subjobs(jobs, field, val, inverse=False, test=None):
    """
    fields are:
    'ACCOUNT', 'CPUS', 'JOBID', 'NAME', 'NODELIST(REASON)', 'NODES', 
    'PARTITION', 'PRIORITY', 'QOS', 'STATE', 'TIME', 'TIMELIMIT', 'USER'
    """
    if not test:
        same = lambda x, y: x == y
        diff = lambda x, y: x != y
        test = diff if inverse else same
    return dict([(j, jobs[j]) for j in jobs if test(jobs[j][field], val)])


def get_list():
    """
    Get the list of jobs running, with maximum amount of information :)
    """
    table = Popen(CMD, shell=True, stdout=PIPE).communicate()[0]
    jobs = {}
    headers = table.split('\n')[0].split()
    for line in table.split('\n')[1:]:
        if not line:
            continue
        jobid = line.split()[2]
        jobs[jobid] = {}
        for i, val in enumerate(line.split()):
            jobs[jobid][headers[i]] = val
    return jobs


def print_stats(jobs, users, width=80, verbose=2, # jobsname=None,
                groupby=None, indices=False, blanks=None):
    """
    Main printer.

    :param jobs: dictionary with job list obtained from :func:`get_list`
    :param users: list of users to be displayed
    :param 80 width: print area width
    :param None groupy: a key to group user's jobs. If None, jobs will only be
       grouped by user name
    :param False indices: to only get the groups defined here
    :param None blanks: number of spaces before returned groups (only used with
       indices=True)
    """
    grp_time = '(groups: +- %s)' % (time2str(TIME_ROUND / 2,
                                               tround='m') if groupby else '')
    out = 'SLURM monitor (uptime %s) %s\n\n' % (time2str(int(time() - BEGTIME)),
                                                grp_time)
    indx = 0
    for user in users + (['total'] if len(users) else []):
        userjobs = jobs if user == 'total' else subjobs(jobs, 'USER', user)
        round_time = lambda x: (totime(userjobs[x][groupby]) / TIME_ROUND)
        #
        groups = []
         # grouby timelimit with +- TIME_ROUND/2 precisison
        if groupby == 'TIMELIMIT' and not user=='total':
            groups = set([round_time(j) for j in userjobs])
            groups = sorted([t for t in groups])
        elif groupby == 'NAME' and not user == 'total':
            groups = set([userjobs[j]['NAME'].split('__')[0]
                          for j in userjobs if '__' in userjobs[j]['NAME']])
            if len(groups) == 0:
                groups = ['']
            groups = sorted([t for t in groups])
        elif groupby == 'NAME' and user == 'total':
            groups = ['']
        else:
            groups = [None]
        for grp in groups:
            if groupby == 'TIMELIMIT' and user!='total':
                grpjobs = subjobs(userjobs, 'TIMELIMIT', grp,
                                  test=lambda x, y: totime(x) / TIME_ROUND == y)
            elif groupby == 'NAME':
                grpjobs = subjobs(userjobs, 'NAME', grp,
                                  test=lambda x, y: x.startswith(y) if y > -1 else False)
            else:
                grpjobs = userjobs
            paused = check_pause(grpjobs) if user!='total' else False
            if indices and user!='total':
                if groupby == 'TIMELIMIT':
                    print blanks + ('\033[7;30;43m[\033[m' +
                                    '\033[7;30;4;41m%2s\033[m' +
                                    '\033[7;30;43m]%8s %s\033[m') % (
                        indx, '~' + time2str(int(TIME_ROUND * (grp + 1.5)),
                                             tround='m')
                        if grp > -1 else 'ALL', '(paused)' if paused else '')
                else:
                    print blanks + ('\033[7;30;43m[\033[m' +
                                    '\033[7;30;4;41m%2s\033[m' +
                                    '\033[7;30;43m]%8s %s\033[m') % (
                        indx, '~' + grp if grp > -1 else 'ALL', '(paused)' if paused else '')
                indx += 1
                yield grpjobs
            running = subjobs(grpjobs, 'STATE'           , 'RUNNING'     )
            cmpling = subjobs(grpjobs, 'STATE'           , 'COMPLETING'  )
            pending = subjobs(grpjobs, 'STATE'           , 'PENDING'     )
            dependy = subjobs(grpjobs, 'NODELIST(REASON)', '(Dependency)')
            doneing = subjobs(grpjobs, 'STATE'           , 'COMPLETED'   )
            errored = subjobs(grpjobs, 'STATE'           , 'CANCELLED'   )
            #
            running = count_procs(running)
            cmpling = count_procs(cmpling)
            pending = count_procs(pending)
            dependy = count_procs(dependy)
            doneing = count_procs(doneing)
            errored = count_procs(errored)
            #
            limtime = get_time(grpjobs, tround='m')
            maxtime = get_time(subjobs(grpjobs, 'STATE', 'RUNNING'),
                               kind='TIME', tround='m', calc='max')
            if user == getuser() and doneing: # if user is getuser and jobs already done
                runtime = get_time(subjobs(grpjobs, 'STATE', 'COMPLETED'),
                                   kind='SPENT', tround='m', fact=1)
            else:
                runtime = get_time(subjobs(grpjobs, 'STATE', 'RUNNING'),
                                   kind='TIME', tround='m', fact=2)
            #
            avgprio = get_prio(grpjobs)
            njobs = (running + pending + doneing + cmpling + errored) or 1
            factor = float(width) / njobs
            name = '%-12s' % (user.upper())
            #cnt = int(log(running + pending)) if (running + pending) else 0
            cnt = 0.0001*running + running/25
            if user != 'total':
                name = ['\033[7;31m%s\033[m' % (l) if i < cnt else l \
                            for i, l in enumerate(name)]
            else:
                name = name.lower()
            out += ''.join(name)
            out += single_stats(running, pending, dependy, cmpling, doneing,
                                errored,
                                limtime, runtime, maxtime, factor, grp, paused,
                                avgprio, verbose, groupby)
    if not indices:
        os.system('clear')
        stdout.write(out)
    

def single_stats(running, pending, dependy, cmpling, done, error, limtime,
                 runtime, maxtime, factor, grp, paused, avgprio, verbose,
                 groupby):
    """
    Returns one item (2 lines).
    """
    if verbose == 2:
        sprio = '|prio:%s(%s-%s)' % (str(round(avgprio[0]*1000, 3)),
                                     str(round(avgprio[1]*1000, 3)),
                                     str(round(avgprio[2]*1000, 3)))
    else:
        sprio = ''
    def get_bar(prio):
        for s in '|' * (running + pending + dependy + cmpling + done +error - len(prio)-1) + prio + '|':
            yield s
    priobar = get_bar(sprio)
    out = ''
    if verbose == 2:
        out += ('run:\033[0;31m%-3s\033[m ' +
                'cpl:\033[0;35m%-1s\033[m ' + 
                'pen:\033[0;33m%-5s\033[m' + 
                '(dep:\033[1;30m%-5s\033[m) ' + 
                'end:\033[0;32m%-5s\033[m' +
                '(err:\033[0;34m%-3s\033[m) ' + 
                'T:%-7s(.-%-7s)' + 
                '<%s\n') % (running, cmpling, pending, dependy, done + error,
                            error, runtime, maxtime, limtime)
    else:
        out += ('run:\033[0;31m%-3s\033[m ' +
                'cpl:\033[0;35m%-1s\033[m ' + 
                'pen:\033[0;33m%-5s\033[m' + 
                '(dep:\033[1;30m%-5s\033[m) ' + 
                'end:\033[0;32m%-5s\033[m' +
                '(err:\033[0;34m%-3s\033[m) ' + 
                'T:%-7s<%-7s'+
                ' prio:%s\n') % (
            running, cmpling, pending, dependy, done + error, error, runtime, limtime,
            round(avgprio[2]*1000, 3))        
    if verbose:
        if grp > -1:
            out += '%-12s' % (('  ~' + time2str(int(TIME_ROUND * (grp + 1.5)),
                                                tround='m'))
                              if groupby == 'TIMELIMIT' else grp if groupby == 'NAME' else '')
        else:
            out += ' ' * 12
        ## summing up remaining of floats and finding where to put it
        restr = ((factor * (running)           * 10)%10)/10
        restc = ((factor * (cmpling)           * 10)%10)/10
        restp = ((factor * (pending - dependy) * 10)%10)/10
        restd = ((factor * (dependy)           * 10)%10)/10
        restf = ((factor * (done)              * 10)%10)/10
        reste = ((factor * (error)             * 10)%10)/10
        most = max([i for i in enumerate([restr, restc, restp, restd, restf, reste])],
                   key=lambda x:x[1])[0]
        rest  = restr + restp + restd + restf
        running = int(factor * running             + (rest if most == 0 else 0))
        cmpling = int(factor * cmpling             + (rest if most == 1 else 0))
        pending = int(factor * (pending - dependy) + (rest if most == 2 else 0))
        dependy = int(factor * dependy             + (rest if most == 3 else 0))
        done    = int(factor * done                + (rest if most == 4 else 0))
        error   = int(factor * error               + (rest if most == 5 else 0))
        # print the bars
        out += (''.join(['\033[0;31m%s\033[m' % (priobar.next()) for i in xrange(running)]) +
                ''.join(['\033[0;35m%s\033[m' % (priobar.next()) for i in xrange(cmpling)]) +
                ''.join(['\033[0;33m%s\033[m' % (priobar.next()) for i in xrange(pending)]) +
                ''.join(['\033[1;30m%s\033[m' % (priobar.next()) for i in xrange(dependy)]) +
                ''.join(['\033[0;32m%s\033[m' % (priobar.next()) for i in xrange(done)   ]) +
                ''.join(['\033[0;34m%s\033[m' % (priobar.next()) for i in xrange(error)  ])) + '\n'
        if verbose == 2:
            out += '\n'
    if paused:
        for let in 'PAUSED-':
            out = out.replace('|', let, 1)
    return out
    

def count_procs(jobs):
    procs = 0
    for job in jobs:
        procs += int(jobs[job]['CPUS'])# * int(job['NODES'])
    return procs


def totime(timestr):
    times = [t for t in timestr.split(':')]
    if '-' in times[0]:
        times = times[0].split('-') + times[1:]
    elif len(times) < 3:
        times = [0] + [0] + times
    else:
        times = [0] + times
    try:
        times = [int(t) if not 'N' in str(t) else 0 for t in times]
        return int(times[0] * 24 * 60 * 60 + times[1] *60 *60 + times[2] * 60)
    except Exception as e:
        print e, times
        exit()


def time2str(seconds, tround='s'):
    if tround == 'm' and seconds%60 >= 30:
        seconds+= 60 - seconds%60
    days    = (seconds / (60*60*24))
    seconds -= days * 24 * 60 * 60
    hours   = (seconds / (60*60))
    seconds -= hours * 60 *60
    minutes = (seconds / 60)
    seconds -= minutes * 60
    if tround == 'm':
        return '%s-%02d:%02d' % (days, hours, minutes)
    return '%s-%02d:%02d:%02d' % (days, hours, minutes, seconds)


def get_time(jobs, kind='TIMELIMIT', tround='s', calc='avg', fact=1):
    if kind == 'SPENT':
        times = [jobs[j][kind] for j in jobs]
    else:
        times = [totime(jobs[j][kind]) for j in jobs]
    try:
        if calc=='avg':
            return time2str(int(fact * float(sum(times)) / len(jobs)), tround)
        elif calc == 'max':
            if len(jobs):
                return time2str(int(float(max(times))), tround)
            else:
                return 0
    except ZeroDivisionError:
        return 0


def get_prio(jobs, kind='PRIORITY'):
    prio = [float(jobs[j][kind]) for j in subjobs(jobs, 'STATE', 'PENDING')]
    try:
        return sum(prio) / len(prio), min(prio), max(prio)
    except ZeroDivisionError:
        return 0., 0., 0.


def clean_done_users(jobs, users, groupby=None):
    """
    check if some user has no running neither pending jobs, and remove it from
    the main dict.
    
    :param jobs: dict of jobs
    :param users: list of users in jobs

    :returns: new dict of jobs
    """
    for user in users:
        userjobs = subjobs(jobs, 'USER', user)
        round_time = lambda x: (totime(userjobs[x][groupby]) / TIME_ROUND)
        if groupby == 'TIMELIMIT' and not user=='total':
            groups = set([round_time(j) for j in userjobs])
            groups = sorted([t for t in groups])
        else:
            groups = [None]
        for grp in groups:
            if groupby == 'TIMELIMIT' and user!='total':
                grpjobs = subjobs(userjobs, 'TIMELIMIT', grp,
                                  test=lambda x, y: totime(x) / TIME_ROUND == y)
            elif groupby == 'NAME':
                grpjobs = subjobs(userjobs, 'NAME', grp,
                                  test=lambda x, y: x.startswith(y) if y > -1 else False)
            else:
                grpjobs = userjobs
            cnt  = count_procs(subjobs(grpjobs, 'STATE', 'RUNNING'))
            cnt += count_procs(subjobs(grpjobs, 'STATE', 'PENDING'))
            if cnt:
                continue
            for j in subjobs(grpjobs, 'STATE', 'COMPLETED').keys() + subjobs(grpjobs, 'STATE', 'CANCELLED').keys():
                del(jobs[j])


def kill_jobs(job_list):
    Popen('mncancel ' + ' '.join(job_list.keys()), shell=True, stdout=PIPE,
          stderr=PIPE)


def pause_jobs(job_list, paused=True):
    if paused:
        print ' releasing...'
        cmd = 'mnhold --release '
    else:
        print ' pausing...'
        cmd = 'mnhold '
    for job in job_list:
        while True:
            try:
                Popen(cmd + job, shell=True,
                      stdout=PIPE, stderr=PIPE)
                break
            except OSError:
                sleep(1)


def check_pause(job_list):
    return '(JobHeldUser)' in [job_list[j]['NODELIST(REASON)'] \
                               for j in job_list]


def color_str(chars, col):
    if col == 'greyred':
        return '\033[7;30;4;41m' + chars + '\033[m'
    elif col == 'greygold':
        return '\033[7;30;43m' + chars + '\033[m'


def color_bar(name, width=8):
    lname = len(name)
    out = ''
    name =  ' ' * ((width - lname) / 2 + (width - lname) % 2) + name
    name += ' ' * ((width - lname) / 2)
    lname = len(name)
    cnt = 0
    while cnt < lname:
        let = name[cnt]
        if let.isupper() or let in ['-', '+']:
            out += '\033[7;30;4;41m' + let + '\033[m'
            cnt += 1
        else:
            out += '\033[7;30;43m'
            while cnt < lname:
                let = name[cnt]
                if let.isupper() or let in ['-', '+']:
                    break
                out += let
                cnt += 1
            out += '\033[m'
    return out


def console(opts, jobs, users):
    global TIME_ROUND
    global AUTOWIDTH
    help_s = """
Help:
******
  * +: more info displayed
  * -: less info displayed (more compact)
  * c: clean up users with no pending jobs
  * f: change refresh rate (minutes)
  * g: group by a given element (implemented: TIMELIMIT)
  * h: help
  * k: kill list of jobs (only for user %s)
  * l: list jobs
  * p: suspend/resume list of jobs
  * q: quit
  * r: reload
  * w: change width of the display (min 78)

Abbreviations:
 - run: running jobs
 - cpl: completing jobs
 - pen: pending jobs
 - dep: pending jobs with dependencies
 - end: finished jobs
 - err: finished jobs with SLURM errors (e.g. out of memory)
 - T  : time -> T:MEANTIME(.-MAXTIME)<LIMTIME, where, MEANTIME is either twice
        the mean time of your running jobs, either the mean spent time of your
        completed jobs (only for your jobs); MAXTIME the time spent by your
        longest job; and LIMTIME the mean time limit set.
- prio: priority-> prio:MEANPRIO(MINPRIO-MAXPRIO); for pending jobs (and
        jobs with dependencies)
        
""" % (getuser())
    options = [' +/- info  ', 'reFresh rate', 'Group', 'Help',
               'Kill', 'Pause/Play', 'Quit', 'Reload', 'Clean']
    os.system('tput civis')
    getch = _Getch()
    toreload  = True
    print ("\033[7;30;43m %s \033[m" % (" " * int((opts.width-81+1.5)/2)) +
           ("%s " * 8 + "%s") % tuple([color_bar(o) for o in sorted(options)]) +
           "\033[7;30;43m %s\033[m" %  (" " * int((opts.width-81+2)/2)))
    while True:
        s = getch(opts.refresh).lower()
        stdout.flush()
        if s == 'rr':
            break
        if s == 'r':
            print 'reloading on demand...'
            break
        elif s == 'h':
            print help_s
        elif s == 'l':
            print '\n   %-8s %-30s %-10s %-4s %-10s %-10s' % ('JOBID', 'JOBNAME',
                                                           'STATE', 'CPUS',
                                                           'TIME', 'TIMELIMIT')
            print '   ' + '-' * 76
            for j in jobs:
                if jobs[j]['USER'] != getuser():
                    continue
                print '   %-8s %-30s %-10s %-4s %-10s %-10s' % (
                    jobs[j]['JOBID'], jobs[j]['NAME'] if len(jobs[j]['NAME'])<30 else jobs[j]['NAME'][:30], jobs[j]['STATE'],
                    jobs[j]['CPUS'], jobs[j]['TIME'], jobs[j]['TIMELIMIT'])
        elif s == '+':
            opts.verbose += 1 if opts.verbose < 2 else 0
            toreload  = False
            break
        elif s == '-':
            opts.verbose -= 1 if opts.verbose else 0
            toreload  = False
            break
        elif s == 'c':
            clean_done_users(jobs, users, opts.groupby)
            toreload  = False
            break
        elif s == 'q':
            raise KeyboardInterrupt
        elif s == 'w':
            width = raw_input('new width (actual=%s; minimum=82; "auto" for max available): ' % opts.width)
            try:
                width = int(width)
                AUTOWIDTH = False
            except ValueError:
                if width == 'auto':
                    width = getTerminalSize()[0] - 12
                    AUTOWIDTH = True
                else:
                    print 'Integer needed...\n nothing done\n'
                    sleep(1)
                    toreload  = False
                    break
            opts.width = 82 if width < 82 else width
            toreload  = False
            break
        elif s == 'g':
            blanks = ' ' * 15
            print blanks + '\033[7;30;43m \033[m'
            print blanks + ('\033[7;30;43m \033[m' +
                            '\033[7;30;4;41mT\033[m' +
                            '\033[7;30;43mime (%s minute precision)\033[m'
                            ) % (TIME_ROUND / 60)
            print blanks + ('\033[7;30;43m \033[m' +
                            '\033[7;30;4;41mN\033[m' +
                            '\033[7;30;43mame (first part of job name, before "__")\033[m'
                            )
            stdout.write(blanks + '\033[m' +
                         '\033[7;30;43m \033[m' +
                         '\033[7;30;4;41mO\033[m' +
                         '\033[7;30;43mnly group by user name\033[m')
            stdout.flush()
            groupby = getch(opts.refresh).lower()
            if groupby == 't':
                opts.groupby = 'TIMELIMIT'
            elif groupby == 'n':
                opts.groupby = 'NAME'
            else:
                opts.groupby = None
            if not groupby == 't':
                toreload  = False
                break
            stdout.write('\r')
            os.system('tput cnorm')
            time_round = raw_input(blanks + '   ' +
                                   '\033[7;30;43mGroup by time limit of' +
                                   '(actual=%s min):\033[m '
                                   % (TIME_ROUND / 60))
            try:
                time_round = int(time_round) * 60
                TIME_ROUND = time_round
            except ValueError:
                print 'Integer needed...\n nothing done\n'
                sleep(1)
                toreload  = False
                break
            toreload  = False
            break
        elif s == 'f':
            os.system('tput cnorm')
            refresh = raw_input('new refresh rate (actual=' +
                                '%s minutes): ' % (opts.refresh/60))
            try:
                refresh = int(float(refresh) * 60)
            except ValueError:
                print 'Integer needed...\n nothing done\n'
                sleep(1)
                toreload  = False
                break
            opts.refresh = 10 if refresh < 10 else refresh
            toreload  = False
            break
        elif s == 'k':
            blanks = ' ' * 42
            print blanks + '\033[7;30;43m \033[m'
            groups = list(print_stats(jobs, [getuser()], width=80,
                                      groupby=opts.groupby, indices=True,
                                      blanks=blanks, verbose=opts.verbose))
            stdout.write(blanks +
                         '\033[7;30;43m[\033[m' +
                         '\033[7;30;4;41m x\033[m' +
                         '\033[7;30;43m]     JobID\033[m\n')
            stdout.write(blanks +
                         '\033[7;30;43m[\033[m' +
                         '\033[7;30;4;41m q\033[m' +
                         '\033[7;30;43m]     Nothing\033[m\n')
            stdout.flush()
            tokill = getch(opts.refresh).lower()
            stdout.flush()
            if tokill.lower() == 'q':
                toreload = False
                break
            if tokill == 'x':
                jobid = raw_input('JobID of the job to kill: ')
            kill = raw_input('\n\n=====>  You selected to kill ' +
                             '%s occupying %s CPUs [y|n]: ' %
                             ('job: ' + jobid if tokill == 'x' else str(len(groups[int(tokill)])) + ' jobs',
                              jobs[jobid]['CPUS'] if tokill == 'x' else count_procs(groups[int(tokill)])))
            if kill == 'y' or kill == 'yes':
                print 'bang bang!'
                if tokill == 'x':
                    tokill = {jobid: None}
                else:
                    tokill = groups[int(tokill)]
                try:
                    kill_jobs(tokill)
                except ValueError:
                    print 'Integer needed...\n nothing done\n'
                    sleep(1)
                    toreload  = False
                    break
                except IndexError:
                    print 'Bad group number\n'
                    sleep(1)
                    toreload  = False
                    break
            else:
                print 'aborted...'
                sleep(1)
                toreload  = False
            break
        elif s == 'p':
            blanks = " " * 51
            print blanks + '\033[7;30;43m \033[m'
            groups = list(print_stats(jobs, [getuser()], width=80,
                                      groupby=opts.groupby, indices=True,
                                      blanks=blanks, verbose=opts.verbose))
            stdout.write(blanks +
                         '\033[7;30;43m[\033[m' +
                         '\033[7;30;4;41m x\033[m' +
                         '\033[7;30;43m]     JobID\033[m\n')
            stdout.write(blanks +
                         '\033[7;30;43m[\033[m' +
                         '\033[7;30;4;41m q\033[m' +
                         '\033[7;30;43m]     Nothing\033[m\n')
            topause = getch(opts.refresh).lower()
            stdout.flush()
            if topause.lower() == 'q':
                toreload = False
                break
            if topause == 'x':
                jobid = raw_input('JobID of the job to Pause/Play: ')
            # system('tput cnorm')
            # topause = raw_input('\nEnter the "id_jobs" number to pause/' +
            #                     'suspend all corresponding jobs\n}:-> ')
            topause = {jobid: jobs[jobid]} if topause == 'x' else groups[int(topause)]
            try:
                pause = check_pause(topause)
            except ValueError:
                print 'Not found...\n nothing done\n'
                sleep(1)
                toreload  = False
                break
            try:
                pause_jobs(topause, pause)
            except IndexError:
                print 'Not valid group number\n'
                toreload  = False
                sleep(1)
            break
        else:
            toreload = False
            break
    if toreload:
        print 'reloading...'
        return True
    else:
        return False


def date_2_sec(date):
    date, hour = date.split('T')
    year, month, day = [int(d) for d in date.split('-')]
    hour, minute, second = [int (h) for h in hour.split(':')]
    return mktime(datetime(year, month, day, hour, minute, second).timetuple())


def update_ended(jobs, ended):
    if not ended:
        return
    table = Popen(CMDSACCT % (','.join(ended)), shell=True,
                  stdout=PIPE).communicate()[0]
    for line in table.split('\n'):
        try:
            jobid, state, beg, end = line.split()
        except ValueError:
            continue
        jobs[jobid]['SPENT'] = date_2_sec(end) - date_2_sec(beg)
        jobs[jobid]['STATE'] = state.replace('+', '')


def main():
    """
    main function
    """
    global AUTOWIDTH
    opts = get_options()
    jobs = {}
    toreload = True
    while True:
        try:
            if toreload:
                for job in jobs:
                    jobs[job]['STATE'] = 'COMPLETED'
                    jobs[job]['NODELIST(REASON)'] = ''
                jobs.update(get_list())
                my_ended = []
                for job in jobs:
                    if jobs[job]['USER'] != getuser():
                        continue
                    if jobs[job]['STATE'] != 'COMPLETED':
                        continue
                    my_ended.append(job)
                update_ended(jobs, my_ended)
            if opts.user != 'all':
                users = [opts.user]
            else:
                users = sorted(list(set([jobs[j]['USER'] for j in jobs])))
            # put list here because of yield inside... not nice
            if AUTOWIDTH:
                opts.width = getTerminalSize()[0] - 12
            list(print_stats(jobs, users, width=opts.width,
                             # jobsname=opts.jobsname,
                             groupby=opts.groupby, verbose=opts.verbose))
            if not opts.watch:
                break
            toreload = console(opts, jobs, users)
        except KeyboardInterrupt:
            print '\nbye-bye %s ;)\n' % getuser()
            print bye()
            os.system('tput cnorm')
            exit()


def getTerminalSize():
    env = os.environ
    def ioctl_GWINSZ(fd):
        try:
            import fcntl, termios, struct
            cr = struct.unpack('hh', fcntl.ioctl(fd, termios.TIOCGWINSZ,
        '1234'))
        except:
            return
        return cr
    cr = ioctl_GWINSZ(0) or ioctl_GWINSZ(1) or ioctl_GWINSZ(2)
    if not cr:
        try:
            fd = os.open(os.ctermid(), os.O_RDONLY)
            cr = ioctl_GWINSZ(fd)
            os.close(fd)
        except:
            pass
    if not cr:
        cr = (env.get('LINES', 25), env.get('COLUMNS', 80))

        ### Use get(key[, default]) instead of a try/catch
        #try:
        #    cr = (env['LINES'], env['COLUMNS'])
        #except:
        #    cr = (25, 80)
    return int(cr[1]), int(cr[0]) # width, height of the terminal

    
def get_options():
    '''
    parse option from call
    '''
    # Prepare a default value for width
    width, _ = getTerminalSize()
    global AUTOWIDTH
    parser = OptionParser(
        usage="%prog [options] file [options [file ...]]",
        description="""\
        for slurm
        """
        )
    parser.add_option('-u', dest='user', default=getuser(),
                      help='[%default] username (-u all to see everybody).')
    parser.add_option('--nostats', action='store_false',
                      dest='stats', default=True, 
                      help='[%default] job stats.')
    # parser.add_option('--jobs', action='store', default=None,
    #                   dest='jobsname',
    #                   help='Job name to search for in log.')
    parser.add_option('--watch', action='store_true', default=False,
                      dest='watch', help='[%default] watch (ctrl-c to quit).')
    parser.add_option('--refresh', action='store', default=30,
                      dest='refresh',
                      help=('[%default] refresh rate in MINUTES,' +
                            'used with watch'))
    parser.add_option('--groupby', action='store', default=None,
                      dest='groupby',
                      help=('[%default] group user jobs by time limit ' +
                            '(groupby=time).'))
    parser.add_option('--width', action='store', default="auto (%s)" % (width - 12),
                      dest='width',
                      help='[%default] display width (should be > 80)')
    parser.add_option('--verbosity', action='store', default=2,
                      dest='verbose',
                      help='[%default] how much info to display (min:0, max:2)')

    opts = parser.parse_args()[0]
    opts.refresh = int(float(opts.refresh) * 60)
    try:
        opts.width = int(opts.width)
        AUTOWIDTH = False
    except ValueError:
        if opts.width.startswith('auto '):
            opts.width = width - 12
            AUTOWIDTH = True
        else:
            raise Exception("Bad width value passed.")
    opts.verbose = int(opts.verbose)
    opts.groupby = 'TIMELIMIT' if opts.groupby == 'time' else 'NAME' if opts.groupby == 'name' else None
    return opts


def bye():
    return ['\n Bye-bye\n', '\n Talogo!!\n', '\n Have a nice day\n',
            '\n And they lived happily ever after and they had a lot of CPUs\n',
            '\n The End.\n', '\n Au revoir\n', 
            '\n Nooooo come back!!!! Quick!!\n',
            '\n Thanks.. I really enjoyed\n',
            "\n I'm a poor lonesome cowboy, and a long way from home...\n",
            '\n Flying Spaghetti Monster be with you.\n'] [int(str(time())[-1])]


class _Getch:
    """Gets a single character from standard input.
    Does not echo to the screen."""
    def __init__(self):
        self.impl = _GetchUnix()

    def __call__(self, refresh):
        return self.impl(refresh)


class _GetchUnix:
    def __init__(self):
        import tty

    def __call__(self, refresh):
        import tty, termios
        while True:
            fd = stdin.fileno()
            old_settings = termios.tcgetattr(fd)
            try:
                tty.setraw(stdin.fileno())
                rlist, _, _ = select([stdin], [], [], refresh)
                if not rlist:
                    return 'rr'
                ch = stdin.read(1)
                if not ch.isalnum():
                    if ch == '\x1b':
                        return 'qq'
                    # if we have an arrow
                    if ch == "\x1B":
                        stdin.read(2)
                    return ch
            finally:
                termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            return ch


# def get_full_job_list(jobsname):
#    n = 0
#    for rep in listdir(LOGPATH):
#        if not jobsname in rep:
#            continue
#        for fname in listdir(LOGPATH + rep):
#            if fname.endswith('.cmd'):
#                n += 1
#        break
#    return n


if __name__ == "__main__":
    exit(main())
    # try:
    #     exit(main())
    # except Exception as e:
    #     system('tput cnorm')
    #     print 'ERROR:', e
