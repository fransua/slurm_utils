"""
22 Mar 2013

sacct -u fjserra   -oJobID,State,CPUTime -s r,cd

"""

from subprocess import Popen, PIPE
from optparse   import OptionParser
from os         import system
from sys        import stdout, stdin
from getpass    import getuser
from time       import time, sleep
from select     import select


CMD = '/opt/perf/bin/squeue -o "%a %u %i %j %T %M %l %C %D %q %P %p %R"'

# LOGPATH = '/home/devel/fjserra/queue/jobs/'

BEGTIME = time() # in the begining of time...

TIME_ROUND = 60000 # used for grouping jobs by time (600 corresponds to +- 5 min)


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


def print_stats(jobs, users, width=80, # jobsname=None,
                groupby=None, indices=False):
    out = 'SLURM monitor (uptime %s)\n\n' % (time2string(int(time()-BEGTIME)))
    indx = 0
    for user in users + (['total'] if len(users) else []):
        userjobs = jobs if user == 'total' else subjobs(jobs, 'USER', user)
        #
        groups = []
        if groupby == 'TIMELIMIT' and not user=='total': # grouby timelimit with 10 minute precisison
            groups = set([(totime(userjobs[j][groupby])/TIME_ROUND) for j in userjobs])
            groups = sorted([t for t in groups])
        else:
            groups = [None]
        for grp in groups:
            if grp > -1:
                grpjobs = subjobs(userjobs, 'TIMELIMIT', grp,
                                  test=lambda x, y: totime(x)/TIME_ROUND==y)
            else:
                grpjobs = userjobs
            paused = check_pause(grpjobs)
            if indices and user!='total':
                print '  [%2s] %12s %10s %s' % (
                    indx, user, time2string(grp * TIME_ROUND * 1.5, tround='m')
                    if grp else 'ALL', '(paused)' if paused else '')
                indx += 1
                yield grpjobs
            running = count_procs(subjobs(grpjobs, 'STATE'           , 'RUNNING'     ))
            cmpling = count_procs(subjobs(grpjobs, 'STATE'           , 'COMPLETING'  ))
            pending = count_procs(subjobs(grpjobs, 'STATE'           , 'PENDING'     ))
            dependy = count_procs(subjobs(grpjobs, 'NODELIST(REASON)', '(Dependency)'))
            doneing = count_procs(subjobs(grpjobs, 'STATE'           , 'DONE'        ))
            #
            avgtime = get_time(grpjobs, tround='m')
            runtime = get_time(subjobs(grpjobs, 'STATE', 'RUNNING'),
                               kind='TIME', tround='m')
            njobs = (running + pending + doneing + cmpling) or 1
            # if jobsname and user == 'fjserra':
            #     njobs = get_full_job_list(jobsname)
            #     doneing = njobs - (running + pending)
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
                                avgtime, runtime, factor, grp, paused)
    if not indices:
        system('clear')
        stdout.write(out)
    

def single_stats(running, pending, dependy, cmpling, done, avgtime,
                 runtime, factor, grp, paused):
    out = ''
    out += (   'run:\033[0;31m%-3s\033[m ' +
               'compl:\033[0;35m%-5s\033[m ' + 
               'pend:\033[0;33m%-5s\033[m ' + 
            '(depn:\033[1;30m%-5s\033[m) ' + 
            'done:\033[0;32m%-5s\033[m limit:%s ' + 
            'spent:%s\n') % (running, cmpling, pending, dependy, done, 
                             avgtime, runtime)
    out += '%-12s' % (('  ~' + time2string(grp * TIME_ROUND + TIME_ROUND / 2, tround='m'))
                      if grp > -1 else '')
    ## summing up remaining of floats and finding where to put it
    restr = ((factor * (running)           * 10)%10)/10
    restc = ((factor * (cmpling)           * 10)%10)/10
    restp = ((factor * (pending - dependy) * 10)%10)/10
    restd = ((factor * (dependy)           * 10)%10)/10
    restf = ((factor * (done)              * 10)%10)/10
    most = max([i for i in enumerate([restr, restc, restp, restd, restf])],
               key=lambda x:x[1])[0]
    rest  = restr + restp + restd + restf
    running = int(factor * running             + (rest if most == 0 else 0))
    cmpling = int(factor * cmpling             + (rest if most == 1 else 0))
    pending = int(factor * (pending - dependy) + (rest if most == 2 else 0))
    dependy = int(factor * dependy             + (rest if most == 3 else 0))
    done    = int(factor * done                + (rest if most == 4 else 0))
    # print the bars
    out += ('\033[0;31m|\033[m' * running +
            '\033[0;35m|\033[m' * cmpling + 
            '\033[0;33m|\033[m' * pending +
            '\033[1;30m|\033[m' * dependy +
            '\033[0;32m|\033[m' * done     ) + '\n\n'
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
        return times[0] * 24 *60 * 60 + times[1] *60 *60 + times[2] * 60
    except Exception as e:
        print e, times
        exit()


def time2string(seconds, tround='s'):
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


def get_time(jobs, kind='TIMELIMIT', tround='s'):
    times = sum([totime(jobs[j][kind]) for j in jobs])
    try:
        return time2string(int(float(times) / len(jobs)), tround)
    except ZeroDivisionError:
        return 0


def clean_done_users(jobs, users):
    """
    check if some user has no running neither pending jobs, and remove it from
    the main dict.
    
    :param jobs: dict of jobs
    :param users: list of users in jobs

    :returns: new dict of jobs
    """
    for user in users:
        userjobs = subjobs(jobs, 'USER', user)
        cnt  = count_procs(subjobs(userjobs, 'STATE', 'RUNNING'))
        cnt += count_procs(subjobs(userjobs, 'STATE', 'PENDING'))
        if cnt:
            continue
        for j in jobs.keys():
            if jobs[j]['USER'] == user:
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
    return 'JobHeldUser' in job_list.values()[0]['NODELIST(REASON)']



def color_bar(name, width=8):
    lname = len(name)
    out = ''
    name = ' ' * ((width - lname)/2 + (width - lname)%2) + name + ' ' * ((width - lname)/2)
    lname = len(name)
    cnt = 0
    while cnt < lname:
        let = name[cnt]
        if let.isupper():
            out += '\033[7;36m' + let + '\033[m'
            cnt += 1
        else:
            out += '\033[7;37m'
            while cnt < lname:
                let = name[cnt]
                if let.isupper():
                    break
                out += let
                cnt += 1
            out += '\033[m'
    return out


def console(opts, jobs, users):
    help_s = """
Help:
******
  * h: help
  * r: reload
  * c: clean up users with no pending jobs
  * f: change refresh rate (minutes)
  * w: change width of the display (min 78)
  * g: group by a given element (implemented: TIMELIMIT)
  * k: kill list of jobs (only for user %s)
  * p: suspend/resume list of jobs
  * q: quit
""" % (getuser())
    options = ['Clean done ', 'reFresh rate', 'Group', 'Help', 'Kill', 'Pause/Play', 'Quit', 'Reload', 'Width']
    system('tput civis')
    getch = _Getch()
    toreload  = True
    print ("\033[7;37m   \033[m" +
           ("%s " * 8 + "%s") % tuple([color_bar(o) for o in sorted(options)]) +
           "\033[7;37m  \033[m")
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
        elif s == 'c':
            clean_done_users(jobs, users)
            toreload  = False
            break
        elif s == 'q':
            raise KeyboardInterrupt
        elif s == 'w':
            width = raw_input('new width (actual=%s): ' % opts.width)
            width = int(width)
            opts.width = 78 if width < 78 else width
            toreload  = False
            break
        elif s == 'g':
            system('tput cnorm')
            groupby = raw_input('group user jobs by a feature ' +
                                '(actual: %s):\n' % opts.groupby +
                                ' - TIMELIMIT: write "time" ' +
                                '(10 minute precision)\n'
                                ' - None     : write anything\n}:---> ')
            groupby = groupby.strip()
            opts.groupby = 'TIMELIMIT' if groupby in ['time'] else None
            toreload  = False
            break
        elif s == 'f':
            system('tput cnorm')
            refresh = raw_input('new refresh rate (actual=' +
                                '%s minutes): ' % (opts.refresh/60))
            refresh = int(float(refresh) * 60)
            opts.refresh = 10 if refresh < 10 else refresh
            toreload  = False
            break
        elif s == 'k':
            system('tput cnorm')
            print 'job list grouped by %s:' % (opts.groupby)
            print '%-9s %9s %s' % ('id_jobs', 'user', opts.groupby)
            groups = list(print_stats(jobs, [getuser()], width=80,
                                      groupby=opts.groupby, indices=True))
            tokill = raw_input('\nEnter the "id_jobs" number to kill all ' +
                               'corresponding jobs\n}:---> ')
            kill = raw_input('**********\n  You selected to kill ' +
                             '%s jobs occupying %s CPUs [y|n]: ' %
                             (len(groups[int(tokill)]),
                              count_procs(groups[int(tokill)])))
            if kill == 'y' or kill == 'yes':
                print 'bang bang!'
                kill_jobs(groups[int(tokill)])
            else:
                print 'aborted...'
            break
        elif s == 'p':
            system('tput cnorm')
            print 'job list grouped by %s:' % (opts.groupby)
            print '%-9s %9s %s' % ('id_jobs', 'user', opts.groupby)
            groups = list(print_stats(jobs, [getuser()], width=80,
                                      groupby=opts.groupby, indices=True))
            topause = raw_input('\nEnter the "id_jobs" number to pause/suspend all ' +
                                'corresponding jobs\n}:---> ')
            try:
                pause = check_pause(groups[int(topause)])
            except ValueError:
                print 'aborted...'
                break
            pause_jobs(groups[int(topause)], pause)
            break
    if toreload:
        print 'reloading...'
        return True
    else:
        return False


def main():
    """
    main function
    """
    opts = get_options()
    jobs = {}
    toreload = True
    while True:
        try:
            if toreload:
                for job in jobs:
                    jobs[job]['STATE'] = 'DONE'
                    jobs[job]['NODELIST(REASON)'] = ''
                jobs.update(get_list())
            if opts.user != 'all':
                users = [opts.user]
            else:
                users = sorted(list(set([jobs[j]['USER'] for j in jobs])))
            list(print_stats(jobs, users, width=opts.width, # jobsname=opts.jobsname,
                             groupby=opts.groupby)) # put list here because of yield inside... not nice
            if not opts.watch:
                break
            toreload = console(opts, jobs, users)
        except KeyboardInterrupt:
            print '\nbye-bye %s ;)\n' % getuser()
            print bye()
            system('tput cnorm')
            exit()


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
    parser.add_option('-u', dest='user', default=getuser(),
                      help='[%default] username (-u all to see everybody).')
    parser.add_option('--nostats', action='store_false',
                      dest='stats', default=True, 
                      help='[%default] job stats.')
    # parser.add_option('--jobs', action='store', default=None,
    #                   dest='jobsname',
    #                   help='Job name to search for in log (only for francois).')
    parser.add_option('--watch', action='store_true', default=False,
                      dest='watch', help='[%default] watch (ctrl-c to quit).')
    parser.add_option('--refresh', action='store', default=10,
                      dest='refresh',
                      help=('[%default] refresh rate in MINUTES,' +
                            'used with watch'))
    parser.add_option('--groupby', action='store', default=None,
                      dest='groupby',
                      help=('[%default] group user jobs by time limit (groupby=time).'))
    parser.add_option('--width', action='store', default=82,
                      dest='width',
                      help='[%default] display width (should be > 80)')

    opts = parser.parse_args()[0]
    opts.refresh = int(float(opts.refresh) * 60)
    opts.width = int(opts.width)
    opts.groupby = 'TIMELIMIT' if opts.groupby=='time' else None
    return opts


def bye():
    return ['\n Bye-bye\n', '\n Talogo!!\n', '\n Have a nice day\n',
            '\n And they lived happily ever after and they had a lot of CPUs\n',
            '\n The End.\n', '\n Au revoir\n', 
            '\n Nooooo come back!!!! Quick!!\n', '\n Thanks.. I really enjoyed\n',
            "\n I'm a poor lonesome cowboy, and a long way from home...\n",
            '\n Flying Spaghetti Monster be with you.\n'] [int (str (time())[-1])]


class _Getch:
    """Gets a single character from standard input.  Does not echo to the screen."""
    def __init__(self):
        self.impl = _GetchUnix()

    def __call__(self, refresh):
        return self.impl(refresh)


class _GetchUnix:
    def __init__(self):
        import tty

    def __call__(self, refresh):
        import tty, termios
        fd = stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(stdin.fileno())
            rlist, _, _ = select([stdin], [], [], refresh)
            if not rlist:
                return 'rr'
            ch = stdin.read(1)
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
    try:
        exit(main())
    except Exception as e:
        system('tput cnorm')
        print 'ERROR:', e
