"""
19 Mar 2014


"""

from subprocess import Popen, PIPE
from optparse   import OptionParser
from sys        import stdout, stdin
from getpass    import getuser
from time       import time, sleep, mktime
from select     import select
from datetime   import datetime
from os.path    import expanduser, join as path_join, exists
from cPickle    import load, dump


################################################################################
## globals
CMD = '/opt/perf/bin/squeue -o "%a %u %i %j %T %M %l %C %D %q %P %p %R"'
CMDSACCT  = '/usr/local/bin/sacct -j %s --format JobId,State,Start,End -X -n'

BEGTIME = time() # in the begining of time...
AUTOWIDTH = True

TIME_ROUND = 15000 # used to group jobs by time (600 corresponds to +- 5 min)

CFG_PATH = path_join(expanduser('~'), '.slurm_monitor.cfg')

if exists(CFG_PATH):
    JOBS = load(open(CFG_PATH))
################################################################################
    
def update_job_list():
    """
    Update the list of jobs running, with maximum amount of information :)
    """
    table = Popen(CMD, shell=True, stdout=PIPE).communicate()[0]
    headers = table.split('\n')[0].split()
    for line in table.split('\n')[1:]:
        if not line:
            continue
        jobid = line.split()[2]
        JOBS.setdefault(jobid, {})
        for i, val in enumerate(line.split()):
            JOBS[jobid][headers[i]] = val


class JobGroup(object):
    def __init__(self, name, jobs):
        self.name = name
        self.jobs = jobs

    def print_stats(self, expanded=2):
        pass

    def subjobs(self, field, val, inverse=False, test=None):
        """
        :param field: fields can be one of:
           'ACCOUNT', 'CPUS', 'JOBID', 'NAME', 'NODELIST(REASON)', 'NODES', 
           'PARTITION', 'PRIORITY', 'QOS', 'STATE', 'TIME', 'TIMELIMIT', 'USER'
        :param val: check if the value of this field is a function of this 'val'
           (default is equal, see 'test' parameter)
        :param False inverse: get jobs with a value different from 'val' at
           'field'
        :param None test: a function to do a specific test on 'field', instead
           of equality or differnce.

        :returns: a dictionary of jobs that passed the test
        """
        if not test:
            same = lambda x, y: x == y
            diff = lambda x, y: x != y
            test = diff if inverse else same
        return dict([(j, self.jobs[j]) for j in self.jobs
                     if test(self.jobs[j][field], val)])


class Monitor(object):
    """
    :param 2 expanded: how much information is displayed from 0 to 2
    """
    def __init__(self, expanded=2):
        self.expanded = expanded

    def clean(self):
        pass

    def update(self):
        update_job_list()
        self.refresh()

    def refresh(self):
        pass
        
    

def main():
    """
    main function
    """
    pass


if __name__ == "__main__":
    exit(main())
