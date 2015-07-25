import os
import sys
import re
import subprocess

from dispatch import DispatchDriver
from helpers import *

class SGEDriver(DispatchDriver):
    def _script_text(job, job_file):
        preamble = '''#!/bin/bash
    #PBS -S /bin/bash

    cd $PBS_O_WORKDIR
    echo "Current working directory is `pwd`"
    echo "Starting run at: `date`"'''
        cleanup = 'rm %s' % job_file
        end = '''echo "Job finished with exit code $? at: `date`"'''
        return '\n'.join([preamble, job, cleanup, end])

    def submit_job(self, job):
        output_file = job_output_file(job)
        job_file    = job_file_for(job)
        mint_path   = sys.argv[0]
        sge_script  = 'python %s --run-job "%s" .' % (mint_path, job_file)

        script_name = '%s-%d.pbs' % (job.name, job.id)
        with open(script_name, 'w') as f:
            f.write(self._script_text(sge_script, script_name))

        qsub_cmd    = ['qsub', '-S', '/bin/bash',
                       '-N', "%s-%d" % (job.name, job.id),
                       '-e', output_file,
                       '-o', output_file,
                       script_name
                      ]

        out = subprocess.check_output(qsub_cmd)
        self.queue_name = out.split('.')[1]

        if match:
            return int(out.split('.')[0])
        else:
            return None, out

    def parse_code(code):
        status_codes = {'C': 'completed after having run',
                        'E': 'exiting after having run.',
                        'H': 'held.',
                        'Q': 'queued, eligible to run or routed.',
                        'R': 'running.',
                        'T': 'being moved to new location.',
                        'W': 'waiting for its execution time'}

        return status_codes.get(code, 'Status Code Unknown: %s' % code)

    def is_proc_alive(self, job_id, sgeid):
        queue_id = '%d.%s' % (job_id, self.queue_name)
        status = subprocess.check_output(['qstat', queue_id])

        if status == 'qstat: Unknown Job Id %s' % queue_id:
            log("Job %d (%d) is finished.\n" % (job_id, sgeid))
            return False
        else:
            stat = status.split('\n')[2].split()
            code = stat[-2]
            log("Job %d (%d) is %s\n" % (job_id, sgeid,
                self.parse_code(code)))
            return True


def init():
    return SGEDriver()
