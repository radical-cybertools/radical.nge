
# radical.nge

NGE pilot

nge = NGE(url='http://user:pass@host/')

print nge.list_sessions()
nge.session('session.1')
print nge.session()
        
nge.request_resources([{'resource' : 'local.localhost',
                        'cores'    : 160,
                        'walltime' : 20}])
tasks = list()
for _ in range(unum):
    tasks.append({'executable'       : '/bin/date',
                  'cpu_processes'    : 1,
                  'cpu_threads'      : 2,
                  'cpu_process_type' : rp.POSIX,
                  'cpu_thread_type'  : rp.POSIX,
                  'cpu_process_type' : 'fork',
                })
nge.submit_tasks(tasks)

print nge.list_tasks()
print nge.get_task_states()

nge.wait_task_states(states=rp.FINAL)

print nge.get_task_states()

nge.cancel_resources()
nge.close()  # close session, cancel resources

