'''
Hadi Rouhani

In this program we are simulating a G/G/1/K queueing system.

The time complexity of this program is O(n) where n is the number of jobs during the time simulation

'''

import pandas as pd
import sys
import numpy as np
import math

class Handle_Dataframe:

    def __init__(self, _file, _number_of_samples):
        self._file = _file
        self.num_of_samples = _number_of_samples

    def get_dataframe(self):
        _df = pd.read_csv(self._file)
        _df.loc[-1] = _df.columns.values[0]
        _df.index = _df.index + 1
        _df_sorted = _df.sort_index()
        _df2 = _df_sorted.rename({str(_df_sorted.columns.values[0]): 'Data'}, axis=1)
        _timed_column = _df2['Data'].str[:13].astype('float')
        _data_column = _df2['Data'].str[13:].astype('int')
        _converted_df = pd.DataFrame({'Time': _timed_column, 'Data': _data_column})
        _converted_df['Interarrival'] = np.append(_converted_df['Time'][0], np.diff(_converted_df['Time']))
        self.df = _converted_df  # .iloc[:20, :]
        return


class Node:

    def __init__(self, data):
        self.data = data
        self.next = None


class Queue:

    def __init__(self, system_cap):
        self.front = self.rear = None # System is initialized with no job in the system
        self.sys_size = 0       # number of jobs in the system initially is 0
        self.blocked_jobs = 0
        self.completed_jobs = 0
        self.sys_cap = system_cap
        self.job_start_time = 0
        self.job_service_time = 0
        self.job_complete_time = float('inf') # We consider inf when there is no job in the system
        self.idle_time = 0
        self.no_of_jobs_in_system = []
        self.prev_sys_size = 0

    def isEmpty(self):
        return self.front is None

    def HandleJob(self, item, clock):
        if self.sys_size == 0:
            self.job_start_time = clock
            self.job_service_time = item.service_time
            self.job_complete_time = clock + item.service_time
        self.no_of_jobs_in_system.append(self.sys_size)
        self.prev_sys_size = self.sys_size
        self.EnQueue(item)



    # Method to add an item to the queue
    def EnQueue(self, item):
        if self.sys_size <= self.sys_cap:
            temp = Node(item)
            if self.rear is None:
                self.front = self.rear = temp
                self.sys_size += 1
                return
            self.rear.next = temp
            self.rear = temp
            self.sys_size += 1

        else:
            self.blocked_jobs += 1
            return

    # Method to remove an item from queue
    def DeQueue(self, clock):
        self.prev_sys_size = self.sys_size
        if self.isEmpty():
            return
        temp = self.front
        self.front = temp.next
        self.completed_jobs += 1
        self.sys_size -= 1
        self.job_start_time = clock
        if self.sys_size != 0:
            self.front.data.service_end_time = clock + self.front.data.service_time
            self.job_complete_time = clock + self.front.data.service_time
        else:
            self.job_complete_time = float('inf')

        if self.front is None:
            self.rear = None


class Jobs:
    def __init__(self, job_id, arrival_time, service_time):
        self.job_id = job_id
        self.arrival_time = arrival_time
        self.service_time = service_time


class Simulate:
    def __init__(self, simulation_time, system_size, service_rate, df):
        self.clock = 0.0
        self.simulation_time = simulation_time
        self.system_size = system_size
        self.service_rate = service_rate
        self.df = df
        self.the_system = Queue(self.system_size)
        self.idle_time_starts = 0
        self.idle_times = 0

    def run(self):
        _id = 0
        df = self.df
        the_queue = self.the_system
        while self.clock <= self.simulation_time:
            if the_queue.sys_size == 0:
                self.idle_time_starts = self.clock



            if _id < len(df['Time']):
                next_arrival, next_service = df['Time'][_id], df['Data'][_id] / self.service_rate
                new_event = min(next_arrival, the_queue.job_complete_time)


            if new_event is next_arrival:
                self.clock = next_arrival
                next_arrival = 0
                the_queue.HandleJob(Jobs(_id, next_arrival, next_service), self.clock)
                _id += 1
            else:
                self.clock = the_queue.job_complete_time
                the_queue.DeQueue(self.clock)

            if the_queue.sys_size == 1 and the_queue.prev_sys_size == 0:
                self.idle_times += self.clock - self.idle_time_starts

            if _id == len(df['Time']):
                if math.isinf(the_queue.job_complete_time):
                    last_idle_time = (1 - the_queue.job_service_time)
                    self.idle_times += last_idle_time
                    break



if __name__ == "__main__":
  args = sys.argv
  queue_size = 10 #int(args[2])
  filename =  'test2.TL'#args[1] #
  L =  1250##int(args[3])
  T =  4 ##float(args[4])

  net_obj = Handle_Dataframe(filename, queue_size)
  net_obj.get_dataframe()
  dataframe = net_obj.df
  sim_obj = Simulate(T, queue_size, L, dataframe)
  sim_obj.run()
  print(f'no packets sent out: {sim_obj.the_system.completed_jobs}')
  print(f'no blocked arrivals: {sim_obj.the_system.blocked_jobs}')
  print('idle time of the server:', "%.6f"% sim_obj.idle_times)
  print('average number of packets seen in the gateway:', "%.2f"% np.mean(sim_obj.the_system.no_of_jobs_in_system))
