import pandas as pd
import sys
import numpy as np


class Handle_Dataframe:

    def __init__(self, _file):
        self._file = _file

    def get_dataframe(self):
        _df = pd.read_csv(self._file)
        _df.loc[-1] = _df.columns.values[0]
        _df.index = _df.index + 1
        _df_sorted = _df.sort_index()
        _df2 = _df_sorted.rename({str(_df_sorted.columns.values[0]): 'Data'}, axis=1)
        _timed_column = _df2['Data'].str[:13].astype('float')
        _data_column = _df2['Data'].str[13:].astype('int')
        _converted_df = pd.DataFrame({'Job id': np.arange(len(_timed_column)),'Time': _timed_column, 'Service Demand': _data_column})
        # _converted_df['Interarrival'] = np.append(_converted_df['Time'][0], np.diff(_converted_df['Time']))

        _converted_df['Service Left'] = _converted_df['Service Demand']
        _converted_df['Service Requirement'] = np.ones(len(_timed_column))*np.inf
        _converted_df['Response Time'] = np.zeros(len(_timed_column))
        self.df = _converted_df  # .iloc[:20, :]
        return



cpu = float(sys.argv[2])
# cpu = 100#int(sys.argv[2])
filename = sys.argv[1]
# filename = 'test1.TL'#sys.argv[1]
df_obj = Handle_Dataframe(filename)
df_obj.get_dataframe()
df = df_obj.df
N, N_col = df.shape # Total number of jobs to be served


clock = 0  # Time
t_A = 0   # Next arrival time
N_A = 0   # Number of arrivals
t_S = float('inf')
L = 0      # Number of jobs in the system
id = 0
ps_cpu = cpu
serviced_jobs = []
while True:
    if t_A < t_S:
        if L == 0:
            my_queue = np.zeros((1, N_col))
            L += 1
            ps_cpu = cpu / L

            new_job = np.array(df.iloc[id,:])
            new_job[4] = new_job[2] / cpu
            t_A = new_job[1]
            clock = t_A
            new_job = np.reshape(new_job,(1,N_col))
            my_queue = np.append(my_queue, new_job, axis=0)
            my_queue = np.delete(my_queue, (0), axis=0)
            my_queue = np.array(my_queue)
            my_queue = my_queue[my_queue[:, 3].argsort()]
            id += 1
            t_S = clock + my_queue[0][3] / ps_cpu
            if id < N:
                t_A = df['Time'][id]
            else:
                t_A = float('inf')
        else:
            L += 1
            processed_service = (t_A - clock)*ps_cpu
            my_queue[:,3] = my_queue[:,3] - processed_service
            clock = t_A
            new_job = np.array(df.iloc[id, :])
            new_job[4] = new_job[2] / cpu
            new_job = np.reshape(new_job, (1, N_col))
            my_queue = np.append(my_queue, new_job, axis=0)
            my_queue = my_queue[my_queue[:, 3].argsort()]
            id += 1
            if id < N:
                t_A = df['Time'][id]
            else:
                t_A = float('inf')
            ps_cpu = cpu/L
            t_S = clock + (my_queue[0][3] / ps_cpu)

    elif t_A > t_S:
        processed_service = (t_S - clock) * ps_cpu
        my_queue[:, 3] = my_queue[:, 3] - processed_service
        n_C = np.where(my_queue[:, 3] <= 1e-7)
        clock = t_S
        for c in n_C[0]:
            response_time = clock - my_queue[c][1]
            slowdown = response_time / my_queue[c][4]
            completed_job = [my_queue[c][0], response_time, slowdown]
            serviced_jobs.append(completed_job)
        my_queue = np.delete(my_queue, (n_C[0]), axis=0)
        L = L - len(n_C[0])

        if L == 0:
            ps_cpu = cpu
            t_S = float('inf')
        else:
            ps_cpu = cpu / L
            t_S = clock + my_queue[0][3] / ps_cpu

        if t_A == float('inf') and t_S == float('inf'):
            break

    elif t_A == t_S:
        processed_service = (t_A - clock) * ps_cpu
        my_queue[:, 3] = my_queue[:, 3] - processed_service
        n_C = np.where(my_queue[:, 3] <= 1e-7)
        clock = t_A
        for c in n_C[0]:
            response_time = clock - my_queue[c][1]
            slowdown = response_time / my_queue[c][4]
            completed_job = [my_queue[c][0], response_time, slowdown]
            serviced_jobs.append(completed_job)
        L = L - len(n_C[0])
        my_queue = np.delete(my_queue, (n_C[0]), axis=0)
        new_job = np.array(df.iloc[id, :])
        new_job[4] = new_job[2] / cpu
        new_job = np.reshape(new_job, (1, N_col))
        my_queue = np.append(my_queue, new_job, axis=0)
        my_queue = my_queue[my_queue[:, 3].argsort()]
        L += 1
        id += 1
        if id < N:
            t_A = df['Time'][id]
        else:
            t_A = float('inf')

        t_S = clock + my_queue[0][3] / ps_cpu


serviced_jobs = np.array(serviced_jobs)

mean_response_time = np.mean(serviced_jobs[:,1])
var_response_time = np.var(serviced_jobs[:,1])
slowdown_average = np.mean(serviced_jobs[:,2])

print(f'response time average: {mean_response_time}')
print(f'response time variance: {var_response_time}')
print(f'slowdown average: {slowdown_average}')

