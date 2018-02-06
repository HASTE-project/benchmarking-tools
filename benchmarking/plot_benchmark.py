import matplotlib.pyplot as plt
import ast

"""
Reads the output from 'benchmark.py' and plots it:
"""

filename = '/Users/benblamey/projects/haste/results/2018_02_06__14_52_29-hio-benchmarking.txt'

results = []

with open(filename) as f:
    for line in f:
        # expectline line to be a printed 'dict':
        line_dict = ast.literal_eval(line)
        results.append(line_dict)

y_durations = [float(result['total']) - float(result['prepare']) for result in results]
x_number_of_containers = [int(result['container_count']) for result in results]

#row = ast.literal_eval("{'benchmarking': 'benchmarking', 'file': 'benchmark_full_pipeline', 'topic': 'full', 'description': '', 'started_at_time': '1517923501.820833', 'ended_time': '1517923542.4397023', 'duration_secs': '40.61886930465698', 'number_of_bytes': '-1', 'container_count': 1}")

print(results)

fig = plt.figure()
ax = fig.add_subplot(111)

ax.scatter(x_number_of_containers, y_durations)

ax.set_xlabel('number of containers')
ax.set_ylabel('seconds to process')
ax.set_title('Total time to run whole HASTE pipeline, 500 images')

plt.show()
