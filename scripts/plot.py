import matplotlib.pyplot as plt

from collections import defaultdict

names = ['Hydrofoil', 'Raft+', 'Ben-Or+']
num_runs = 10
all_throughputs = None
time = None

plt.rcParams["font.family"] = "Times New Roman"

for j in range(len(names)):
    throughputs = defaultdict(float)
    for i in range(j*num_runs, (j+1)*num_runs):
        with open(f'client-{i}.throughput.txt', 'r') as f:
            for line in f:
                # Split the line into columns
                columns = line.split()
                if len(columns) < 5:
                    continue    
                # Extract the 2nd and 3rd columns
                throughputs[float(columns[0])] += float(columns[3])

    throughputs_list = sorted(throughputs.items()) # sorted by key, return a list of tuples
    time, one_system_throughput = zip(*throughputs_list) # unpack a list of pairs into two tuples

    time = list(time)[100:300]
    one_system_throughput = list(one_system_throughput)[100:300]

    time = [t-time[0] for t in time]
    one_system_throughput = [t/num_runs for t in one_system_throughput]

    if all_throughputs == None:
        all_throughputs = [one_system_throughput]
    else:
        all_throughputs.append(one_system_throughput)

plt.figure()
# Create a scatter plot of col2_list vs col3_list
plt.axvline(x = 1, color = 'black', linestyle = 'dotted')
plt.axvline(x = 3, color = 'black', linestyle = 'dotted')

plt.plot(time, all_throughputs[0], label = names[0], color='blue')
plt.plot(time, all_throughputs[1], label = names[1], color='red')
plt.plot(time, all_throughputs[2], label = names[2], color='green')

# Add axis labels and a title
plt.xlabel('Time (s)')
plt.ylabel('Throughput (ops/s)')
plt.title(f'Max Throughput Over Time (5 Replicas)')
plt.legend()

# dict_design = dict(facecolor='black', shrink=0.05, width=0.7, headwidth=5, headlength=7, alpha=0.5)
dict_design = dict(facecolor='black', shrink=0.05, width=0.7, headwidth=5, headlength=7)
plt.annotate('Leader Failure',xy=(1,3000),xytext=(1.5,3000),arrowprops=dict_design, va='center')
plt.annotate('Leader Recovery',xy=(3,200),xytext=(1.7,200),arrowprops=dict_design, va='center')

# Display the plot
plt.savefig(f'throughput', dpi=300)