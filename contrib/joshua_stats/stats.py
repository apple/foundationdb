import xml.etree.ElementTree as ET
import numpy as np
import matplotlib.pyplot as plt
import io
import sys

FONT_SIZE = 16
FIG_SIZE = (4.5, 4.5)

def get_realtime(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	times = []
	for child in root:
		if "Command" not in child.attrib:
			continue
		if "noSim" in child.attrib["Command"]:
			continue
		if "SimElapsedTime" not in child.attrib:
			continue
		if "RealElapsedTime" not in child.attrib:
			continue
		time = float(child.attrib["RealElapsedTime"])
		times.append(time)
	return times

def get_simtime(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	times = []
	for child in root:
		if "Command" not in child.attrib:
			continue
		if "noSim" in child.attrib["Command"]:
			continue
		if "SimElapsedTime" not in child.attrib:
			continue
		if "RealElapsedTime" not in child.attrib:
			continue
		time = float(child.attrib["SimElapsedTime"])
		times.append(time)
	return times

def get_test_stats(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	times = dict()
	for child in root:
		if "Command" not in child.attrib:
			continue
		if "SimElapsedTime" not in child.attrib:
			continue
		if "RealElapsedTime" not in child.attrib:
			continue
		test = child.attrib["Command"].split(" ")[4]
		realtime = float(child.attrib["RealElapsedTime"])
		if test not in times:
			times[test] = []
		times[test].append(realtime)
	# Sort by number of elements in each list (ascending)
	sorted_times = dict(sorted(times.items(), key=lambda item: len(item[1])))
	return sorted_times

def get_speedup_long_simulation_time(filepath, threshold):
	tree = ET.parse(filepath)
	root = tree.getroot()
	speedups = []
	for child in root:
		if "Command" not in child.attrib:
			continue
		if "noSim" in child.attrib["Command"]:
			continue
		if "SimElapsedTime" not in child.attrib:
			continue
		if "RealElapsedTime" not in child.attrib:
			continue
		time_sim = float(child.attrib["SimElapsedTime"])
		time_real = float(child.attrib["RealElapsedTime"])
		if time_sim < threshold:
			continue
		ratio = time_sim * 1.0/time_real
		if ratio > 100:
			continue
		speedups.append(ratio)
	return speedups

def get_peakMemory(filepath):
	tree = ET.parse(filepath)
	root = tree.getroot()
	mems = []
	for child in root:
		if "Command" not in child.attrib:
			continue
		if "noSim" in child.attrib["Command"]:
			continue
		if "SimElapsedTime" not in child.attrib:
			continue
		if "RealElapsedTime" not in child.attrib:
			continue
		mem = float(child.attrib["PeakMemory"])*1.0/1024/1024
		mems.append(mem)
	return mems

def print_stats(data):
	p90 = np.percentile(data, 90)   # 90th percentile
	p50 = np.percentile(data, 50)   # median
	avg = np.mean(data)             # average
	print("P90:", p90)
	print("P50:", p50)
	print("Average:", avg)
	print("Data point count:", len(data))
	print("Total:", len(data) * avg)

def draw_realtime(times, do_y_log_scale, label):
	plt.figure(figsize=FIG_SIZE)
	if do_y_log_scale:
		plt.yscale('log')
	plt.hist(times, color='white', edgecolor='black', label="realtime", bins=10)
	plt.locator_params(axis='x', nbins=10)
	plt.xlabel('Physical execution time', fontsize=FONT_SIZE)
	if do_y_log_scale:
		plt.ylabel('Frequency (Log(*))', fontsize=FONT_SIZE)
	else:
		plt.ylabel('Frequency', fontsize=FONT_SIZE)
	# plt.title('Physical execution time of 100,000 tests', fontsize=FONT_SIZE)
	plt.xticks(fontsize=FONT_SIZE, rotation=45, ha='right')
	plt.yticks(fontsize=FONT_SIZE)
	ax = plt.gca()
	ax.tick_params(axis='x', which='major', length=10, width=2)
	ax.tick_params(axis='y', which='major', length=10, width=2)
	ax.tick_params(axis='x', which='minor', length=5, width=1)
	ax.tick_params(axis='y', which='minor', length=5, width=1)
	plt.savefig(label + "realtime.png", bbox_inches='tight', dpi=300)
	plt.close()

def draw_simtime(times, do_y_log_scale, label):
	plt.figure(figsize=FIG_SIZE)
	if do_y_log_scale:
		plt.yscale('log')
	plt.hist(times, color='white', edgecolor='black', label="simtime", bins=10)
	plt.locator_params(axis='x', nbins=10)
	plt.xlabel('Simulated execution time', fontsize=FONT_SIZE)
	if do_y_log_scale:
		plt.ylabel('Frequency (Log(*))', fontsize=FONT_SIZE)
	else:
		plt.ylabel('Frequency', fontsize=FONT_SIZE)
	# plt.title('Simulated execution time of 100,000 tests', fontsize=FONT_SIZE)
	plt.xticks(fontsize=FONT_SIZE, rotation=45, ha='right')
	plt.yticks(fontsize=FONT_SIZE)
	ax = plt.gca()
	ax.tick_params(axis='x', which='major', length=10, width=2)
	ax.tick_params(axis='y', which='major', length=10, width=2)
	ax.tick_params(axis='x', which='minor', length=5, width=1)
	ax.tick_params(axis='y', which='minor', length=5, width=1)
	plt.savefig(label + "_simtime.png", bbox_inches='tight', dpi=300)
	plt.close()

def draw_memory(mems, do_y_log_scale, label):
	plt.figure(figsize=FIG_SIZE)
	if do_y_log_scale:
		plt.yscale('log')
	plt.hist(mems, color='white', edgecolor='black', label="peakmemory", bins=10)
	plt.locator_params(axis='x', nbins=10)
	plt.xlabel('Peak memory usage (MB)', fontsize=FONT_SIZE)
	if do_y_log_scale:
		plt.ylabel('Frequency (Log(*))', fontsize=FONT_SIZE)
	else:
		plt.ylabel('Frequency', fontsize=FONT_SIZE)
	# plt.title('Peak memory usage of 100,000 tests'.format(filepath), fontsize=FONT_SIZE)
	plt.xticks(fontsize=FONT_SIZE, rotation=45, ha='right')
	plt.yticks(fontsize=FONT_SIZE)
	ax = plt.gca()
	ax.tick_params(axis='x', which='major', length=10, width=2)
	ax.tick_params(axis='y', which='major', length=10, width=2)
	ax.tick_params(axis='x', which='minor', length=5, width=1)
	ax.tick_params(axis='y', which='minor', length=5, width=1)
	plt.savefig(label + "_memory.png", bbox_inches='tight', dpi=300)
	plt.close()

def draw_speedup_long_physical_time(speedups, do_y_log_scale, threshold, label):
	plt.figure(figsize=FIG_SIZE)
	if do_y_log_scale:
		plt.yscale('log')
	plt.xlim(min(speedups), max(speedups))
	plt.hist(speedups, color='white', edgecolor='black', label="speedup", bins=10)
	plt.locator_params(axis='x', nbins=10)
	plt.xlabel('Simulated time / physical time', fontsize=FONT_SIZE)
	if do_y_log_scale:
		plt.ylabel('Frequency (Log(*))', fontsize=FONT_SIZE)
	else:
		plt.ylabel('Frequency', fontsize=FONT_SIZE)
	# plt.title('Simulated time / physical time of long simulation tests'.format(filepath), fontsize=FONT_SIZE)
	plt.xticks(fontsize=FONT_SIZE, rotation=45, ha='right')
	plt.yticks(fontsize=FONT_SIZE)
	ax = plt.gca()
	ax.tick_params(axis='x', which='major', length=10, width=2)
	ax.tick_params(axis='y', which='major', length=10, width=2)
	ax.tick_params(axis='x', which='minor', length=5, width=1)
	ax.tick_params(axis='y', which='minor', length=5, width=1)
	plt.savefig(label + "_speedup_long_simtime" + "_" + str(threshold) + ".png", bbox_inches='tight', dpi=300)
	plt.close()


def main():
	filepath = sys.argv[1]

	figure_label = filepath.replace(".", "")

	realtimes = get_realtime(filepath)
	print("Real time data points:", len(realtimes))
	print_stats(realtimes)
	draw_realtime(realtimes, True, figure_label)

	simtimes = get_simtime(filepath)
	print("Sim time data points:", len(simtimes))
	print_stats(simtimes)
	draw_simtime(simtimes, True, figure_label)

	mems = get_peakMemory(filepath)
	print("Memory data points:", len(mems))
	print_stats(mems)
	draw_memory(mems, True, figure_label)

	simtime_threshold = 600
	speedups = get_speedup_long_simulation_time(filepath, simtime_threshold)
	print("Speedup data points:", len(speedups))
	print_stats(speedups)
	draw_speedup_long_physical_time(speedups, False, simtime_threshold, figure_label)

	time = get_test_stats(filepath)
	with open("simulatin-test-internal-stats.txt", "w") as f:
	    for test in time:
	        f.write(f"{test}\n")
	        old_stdout = sys.stdout
	        sys.stdout = f
	        print_stats(time[test])
	        sys.stdout = old_stdout
	        f.write("\n")

if __name__ == '__main__':
  main()
