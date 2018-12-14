import csv
import time, datetime
import os, sys


# used to simulate
class Trace:
    def __init__(self, trace_id, i, op):
        self.trace_ls = []
        # permanent failure
        if op == 'p':
            path_name = "./lib/tracelib/failure_events/s"
        elif op == 't':
            path_name = "./lib/tracelib/transient_events/s"
        elif op == 'r':
            path_name = "./lib/tracelib/transient_repair/s"
        fname = path_name + str(trace_id) + 'n' + str(i) + ".txt"
        if os.path.exists(fname) is True:
            tracefile = open(fname, "r")
            for line in tracefile:
                line = line.strip('\n')
                self.trace_ls.append(float(line))

    def get_trace_ls(self):
        return self.trace_ls


# parse trace
class Parser:

    TRANSIENT = "transient failure without data loss"
    PERMANENT = "permanent failure with data loss"

    def __init__(self, trace_id, mission_time=87600):
        # pick up some systems and nodes from trace.csv and get failure/repair time
        self.mission_time = mission_time
        self.trace_entry = []
        if os.path.exists("./lib/tracelib/failure_events/") is False:
            os.mkdir("./lib/tracelib/failure_events/")
        if os.path.exists("./lib/tracelib/transient_events/") is False:
            os.mkdir("./lib/tracelib/transient_events/")
        if os.path.exists("./lib/tracelib/transient_repair/") is False:
            os.mkdir("./lib/tracelib/transient_repair/")
        # input trace id, which needs to be mapped to system_id in data.csv     
        self.id_dict = [7, 24, 22, 8, 20, 21, 18, 19, 3, 4, 5, 6, 14, 9, 10, 11, 13, 12, 16, 2, 23, 15]
        self.system_id = self.id_dict[int(trace_id)-1] 
        self.sum_nodes = 0
        with open('./lib/tracelib/data/trace.csv', 'rb') as csvfile:
            tracereader = csv.DictReader(csvfile)
            for row in tracereader:
                if int(row['System']) == self.system_id:
                    if self.sum_nodes == 0:
                        self.sum_nodes = int(row['nodes'])
                    # here: may cause inaccuracy
                    # the first record is in May, 1995
                    if row['node prod'] == 'before tracking':
                        (prod_y, prod_mon) = (1995, 1)
                    else:
                        (prod_y, prod_mon) = self.year_month_convert(row['node prod'])
                    # here: may cause inaccuracy
                    # the last record is on Sep 9, 2005
                    if row['node decom'] == 'current':
                        end_period = self.date_convert_hour(prod_y, prod_mon, '11/01/2005 0:0')
                    else:
                        (decom_year, decom_mon) = self.year_month_convert(row['node decom'])
                        end_period = self.date_convert_hour(prod_y, prod_mon, str(decom_mon) + '/01/' + str(decom_year) + ' 0:0')
                    root_cause = ""
                    cause_id = 0
                    if row['Facilities'] != "":
                        cause_id = 1
                        root_cause = row['Facilities']
                    elif row['Hardware'] != "":
                        cause_id = 2
                        root_cause = row['Hardware']
                    elif row['Human Error'] != "":
                        cause_id = 3
                        root_cause = row['Human Error']
                    elif row['Network'] != "":
                        cause_id = 4
                        root_cause = row['Network']
                    elif row['Undetermined'] != "":
                        cause_id = 5
                        root_cause = row['Undetermined']
                    elif row['Software'] != "":
                        cause_id = 6
                        root_cause = row['Software']
                    # print prod_y, prod_mon,row['Prob Started (mm/dd/yy hh:mm)']
                    start_time = self.date_convert_hour(prod_y, prod_mon, row['Prob Started (mm/dd/yy hh:mm)'])
                    end_time = self.date_convert_hour(prod_y, prod_mon, row['Prob Fixed (mm/dd/yy hh:mm)'])
                    # here: It will be inaccurate from 'down time'
                    down_hour = end_time - start_time
                    type = self.category_failure(cause_id, root_cause, down_hour)
                    self.trace_entry.append(
                        (int(row['System']), int(row['nodenumz']), start_time, end_time, down_hour, end_period, type))
        self.trace_entry = list(set(self.trace_entry))
        self.trace_entry = sorted(self.trace_entry, key=lambda item: (item[0], item[1], item[2]))
        print "sum_nodes", self.sum_nodes
        return None

    # category transient failure and permanent failure
    def category_failure(self, cause_id, root_cause, down_hour):
        # Facilities and Network are transient failures
        if cause_id == 1 or cause_id == 4:
            return self.TRANSIENT
        # Hardware and Software
        elif cause_id == 2 or cause_id == 6:
            if root_cause.find('Disk') != -1 or root_cause.find('SCSI') != -1 or root_cause.find('Drive') != -1 \
                    or root_cause.find('SAN') != -1:
                return self.PERMANENT
            else:
                return self.TRANSIENT
        # Human Error and Undetermined
        elif cause_id == 3 or cause_id == 5:
            if down_hour > float(0.25):
                return self.PERMANENT
            else:
                return self.TRANSIENT

    # get the number of nodes in the system
    def get_sum_nodes(self):
        return self.sum_nodes

    # get node production time
    def year_month_convert(self, node_prod):
        a, b = node_prod.split('-')
        if a.isdigit():
            year = '200' + a
            mon = time.strptime(b, '%b').tm_mon
        else:
            year = '19' + b
            mon = time.strptime(a, '%b').tm_mon
        return (year, mon)

    # convert date to hour according to production time
    def date_convert_hour(self, prod_y, prod_mon, date_time):
        mdy, hm = date_time.split(' ')
        mon, day, year = mdy.split('/')
        hour, minute = hm.split(':')
        t = datetime.datetime(int(year), int(mon), int(day), int(hour), int(minute))
        # ret_hour is related to node production time. E.g., in system 3, node prod = 2003/09
        ret_hour = float((t - datetime.datetime(int(prod_y), int(prod_mon), 1, 0, 0, 0)).total_seconds() * 1.0 / 3600)
        return ret_hour

    def get_trace_id(self, system_ID):
        for idx in range(len(self.id_dict)):
            if self.id_dict[idx] == system_ID:
                return idx+1

    # write failure events of each node to files
    def write_failure_events(self, trace_ID, node, failure_event, type):
        file_name = 's' + str(trace_ID) + 'n' + str(node) + '.txt'
        # print type
        if type == self.TRANSIENT:
            path_name = "./lib/tracelib/transient_events/"
        else:
            path_name = "./lib/tracelib/failure_events/"
        with open(path_name + file_name, "w") as ff:
            check = 0
            if failure_event != None:
                for item in failure_event:
                    if check != 0:
                        assert (item >= check)  # e.g., system 20 both node 171 and node 224 have two failures at the same time
                    check = item
                    ff.write(str(item))
                    ff.write('\n')

    def write_repair_events(self, trace_ID, node, repair_event):
        file_name = 's' + str(trace_ID) + 'n' + str(node) + '.txt'
        path_name = "./lib/tracelib/transient_repair/"
        with open(path_name + file_name, "w") as fr:
            for item in repair_event:
                fr.write(str(item))
                fr.write('\n')

    # extend the failure and repair events to mission time
    def extend_to_mission_time(self, period, mission_time, type, failure_event, repair_event=None):
        failure_num = len(failure_event)
        n = int(mission_time / period)
        # print period, n
        tmp_f = []

        if type == self.PERMANENT:
            for i in xrange(1, n):
                # print i
                tmp_f += [item + period * i for item in failure_event]
            curr_time = period * n
            for idx in range(failure_num):
                # failure event time > mission time (Noted that repair completed time may exceed the mission time)
                if curr_time + failure_event[idx] > mission_time:
                    break
                curr_time += failure_event[idx]
                tmp_f.append(curr_time)
                # In some system and its nodes, repair time is 0.
            failure_event += tmp_f
            return failure_event
        elif type == self.TRANSIENT:
            tmp_r = []
            for i in xrange(1, n):
                # print i
                tmp_f += [item + period * i for item in failure_event]
                tmp_r += [item for item in repair_event]
            curr_time = period * n
            for idx in range(failure_num):
                # failure event time > mission time (Noted that repair completed time may exceed the mission time)
                if curr_time + failure_event[idx] > mission_time:
                    break
                curr_time += failure_event[idx]
                tmp_f.append(curr_time)
                # In some system and its nodes, repair time is 0.
                tmp_r.append(repair_event[idx])
            failure_event += tmp_f
            repair_event += tmp_r
            return (failure_event, repair_event)

    # parse trace of each nodes, extend to mission time and output to failure/repair files
    def parse_traces(self):
        failure_event = []
        transient_event = []
        transient_repair = []
        system_ID = -1
        node_ID = -1
        period = -1
        node_num = 0
        sum_node = 0
        for row in self.trace_entry:
            if system_ID == -1:
                system_ID = row[0]
            if node_ID == -1:
                node_ID = row[1]
                period = row[5]
            elif row[1] != node_ID or row[0] != system_ID:  # will go to next node
                if row[6] == self. TRANSIENT:
                    (transient_event, transient_repair) = self.extend_to_mission_time(period, self.mission_time, row[6],
                                                                                  transient_event, transient_repair)
                elif row[6] == self.PERMANENT:
                    failure_event = self.extend_to_mission_time(period, self.mission_time, row[6], failure_event)
                trace_ID = self.get_trace_id(system_ID)
                self.write_failure_events(trace_ID, node_ID, transient_event, self.TRANSIENT)
                self.write_repair_events(trace_ID, node_ID, transient_repair)
                self.write_failure_events(trace_ID, node_ID, failure_event, self.PERMANENT)
                node_ID = row[1]
                if row[0] != system_ID:
                    node_num += 1
                    print "System %d has %d nodes with failure events." % (system_ID, node_num)
                    system_ID = row[0]
                    node_num = 0
                else:
                    node_num += 1
                sum_node +=1
                period = row[5]
                transient_event = []
                transient_repair = []
                failure_event = []
            if row[6] == self.TRANSIENT:
                transient_event.append(row[2])
                transient_repair.append(row[4])
            else:
                failure_event.append(row[2])
        # assert(len(failure_event)!=0)
        # assert(len(transient_event)!=0)
        (transient_event, transient_repair) = self.extend_to_mission_time(period, self.mission_time, self.TRANSIENT,
                                                                          transient_event, transient_repair)
        failure_event = self.extend_to_mission_time(period, self.mission_time, self.PERMANENT, failure_event)
        trace_ID = self.get_trace_id(system_ID)
        self.write_failure_events(trace_ID, node_ID, transient_event, self.TRANSIENT)
        self.write_repair_events(trace_ID, node_ID, transient_repair)
        self.write_failure_events(trace_ID, node_ID, failure_event, self.PERMANENT)
        node_num += 1
        sum_node += 1
        print "System %d has %d nodes with failure events." %(trace_ID, node_num)
        print "Sum nodes having failure events", sum_node




if __name__ == '__main__':
    system_id = 9
    parser = Parser(system_id)
    # print parser.trace_entry
    parser.parse_traces()
    test_trace = Trace(system_id, 0, 'p')
    trace_ls = test_trace.get_trace_ls()
    print trace_ls
