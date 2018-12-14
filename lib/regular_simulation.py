import logging
import random
import numpy.random as nprandom
from heapq import *
from simulation import Simulation
from state import State
from smp_data_structures import Rack, Node, Disk
from placement import Placement
from lib.tracelib.trace import Trace
from network import Network

formatter = logging.Formatter('%(asctime)-15s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)

# This class is inherited from Simulation
class RegularSimulation(Simulation):
    ##
    # __init__() from Simulation
    #

    ##
    # Initialize the simulation
    #
    def init(self):
        # Initialize the state of the system
        self.state = State(self.num_disks)

        # Employ priority queue to keep all the failures and repairs
        # The element in the queue is (event_time, event_type, device_id)
        self.events_queue = []

        # Keep failed disks awaiting repair
        self.wait_repair_queue = []

        # Keep delayed stripes due to unavailable nodes
        # Key is the disk_idx delayed, value is the list of delayed stripes
        self.delayed_repair_dict = dict()

        self.enable_transient_failures = False

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)
        # self.logger.setLevel(logging.INFO)
        self.logger.addHandler(console)
        self.logger.propagate = False


    ##
    # Reset the simulation
    #
    def reset(self, ite=0):
        # Generate node transient and permanent failure events from trace
        if self.use_trace:
            for i in xrange(self.num_nodes):
                self.nodes[i] = Node(None, None, None, Trace(self.trace_id, i, 'p'),
                                     Trace(self.trace_id, i, 't'), Trace(self.trace_id, i, 'r'))

        self.state = State(self.num_disks)

        for disk in self.disks:
            disk.init_clock(0)
            disk.init_state()
        for node in self.nodes:
            node.init_state()
        for rack in self.racks:
            rack.init_state()

        self.events_queue = []
        self.wait_repair_queue = []
        self.delayed_repair_dict = dict()

        # generate disk failures and put them into events_queue
        for disk_id in xrange(len(self.disks)):
            disk_fail_time = self.disk_fail_dists.draw()
            if disk_fail_time <= self.mission_time:
                self.events_queue.append((disk_fail_time, Disk.EVENT_DISK_FAIL, disk_id))
        # generate node failures and push them into events_queue
        for node_id in xrange(self.num_nodes):
            if not self.use_trace:
                self.events_queue.append((self.node_fail_dists.draw(),
                                          Node.EVENT_NODE_FAIL, node_id))
                if self.enable_transient_failures:
                    self.events_queue.append((self.node_transient_fail_dists.draw(),
                                          Node.EVENT_NODE_TRANSIENT_FAIL, node_id))
            else:
                for node_failure_time in self.nodes[node_id].node_fail_trace.get_trace_ls():
                    # push node failure event to event_queue
                    self.events_queue.append((node_failure_time, Node.EVENT_NODE_FAIL, node_id))
                node_transient_failure_ls = self.nodes[node_id].node_transient_fail_trace.get_trace_ls()
                node_transient_repair_ls = self.nodes[node_id].node_transient_repair_trace.get_trace_ls()
                for ls_idx in xrange(len(node_transient_failure_ls)):
                    node_transient_failure_time = node_transient_failure_ls[ls_idx]
                    node_transient_repair_time = node_transient_repair_ls[ls_idx]
                    self.events_queue.append((node_transient_failure_time, Node.EVENT_NODE_TRANSIENT_FAIL, node_id))
                    self.events_queue.append((node_transient_failure_time + node_transient_repair_time,
                                              Node.EVENT_NODE_TRANSIENT_REPAIR, node_id))

        # generate rack failures and push them into events_queue
        if not self.use_power_outage and self.enable_transient_failures:
            for rack_id in xrange(len(self.racks)):
                self.events_queue.append((self.rack_fail_dists.draw(), Rack.EVENT_RACK_FAIL, rack_id))

        # correlated failures caused by power outage
        if (not self.use_trace) and self.use_power_outage:
            for rack_id in xrange(self.num_racks):
                occur_time = float(0) + self.power_outage_dist.draw()
                while occur_time < self.mission_time:
                    self.events_queue.append((occur_time, Rack.EVENT_RACK_FAIL, rack_id))
                    occur_time += random.expovariate((1/float(self.power_outage_duration)))
                    self.events_queue.append((occur_time, Rack.EVENT_RACK_REPAIR, rack_id))
                    for i in xrange(self.nodes_per_rack):
                        # draw a bernoulli distribution
                        if nprandom.binomial(1, 0.01):
                            self.events_queue.append((occur_time, Node.EVENT_NODE_FAIL,
                                                      (self.nodes_per_rack * rack_id + i)))
                    occur_time += self.power_outage_dist.draw()

        heapify(self.events_queue)
        self.placement = Placement(self.num_racks, self.nodes_per_rack,
                                   self.disks_per_node, self.capacity_per_disk,
                                   self.num_stripes, self.chunk_size,
                                   self.code_type, self.n, self.k,
                                   self.place_type,
                                   self.chunk_rack_config, self.l)

        self.network = Network(self.num_racks, self.nodes_per_rack, self.network_setting)

        self.num_stripes_repaired = 0
        self.num_stripes_repaired_single_chunk = 0
        self.num_stripes_delayed = 0


    ##
    # Generate permanent disk failure event
    #
    def set_disk_fail(self, disk_idx, curr_time):
        heappush(self.events_queue, (self.disk_fail_dists.draw()+curr_time, Disk.EVENT_DISK_FAIL, disk_idx))


    ##
    # Generate repair event for permanent disk failure
    #
    def set_disk_repair(self, disk_idx, curr_time):
        if not self.use_network:
            # get the repair time from a pre-defined repair distribution
            heappush(self.events_queue, (self.disk_repair_dists.draw()+curr_time,
                                         Disk.EVENT_DISK_REPAIR, disk_idx))
        else:
            # repair time = cross-rack repair traffic / available cross-rack bandwidth
            rack_id = disk_idx / (self.nodes_per_rack * self.disks_per_node)

            # If there is no available bandwidth or the rack is under transient failure
            if self.network.get_avail_cross_rack_repair_bwth() == 0 or \
                self.racks[rack_id].get_curr_state() != Rack.STATE_RACK_NORMAL:
                heappush(self.wait_repair_queue, (curr_time, disk_idx))
            else:
                cross_rack_download = 0
                stripes_to_repair = self.placement.get_stripes_to_repair(disk_idx)
                self.num_stripes_repaired += len(stripes_to_repair)
                stripes_to_delay = []

                # for each stripe to repair
                for stripe_id in stripes_to_repair:
                    num_failed_chunk = 0
                    num_alive_chunk_same_rack = 0
                    num_unavail_chunk = 0
                    idx = 0
                    fail_idx = 0
                    alive_chunk_same_rack = []

                    # check the status of each chunk in the stripe
                    for disk_id in self.placement.get_stripe_location(stripe_id):
                        # get the total number of unavailable chunk (due to permanent/transient failures) in this stripe
                        if self.disks[disk_id].state != Disk.STATE_NORMAL:
                            num_unavail_chunk += 1

                        # for RS, DRC
                        if self.placement.code_type != Placement.CODE_TYPE_LRC:
                            if self.disks[disk_id].get_curr_state() == Disk.STATE_CRASHED:
                                num_failed_chunk += 1
                            elif (disk_id / (self.nodes_per_rack * self.disks_per_node)) == rack_id:
                                num_alive_chunk_same_rack += 1
                        # for LRC
                        else:
                            if self.disks[disk_id].get_curr_state() == Disk.STATE_CRASHED:
                                num_failed_chunk += 1
                                if disk_idx == disk_id:
                                    fail_idx = idx
                            elif (disk_id / (self.nodes_per_rack * self.disks_per_node)) == rack_id:
                                num_alive_chunk_same_rack += 1
                                alive_chunk_same_rack.append(idx)
                            idx += 1

                    # this is a single-chunk repair
                    if num_failed_chunk == 1:
                        self.num_stripes_repaired_single_chunk += 1
                    # the repair for this stripe is delayed
                    if num_unavail_chunk > (self.n - self.k):
                        stripes_to_delay.append(stripe_id)

                    # RS
                    if self.placement.code_type == Placement.CODE_TYPE_RS:
                        if num_alive_chunk_same_rack < self.k:
                            cross_rack_download += (self.k - num_alive_chunk_same_rack)
                    # LRC
                    elif self.placement.code_type == Placement.CODE_TYPE_LRC:
                        if num_failed_chunk == 1:
                            # global parity
                            if fail_idx in self.placement.lrc_global_parity:
                                if num_alive_chunk_same_rack < self.k:
                                    cross_rack_download += self.k - num_alive_chunk_same_rack
                            # data chunk or local parity
                            else:
                                # find which group that the failed chunk is in
                                fail_gid = 0
                                for gid in xrange(self.l):
                                    if fail_idx in self.placement.lrc_data_group[gid] or \
                                        fail_idx == self.placement.lrc_local_parity[gid]:
                                        fail_gid = gid
                                        break
                                # find how many chunk in the same rack can be used for repair
                                num_alive_chunk_same_rack = 0
                                for each in alive_chunk_same_rack:
                                    if each in self.placement.lrc_data_group[fail_gid] or \
                                        each == self.placement.lrc_data_group[fail_gid]:
                                        num_alive_chunk_same_rack += 1
                                if num_alive_chunk_same_rack < self.k/self.l:
                                    cross_rack_download += self.k/self.l - num_alive_chunk_same_rack
                        else:
                            if num_alive_chunk_same_rack < self.k:
                                cross_rack_download += (self.k - num_alive_chunk_same_rack)
                    # DRC
                    elif self.placement.code_type == Placement.CODE_TYPE_DRC:
                        if num_failed_chunk == 1:
                            if self.k == 5 and self.n == 9:
                                cross_rack_download += 1.0
                            elif self.k == 6 and self.n == 9:
                                cross_rack_download += 2.0
                            else:
                                print "Only support DRC - (9,6,3), (9,5,3)"
                        else:
                            if num_alive_chunk_same_rack < self.k:
                                cross_rack_download += (self.k - num_alive_chunk_same_rack)
                    else:
                        print "Not correct code type in set_disk_repair()!"


                repair_bwth = self.network.get_avail_cross_rack_repair_bwth()
                self.network.update_avail_cross_rack_repair_bwth(0)
                repair_time = cross_rack_download * self.chunk_size / float(repair_bwth) # seconds
                repair_time /= float(3600) # hours

                if len(stripes_to_delay) != 0:
                    self.num_stripes_delayed += len(stripes_to_delay)
                    self.delayed_repair_dict[disk_idx] = stripes_to_delay

                self.logger.debug("repair_time = %d, repair_bwth = %d" % (repair_time, repair_bwth))
                heappush(self.events_queue, (repair_time+curr_time, Disk.EVENT_DISK_REPAIR, disk_idx, repair_bwth))


    ##
    # Generate permanent node failure event
    #
    def set_node_fail(self, node_idx, curr_time):
        heappush(self.events_queue, (self.node_fail_dists.draw()+curr_time, Node.EVENT_NODE_FAIL, node_idx))


    ##
    # Generate repair event for permanent node failure
    # The repair for the failed node is conducted by the repair for the failed disks on that node
    #
    def set_node_repair(self, node_idx, curr_time):
        for i in xrange(self.disks_per_node):
            disk_idx = node_idx * self.disks_per_node + i
            self.set_disk_repair(disk_idx, curr_time)


    ##
    # Generate transient node failure event
    #
    def set_node_transient_fail(self, node_idx, curr_time):
        heappush(self.events_queue, (self.nodes[node_idx].node_transient_fail_distr.draw()+curr_time,
                                     Node.EVENT_NODE_TRANSIENT_FAIL, node_idx))


    ##
    # Generate repair event for transient node failure
    #
    def set_node_transient_repair(self, node_idx, curr_time):
        heappush(self.events_queue, (self.nodes[node_idx].node_transient_repair_distr.draw()+curr_time,
                                     Node.EVENT_NODE_TRANSIENT_REPAIR, node_idx))


    ##
    # Generate transient rack failure
    #
    def set_rack_fail(self, rack_idx, curr_time):
        heappush(self.events_queue, (self.rack_fail_dists.draw()+curr_time, Rack.EVENT_RACK_FAIL, rack_idx))


    ##
    # Generate repair for transient rack failure
    #
    def set_rack_repair(self, rack_idx, curr_time):
        heappush(self.events_queue, (self.rack_repair_dists.draw()+curr_time, Rack.EVENT_RACK_REPAIR, rack_idx))


    ##
    # Get the next event from the event queue
    #
    def get_next_event(self, curr_time):
        self.logger.debug("len(delayed_repair_dict) = %d, len(wait_repair_queue) = %d" %
                         (len(self.delayed_repair_dict), len(self.wait_repair_queue)))
        # If there are some stripes delayed
        if len(self.delayed_repair_dict) != 0:
            items_to_remove = [] # keep the key of the items to remove
            for key in self.delayed_repair_dict:
                tmp_dict_value = []
                for stripe_id in self.delayed_repair_dict[key]:
                    repair_delay = False
                    num_unavail_chunk = 0
                    for disk_idx in self.placement.get_stripe_location(stripe_id):
                        if self.disks[disk_idx].state != Disk.STATE_NORMAL:
                            num_unavail_chunk += 1
                        if num_unavail_chunk > (self.n - self.k):
                            repair_delay = True
                            break
                    if repair_delay: # stripe whose repair is delayed
                        tmp_dict_value.append(stripe_id)
                if len(tmp_dict_value) == 0:
                    items_to_remove.append(key)
                else:
                    self.delayed_repair_dict[key] = tmp_dict_value
            for key in items_to_remove:
                self.delayed_repair_dict.pop(key)

        # If there are some failed disks awaiting repair
        if len(self.wait_repair_queue) != 0:
            disk_id = self.wait_repair_queue[0][1]
            rack_id = disk_id / (self.nodes_per_rack * self.disks_per_node)
            if self.use_network and self.network.get_avail_cross_rack_repair_bwth() != 0 and \
                self.network.get_avail_intra_rack_repair_bwth(rack_id) != 0 and \
                self.racks[rack_id].get_curr_state() == Rack.STATE_RACK_NORMAL:
                heappop(self.wait_repair_queue)
                self.set_disk_repair(disk_id, curr_time)

        next_event = heappop(self.events_queue)
        next_event_time = next_event[0]
        next_event_type = next_event[1]
        if next_event_time > self.mission_time:
            return (next_event_time, None, None)

        device_idx_set = []
        device_idx_set.append(next_event[2])
        repair_bwth_set = []
        # If use network bandwidth to calculate repair_time
        if self.use_network and next_event_type == Disk.EVENT_DISK_REPAIR:
            repair_bwth_set.append(next_event[3])

        # Gather the events with the same occurring time and event type
        while self.events_queue[0][0] == next_event_time and self.events_queue[0][1] == next_event_type:
            next_event = heappop(self.events_queue)
            device_idx_set.append(next_event[2])
            if self.use_network and next_event_type == Disk.EVENT_DISK_REPAIR:
                repair_bwth_set.append(next_event[3])

        # disk permanent failure
        if next_event_type == Disk.EVENT_DISK_FAIL:
            fail_time = next_event_time
            for device_idx in device_idx_set:
                # avoid the case that this disk is under repair
                if self.disks[device_idx].get_curr_state() != Disk.STATE_CRASHED:
                    if self.delayed_repair_dict.has_key(device_idx):
                        self.delayed_repair_dict.pop(device_idx)
                    # update the state of the disk
                    self.disks[device_idx].fail_disk(fail_time)
                    # generate the repair event
                    self.set_disk_repair(device_idx, fail_time)
            return (fail_time, Disk.EVENT_DISK_FAIL, device_idx_set)

        # node permanent failure
        elif next_event_type == Node.EVENT_NODE_FAIL:
            failed_disks_set = set([])
            fail_time = next_event_time
            for device_idx in device_idx_set:
                # avoid the case that the node is under repair
                if self.nodes[device_idx].get_curr_state() != Node.STATE_NODE_CRASHED:
                    # update the state of node
                    self.nodes[device_idx].fail_node(fail_time)
                    for i in xrange(self.disks_per_node):
                        disk_idx = device_idx * self.disks_per_node + i
                        failed_disks_set.add(disk_idx)
                        # avoid the case that the disk is under repair
                        if self.disks[disk_idx].get_curr_state() != Disk.STATE_CRASHED:
                            if self.delayed_repair_dict.has_key(device_idx):
                                self.delayed_repair_dict.pop(device_idx)
                            # update the state of the disk
                            self.disks[disk_idx].fail_disk(fail_time)
                            # generate the repair event
                            self.set_disk_repair(disk_idx, fail_time)
            return (fail_time, Node.EVENT_NODE_FAIL, failed_disks_set)

        # node transient failure
        elif next_event_type == Node.EVENT_NODE_TRANSIENT_FAIL:
            fail_time = next_event_time
            for device_idx in device_idx_set:
                if self.nodes[device_idx].get_curr_state() == Node.STATE_NODE_NORMAL:
                    # update the state of node
                    self.nodes[device_idx].offline_node()
                    for i in xrange(self.disks_per_node):
                        disk_id = device_idx * self.disks_per_node + i
                        if self.disks[disk_id].get_curr_state() == Disk.STATE_NORMAL:
                            # update the state of disk
                            self.disks[disk_id].offline_disk(fail_time)
                # generate the repair event
                if not self.use_trace:
                    self.set_node_transient_repair(device_idx, fail_time)

            return (fail_time, Node.EVENT_NODE_TRANSIENT_FAIL, None)

        # transient rack failure
        elif next_event_type == Rack.EVENT_RACK_FAIL:
            fail_time = next_event_time
            for device_idx in device_idx_set:
                if self.racks[device_idx].get_curr_state() == Rack.STATE_RACK_NORMAL:
                    # update the state of the rack
                    self.racks[device_idx].fail_rack(fail_time)
                    for i in xrange(self.nodes_per_rack):
                        # update the state of the node
                        node_idx = device_idx * self.nodes_per_rack + i
                        if self.nodes[node_idx].get_curr_state() == Node.STATE_NODE_NORMAL:
                            self.nodes[node_idx].offline_node()
                            for j in xrange(self.disks_per_node):
                                # update the state of the disk
                                disk_idx = node_idx * self.disks_per_node + j
                                if self.disks[disk_idx].get_curr_state() == Disk.STATE_NORMAL:
                                    self.disks[disk_idx].offline_disk(fail_time)
                # generate the repair event
                if not self.use_power_outage:
                    self.set_rack_repair(device_idx, fail_time)

            return (fail_time, Rack.EVENT_RACK_FAIL, None)

        # repair for permanent disk failure
        elif next_event_type == Disk.EVENT_DISK_REPAIR:
            repair_time = next_event_time
            for repair_disk_idx in device_idx_set:
                if self.disks[repair_disk_idx].get_curr_state() == Disk.STATE_CRASHED:
                    # update the state of the disk
                    self.disks[repair_disk_idx].repair_disk(repair_time)
                    # generate next permanent disk failure
                    self.set_disk_fail(repair_disk_idx, repair_time)

                # if the repair event is caused by permanent node failure
                node_idx = repair_disk_idx / self.disks_per_node
                if self.nodes[node_idx].get_curr_state() == Node.STATE_NODE_CRASHED:
                    all_disk_ok = True
                    for i in xrange(self.disks_per_node):
                        disk = self.disks[node_idx * self.disks_per_node + i]
                        if disk.get_curr_state() != disk.STATE_NORMAL:
                            all_disk_ok = False
                            break
                    if all_disk_ok:
                        # update the state of the node
                        self.nodes[node_idx].repair_node()
                        # generate next permanent node failure
                        if not self.use_trace:
                            self.set_node_fail(node_idx, repair_time)
            # update the network status
            if self.use_network:
                idx = 0
                for repair_disk_idx in device_idx_set:
                    repair_bwth = repair_bwth_set[idx]
                    self.network.update_avail_cross_rack_repair_bwth(
                        self.network.get_avail_cross_rack_repair_bwth() + repair_bwth)
                    idx += 1

            # return the set of repaired disks
            return (repair_time, Disk.EVENT_DISK_REPAIR, device_idx_set)

        # repair for node transient failure
        elif next_event_type == Node.EVENT_NODE_TRANSIENT_REPAIR:
            repair_time = next_event_time
            for repair_node_idx in device_idx_set:
                # update the state of the node
                if self.nodes[repair_node_idx].get_curr_state() == Node.STATE_NODE_UNAVAILABLE:
                    self.nodes[repair_node_idx].online_node()
                    # update the state of the disk
                    for i in xrange(self.disks_per_node):
                        disk_id = repair_node_idx * self.disks_per_node + i
                        if self.disks[disk_id].get_curr_state() == Disk.STATE_UNAVAILABLE:
                            self.disks[disk_id].online_disk(repair_time)
                # generate the next transient node failure
                if not self.use_trace:
                    self.set_node_transient_fail(repair_node_idx, repair_time)
            return (repair_time, Node.EVENT_NODE_TRANSIENT_REPAIR, None)

        # repair for rack transient failure
        elif next_event_type == Rack.EVENT_RACK_REPAIR:
            repair_time = next_event_time
            for repair_rack_idx in device_idx_set:
                if self.racks[repair_rack_idx].get_curr_state() == Rack.STATE_RACK_UNAVAILABLE:
                    # update the state of the rack
                    self.racks[repair_rack_idx].repair_rack()
                    for i in xrange(self.nodes_per_rack):
                        node_idx = repair_rack_idx * self.nodes_per_rack + i
                        # update the state of the node
                        if self.nodes[node_idx].get_curr_state() == Node.STATE_NODE_UNAVAILABLE:
                            self.nodes[node_idx].online_node()
                            for j in xrange(self.disks_per_node):
                                disk_idx = node_idx * self.disks_per_node + j
                                # update the state of the disk
                                if self.disks[disk_idx].get_curr_state() == Disk.STATE_UNAVAILABLE:
                                    self.disks[disk_idx].online_disk(repair_time)
                # generate the next transient rack failure
                if not self.use_power_outage:
                    self.set_rack_fail(repair_rack_idx, repair_time)

            return (repair_time, Rack.EVENT_RACK_REPAIR, None)

        else:
            self.logger.error('Wrong type of next_event in get_next_event()!')
            return None


    ##
    # Run an iteration of the simulator
    #
    def run_iteration(self, ite=0):
        self.reset()
        curr_time = 0

        self.logger.info("Regular Simulator: begin an iteration %d, num_failed_disks = %d, "
                         "avail_cross_rack_bwth = %d"
                         % (ite, len(self.state.get_failed_disks()),
                            self.network.get_avail_cross_rack_repair_bwth()))

        while True:
            (event_time, event_type, disk_id_set) = self.get_next_event(curr_time)
            curr_time = event_time
            if curr_time > self.mission_time:
                break
            # update the whole status
            if not self.state.update_state(event_type, disk_id_set):
                self.logger.error('update_state failed!')
            if event_type != None:
                self.logger.debug("Time %s, Event type: %s, Number of failed disks: %s\n" %
                              (event_time, event_type, self.state.get_num_failed_disks()))

            # Check durability when disk_failure/node_failure happens
            if event_type == Disk.EVENT_DISK_FAIL or event_type == Node.EVENT_NODE_FAIL:
                if ite == 1:
                    self.logger.info("Time %s, Event type: %s, Number of failed disks: %s\n" %
                                  (event_time, event_type, self.state.get_num_failed_disks()))
                failed_disks = self.state.get_failed_disks()
                if self.placement.check_data_loss(failed_disks):
                    # the number of failed stripes and the number of lost chunks
                    (num_failed_stripes, num_lost_chunks) = self.placement.get_num_failed_status(failed_disks)
                    # Count in the delayed stripes
                    if len(self.delayed_repair_dict) != 0:
                        for key in self.delayed_repair_dict:
                            num_failed_stripes += len(self.delayed_repair_dict[key])
                            num_lost_chunks += len(self.delayed_repair_dict[key])
                    # Calculate blocked ratio
                    sum_unavail_time = 0
                    for disk_id in xrange(self.num_disks):
                        sum_unavail_time += self.disks[disk_id].get_unavail_time(curr_time) * \
                                            self.placement.get_num_chunks_per_disk(disk_id)
                    blocked_ratio = sum_unavail_time / (self.placement.num_chunks * curr_time)
                    # Calculate the single-chunk repair ratio
                    single_chunk_repair_ratio = 0
                    self.logger.info("num_stripes_repaired_single_chunk = %d, num_stripes_repaired = %d" %
                                     (self.num_stripes_repaired_single_chunk, self.num_stripes_repaired))

                    if self.num_stripes_repaired != 0:
                        single_chunk_repair_ratio = float(self.num_stripes_repaired_single_chunk) / \
                                                    float(self.num_stripes_repaired)

                    return (1, "(%d, %d, %f, %f)" % (num_failed_stripes, num_lost_chunks, blocked_ratio,
                                                     single_chunk_repair_ratio))

        # No data loss
        # Calculate blocked ratio
        sum_unavail_time = 0
        for disk_id in xrange(self.num_disks):
            sum_unavail_time += self.disks[disk_id].get_unavail_time(self.mission_time) * \
                                self.placement.get_num_chunks_per_disk(disk_id)
        blocked_ratio = sum_unavail_time / (self.placement.num_chunks * self.mission_time)
        # Calculate the single-chunk repair ratio
        single_chunk_repair_ratio = 0
        if self.num_stripes_repaired != 0:
            single_chunk_repair_ratio = float(self.num_stripes_repaired_single_chunk) / \
                                        float(self.num_stripes_repaired)

        return (0, "(0, 0, %f, %f)" % (blocked_ratio, single_chunk_repair_ratio))
