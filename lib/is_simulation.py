import sys
import logging
import random
import numpy.random as nprandom
from heapq import *
from simulation import Simulation
from state import State
from smp_data_structures import Rack, Node, Disk
from placement import Placement

formatter = logging.Formatter('%(asctime)-15s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)

# This class is inherited from Simulation
# Uniformization Balanced Failure Biasing Simulation
class UnifBFBSimulation(Simulation):
    ##
    # __init__() from Simulation
    #

    ##
    # Initialize UnifBFBSimulation
    #
    def init(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)
        # self.logger.setLevel(logging.INFO)
        # self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(console)
        self.logger.propagate = False

        # Failure biasing prob
        self.fb_prob = float(self.is_parms.fb_prob)
        # Arrival rate of homogeneous Poisson process, beta
        # self.poisson_rate = float(self.get_failure_rate() * self.is_parms.beta_ratio)
        self.poisson_rate = float(self.is_parms.beta_ratio)
        # Likelihood ratio
        self.lr = float(1.)

        self.logger.debug("UnifBFBSimulation init() - fb_prob = %.6f, poisson_rate = %.6f",
             self.fb_prob, self.poisson_rate)


    ##
    # Reset the simulator
    #
    def reset(self):
        # Reset clocks and state for each disk
        for disk in self.disks:
            disk.init_clock(0)
            disk.init_state()

        # Reset clocks and state for each node
        for node in self.nodes:
            node.init_clock(0)
            node.init_state()

        # Reset clocks and state for each rack
        for rack in self.racks:
            rack.init_state()

        # Reset system state
        self.state = State(self.num_disks, self.num_nodes)

        # Rest repair queue
        self.repair_queue = []

        # Regenerate new placement
        self.placement = Placement(self.num_racks, self.nodes_per_rack,
                                   self.disks_per_node, self.capacity_per_disk,
                                   self.num_stripes, self.chunk_size,
                                   self.code_type, self.n, self.k,
                                   self.place_type,
                                   self.chunk_rack_config, self.l)
        # Reset LR
        self.lr = float(1.)

        self.total_failure_rate = 0.;
        self.total_failrue_rate_cnt = 0;
        self.total_repair_rate = 0.;
        self.total_repair_rate_cnt = 0;


    ##
    # Get failure rate
    #
    def get_failure_rate(self):
        fail_rate = float(0)

        for disk in self.disks:
            fail_rate += disk.curr_disk_fail_rate()

        for node in self.nodes:
            fail_rate += node.curr_node_fail_rate()

        # self.logger.debug("get_failure_rate(): fail_rate = %.6f", fail_rate)
        # print("get_failure_rate(): fail_rate = %.6f" % fail_rate)
        return fail_rate


    ##
    # Get the probability of node failure
    # To decide whether a failure event is node failure or disk failure
    #
    def get_node_failure_prob(self):
        comp_fail_rate = float(0)
        node_fail_rate = float(0)
        for disk in self.disks:
            comp_fail_rate += disk.curr_disk_fail_rate()
        for node in self.nodes:
            node_fail_rate += node.curr_node_fail_rate()

        return node_fail_rate / (node_fail_rate + comp_fail_rate)


    ##
    # Calculate the repair time for a failed component
    # The repair time = the amount of cross_rack data to download / cross_rack bandwidth
    #
    def get_disk_repair_duration(self, disk_idx):
        if not self.use_network:
            # get the repair time from a pre-defined repair distribution
            return self.disk_repair_dists.draw()
        else:
            # repair time = cross-rack repair traffic / available cross-rack bandwidth
            rack_id = disk_idx / (self.nodes_per_rack * self.disks_per_node)
            cross_rack_download = 0
            stripes_to_repair = self.placement.get_stripes_to_repair(disk_idx)
            # self.num_stripes_repaired += len(stripes_to_repair)
            # stripes_to_delay = []

            # for each stripe to repair
            for stripe_id in stripes_to_repair:
                num_failed_chunk = 0
                num_alive_chunk_same_rack = 0
                idx = 0
                fail_idx = 0
                alive_chunk_same_rack = []

                # check the status of each chunk in the stripe
                for disk_id in self.placement.get_stripe_location(stripe_id):

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

                # # this is a single-chunk repair
                # if num_failed_chunk == 1:
                #     self.num_stripes_repaired_single_chunk += 1

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

            repair_duration = cross_rack_download * self.chunk_size / \
                              float(self.network.get_avail_cross_rack_repair_bwth()) # seconds
            # print "repair_time = %.1f" % (repair_duration / 3600.)

            self.total_repair_rate += 3600. / repair_duration
            self.total_repair_rate_cnt += 1

            return repair_duration / 3600.  # hours


    def get_earliest_repair_time(self, curr_time):
        earliest_repair_time = curr_time
        if len(self.repair_queue) > 0:
            for repair_event in self.repair_queue:
                repair_event_time = repair_event[0]
                if repair_event_time > earliest_repair_time:
                    earliest_repair_time = repair_event_time

        return earliest_repair_time


    ##
    # Set next repair time for disk indexed with disk_index
    #
    def set_disk_repair(self, disk_idx, curr_time):
        heappush(self.repair_queue, (self.get_disk_repair_duration(disk_idx) + self.get_earliest_repair_time(curr_time),
                                     Disk.EVENT_DISK_REPAIR, disk_idx))

    ##
    # Set new node repair time for node node_idx
    #
    def set_node_repair(self, node_idx, curr_time):
        node_repair_duration = 0
        # Get the repair duration of each disk on this node
        for i in xrange(self.disks_per_node):
            disk_idx = self.disks_per_node * node_idx + i
            node_repair_duration += self.get_disk_repair_duration(disk_idx)

        heappush(self.repair_queue, (node_repair_duration + self.get_earliest_repair_time(curr_time),
                                     Node.EVENT_NODE_REPAIR, node_idx))


    ##
    # Get the next event in UnifBFBSimulation
    #
    def get_next_event(self, curr_time):
        # Update clock for each disk
        for disk in self.disks:
            disk.update_clock(curr_time)

        # Update clock for each node
        for node in self.nodes:
            node.update_clock(curr_time)

        # If not in a failed state, then draw for next failure
        if self.state.get_sys_state() == self.state.CURR_STATE_OK:
            failure_queue = []

            for each_disk in range(self.num_disks):
                failure_queue.append((self.disks[each_disk].disk_fail_distr.draw_inverse_transform(
                    self.disks[each_disk].read_clock()) + curr_time, Disk.EVENT_DISK_FAIL, each_disk))

            for each_node in range(self.num_nodes):
                failure_queue.append((self.nodes[each_node].node_fail_distr.draw_inverse_transform(
                    self.nodes[each_node].read_clock()) + curr_time, Node.EVENT_NODE_FAIL, each_node))

            heapify(failure_queue)
            (next_event_time, next_event_type, next_event_subsystem) = heappop(failure_queue)

            if next_event_type == Disk.EVENT_DISK_FAIL:
                self.disks[next_event_subsystem].fail_disk(next_event_time)
                self.set_disk_repair(next_event_subsystem, next_event_time)
            elif next_event_type == Node.EVENT_NODE_FAIL:
                self.nodes[next_event_subsystem].fail_node(next_event_time)
                for each_disk_on_this_node in range(next_event_subsystem * self.disks_per_node,
                               (next_event_subsystem + 1) * self.disks_per_node):
                    self.disks[each_disk_on_this_node].fail_disk(next_event_time)
                self.set_node_repair(next_event_subsystem, next_event_time)
            else:
                self.logger.error("UnifBFBSimulation - get_next_event(): wrong next_event_type!")

            return (next_event_time, next_event_type, next_event_subsystem)

        elif self.state.get_sys_state() == self.state.CURR_STATE_DEGRADED:
            if not self.repair_queue:
                self.logger.error("UnifBFBSimulation - get_next_event(): repair_queue is empty!")
                sys.exit(2)

            (repair_time, repair_event, subsystem_idx) = self.repair_queue[0]
            next_event_time = nprandom.exponential(self.poisson_rate) + curr_time

            if repair_time <= next_event_time:
                heappop(self.repair_queue)
                if repair_event == Disk.EVENT_DISK_REPAIR:
                    self.disks[subsystem_idx].repair_disk(repair_time)
                    return (repair_time, Disk.EVENT_DISK_REPAIR, subsystem_idx)
                elif repair_event == Node.EVENT_NODE_REPAIR:
                    self.nodes[subsystem_idx].repair_node()
                    for i in range(self.disks_per_node):
                        disk_idx = subsystem_idx * self.disks_per_node + i
                        self.disks[disk_idx].repair_disk(repair_time)
                    return (repair_time, Node.EVENT_NODE_REPAIR, subsystem_idx)
                else:
                    self.logger.error("UnifBFBSimulation - get_next_event(): wrong repair_event!")

            for disk in self.disks:
                disk.update_clock(next_event_time)
            for node in self.nodes:
                node.update_clock(next_event_time)

            self.total_failure_rate += self.get_failure_rate()
            self.total_failrue_rate_cnt += 1

            draw = nprandom.uniform()
            # Determine whether it is a "real" event or "pseudo" event
            if draw > self.fb_prob:
                # It is a pseudo event
                old_lr = self.lr
                self.lr *=  (1. - self.get_failure_rate() / self.poisson_rate) / (1. - self.fb_prob)
                self.logger.debug("get_next_event(): pseudo event - old_lr = %.10f, update, lr = %.10f", old_lr, self.lr)
                # Return nothing because we are staying in the current state
                return (next_event_time, None, None)

            else:
                # Randomly fail a disk or node
                # prob_node_failure = self.get_node_failure_prob()
                if nprandom.uniform() > self.get_node_failure_prob():
                    # disk failure
                    avail_disks = self.state.get_avail_disks()
                    fail_disk_idx = avail_disks[random.randint(0, len(avail_disks) - 1)]

                    old_lr = self.lr
                    # self.lr *= (self.disks[fail_disk_idx].curr_disk_fail_rate() / self.poisson_rate) \
                    #            / (self.fb_prob * (1 - prob_node_failure) / len(avail_disks))
                    # The above equation equals to the following
                    self.lr *= (self.get_failure_rate() / self.poisson_rate) / self.fb_prob
                    self.logger.debug("get_next_event(): disk failure event, lr = %.10f, update, lr = %.10f",
                                      old_lr, self.lr)

                    self.disks[fail_disk_idx].fail_disk(next_event_time)
                    self.set_disk_repair(fail_disk_idx, next_event_time)

                    return (next_event_time, Disk.EVENT_DISK_FAIL, fail_disk_idx)

                else:
                    avail_nodes = self.state.get_avail_nodes()
                    fail_node_idx = avail_nodes[random.randint(0, len(avail_nodes)-1)]

                    old_lr = self.lr
                    # self.lr *= (self.nodes[fail_node_idx].curr_node_fail_rate() / self.poisson_rate) \
                    #            / (self.fb_prob * prob_node_failure / len(avail_nodes))
                    # The above equation equals to the following
                    self.lr *= (self.get_failure_rate() / self.poisson_rate) / self.fb_prob
                    self.logger.debug("get_next_event(): node failure event - old_lr = %.10f, update, lr = %.10f",
                                      old_lr, self.lr)

                    # Update internal node state
                    self.nodes[fail_node_idx].fail_node(next_event_time)
                    for each_disk_on_failed_node in range(fail_node_idx * self.disks_per_node,
                                                          (fail_node_idx + 1) * self.disks_per_node):
                        self.disks[each_disk_on_failed_node].fail_disk(next_event_time)

                    # Schedule repair for the failed node
                    self.set_node_repair(fail_node_idx, next_event_time)

                    return (next_event_time, Node.EVENT_NODE_FAIL, fail_node_idx)


    ##
    # Run an iteration in UnifBFBSimulation
    #
    def run_iteration(self, ite=0):
        self.reset()
        curr_time = 0
        self.logger.info("UnifBFBSimulator: begin an iteration %d, num_failed_disks = %d, "
                         "avail_cross_rack_bwth = %d"
                         % (ite, len(self.state.get_failed_disks()),
                            self.network.get_avail_cross_rack_repair_bwth()))

        while True:
            (event_time, event_type, subsystem_idx) = self.get_next_event(curr_time)
            curr_time = event_time

            if event_time > self.mission_time:
                break

            if event_type != None:
                self.logger.debug("Time: %.3f, event = %s, subsystem = %d, "
                                  "number_failed_disks = %d, number_failed_nodes = %d" %
                              (event_time, event_type, subsystem_idx,
                               self.state.get_num_failed_disks(), self.state.get_num_failed_nodes()))

                if not self.state.update_state_unifbfb(event_type, subsystem_idx):
                    self.logger.error('Update_state_unifbfb failed!')

            # Check durability when disk failure or node failure happens
            if event_type == Disk.EVENT_DISK_FAIL or event_type == Node.EVENT_NODE_FAIL:
                failed_disks = self.state.get_failed_disks()
                if self.placement.check_data_loss(failed_disks):
                    self.logger.debug("===== END of one iteration, self.lr = %.10f", min(self.lr, 1))
                    (num_failed_stripes, num_lost_chunks) = self.placement.get_num_failed_status(failed_disks)
                    self.logger.info("avg_failure_rate = %.6f" % (self.total_failure_rate / self.total_failrue_rate_cnt))
                    self.logger.info("avg_repair_rate = %.6f" % (self.total_repair_rate / self.total_repair_rate_cnt))
                    return (min(self.lr, 1), "(%d, %d, 0, 0)" % (num_failed_stripes, num_lost_chunks))

        # No data loss
        self.logger.debug("END of one iteration, self.lr = 0 because no data loss")
        return (0, "(0, 0, 0, 0)")
