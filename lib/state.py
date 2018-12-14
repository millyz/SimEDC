import sys
import logging
from bm_ops import *
from smp_data_structures import Disk, Node, Rack

##
# This module is used to store and process state information
# for the data center
#

formatter = logging.Formatter('%(asctime)-15s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)

##
# This encapsulated system state of the disks
#
class State:
    CURR_STATE_OK = "system is operational"
    CURR_STATE_DEGRADED = "system has at least one failure"

    ##
    #  Given a list of disk IDs construct the data
    #  structures needed to capture system state.
    #
    def __init__(self, num_disks=0, num_nodes=0):
        self.num_disks = num_disks
        self.num_nodes = num_nodes
        self.disks_per_node = 0
        if self.num_disks != 0 and self.num_nodes != 0:
            self.disks_per_node = self.num_disks / self.num_nodes

        # Bit-map of available disks
        # If self.num_disks = 5, then avail_disk = 31
        # 31's binary form is 11111
        self.avail_disk = (1 << self.num_disks) - 1
        # Bit-map of failed disks
        self.failed_disks = 0
        # Keep track of number of disk failures
        self.num_failed_disks = 0
        # Keep track of the unavailable disks
        self.unavailable_disk = 0
        self.num_unavailable_disk = 0

        self.avail_nodes = (1 << self.num_nodes) - 1
        # Bit-map of failed nodes
        self.failed_nodes = 0
        self.num_failed_nodes = 0

        # System state
        self.sys_state = self.CURR_STATE_OK

        # Get and set logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)
        # self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(console)
        self.logger.propagate = False


    ##
    # Function to copy another State Obj
    #
    def copy(self, state):
        self.num_disks = state.num_disks
        # self.disks = state.disks[:]
        self.num_failed_disks = state.num_failed_disks
        self.failed_disks = state.failed_disks
        self.num_unavailable_disk = state.num_unavailable_disk
        self.unavailable_disk = state.unavailable_disk
        self.avail_disk = state.avail_disk
        self.sys_state = state.sys_state

    ##
    # Update the state of the whole system
    #
    def update_sys_state(self):
        if self.num_failed_disks == 0 and self.num_unavailable_disk == 0:
            self.sys_state = self.CURR_STATE_OK
        else:
            self.sys_state = self.CURR_STATE_DEGRADED


    # This is for the new added events
    def update_state(self, event_type, disk_id_set):
        if event_type == Node.EVENT_NODE_FAIL or event_type == Disk.EVENT_DISK_FAIL:
            for disk_id in disk_id_set:
                self.fail_disk(disk_id)

        elif event_type == Disk.EVENT_DISK_REPAIR:
            for disk_id in disk_id_set:
                self.repair_disk(disk_id)

        elif event_type == Node.EVENT_NODE_TRANSIENT_FAIL or event_type == Node.EVENT_NODE_TRANSIENT_REPAIR:
            self.logger.debug('Event_type is node_transient_fail/repair in update_state()')

        elif event_type is Rack.EVENT_RACK_FAIL or Rack.EVENT_RACK_REPAIR:
            self.logger.debug('Event_type is rack_failure/rack_repair in update_state()')

        else:
            self.logger.error('Wrong event_type in update_state()!')
            return False

        self.update_sys_state()

        return True


    # Update state in UnifBFBSimulation
    def update_state_unifbfb(self, event_type, subsystem_idx):
        if event_type == None or subsystem_idx == None:
            self.logger.error("State - update_state_unifbfb(): event_type and subsystem_idx should NOT be None!")
            return False

        if self.disks_per_node == 0:
            self.logger.error("State - update_state_unifbfb(): self.disks_per_node should not be 0!")
            return False

        if event_type == Disk.EVENT_DISK_FAIL:
            self.fail_disk(subsystem_idx)
            node_idx = subsystem_idx / self.disks_per_node
            node_fail = True
            for i in range(self.disks_per_node):
                disk_idx = node_idx * self.disks_per_node + i
                if disk_idx not in set(self.get_failed_disks()):
                    node_fail = False
                    break
            if node_fail:
                self.fail_node(node_idx)
        elif event_type == Disk.EVENT_DISK_REPAIR:
            self.repair_disk(subsystem_idx)
            node_idx = subsystem_idx / self.disks_per_node
            node_repair = True
            for i in range(self.disks_per_node):
                disk_idx = node_idx * self.disks_per_node + i
                if disk_idx in set(self.get_failed_disks()):
                    node_repair = False
                    break
            if node_repair and bm_in(node_idx, self.failed_nodes):
                self.repair_node(node_idx)
        elif event_type == Node.EVENT_NODE_FAIL:
            self.fail_node(subsystem_idx)
            for i in range(self.disks_per_node):
                disk_idx = subsystem_idx * self.disks_per_node + i
                self.fail_disk(disk_idx)
        elif event_type == Node.EVENT_NODE_REPAIR:
            self.repair_node(subsystem_idx)
            for i in range(self.disks_per_node):
                disk_idx = subsystem_idx * self.disks_per_node + i
                self.repair_disk(disk_idx)
        else:
            self.logger.error("State - update_state_unif_bfb(): wrong event_type!")
            return False

        self.update_sys_state()

        return True


    ##
    # Set the disk as unavailable because of rack failure
    #
    def set_disk_offline(self, disk_id):
        self.avail_disk = bm_rm(self.avail_disk, disk_id)
        self.unavailable_disk = bm_insert(self.unavailable_disk, disk_id)

        self.num_unavailable_disk += 1
        self.logger.debug("Disk %s is offline" % disk_id)


    ##
    # Set the disk as available because of rack repair
    #
    def set_disk_online(self, disk_id):
        if bm_in(disk_id, self.unavailable_disk):
            self.unavailable_disk = bm_rm(self.unavailable_disk, disk_id)
            self.avail_disk = bm_insert(self.avail_disk, disk_id)

            self.num_unavailable_disk -= 1
            self.logger.debug("Disk %s is online" % disk_id)
        else:
            self.logger.debug("Disk %s is not in self.unavail_disk" % disk_id)


    ##
    # Set a disk as failed
    #
    def fail_disk(self, disk_id):
        if disk_id >= self.num_disks or disk_id < 0:
            self.logger.error("State - fail_disk(): wrong disk_id!")
            sys.exit(2)

        # Insert into bitmap of disk failures
        self.failed_disks = bm_insert(self.failed_disks, disk_id)
        self.avail_disk = bm_rm(self.avail_disk, disk_id)
        # Increment disk failure count
        self.num_failed_disks += 1
        self.logger.debug("Disk %s has failed" % disk_id)


    ##
    # "Repair" a disk by removing it from the failed disk list
    #
    def repair_disk(self, disk_id):
        if disk_id >= self.num_disks or disk_id < 0:
            self.logger.error("State - repair_disk(): wrong disk_id!")
            sys.exit(2)

        # Remove entry from the failed disk bitmap
        self.failed_disks = bm_rm(self.failed_disks, disk_id)
        self.avail_disk = bm_insert(self.avail_disk, disk_id)

        # Decrement disk failure count
        self.num_failed_disks -= 1

        self.logger.debug("Disk %s has been repaired" % disk_id)


    ##
    # Set a node as failed
    #
    def fail_node(self, node_id):
        if node_id >= self.num_nodes or node_id < 0:
            self.logger.error("State - fail_node(): wrong node_id!")
            sys.exit(2)

        # Insert into bitmap of node failures
        self.failed_nodes = bm_insert(self.failed_nodes, node_id)
        self.avail_nodes = bm_rm(self.avail_nodes, node_id)
        # Update the number of failed nodes
        self.num_failed_nodes = len(bm_to_list(self.failed_nodes))

        self.logger.debug("Node %s has failed" % node_id)


    ##
    # "Repair" a node by removing it from the failed node list
    #
    def repair_node(self, node_id):
        if node_id >= self.num_nodes or node_id < 0:
            self.logger.error("State - repair_node(): wrong node_id!")
            sys.exit(2)

        # Remove entry from the failed node bitmap
        self.failed_nodes = bm_rm(self.failed_nodes, node_id)
        self.avail_nodes = bm_insert(self.avail_nodes, node_id)
        # Update the number of failed nodes
        self.num_failed_nodes = len(bm_to_list(self.failed_nodes))

        self.logger.debug("Node %s has been repaired" % node_id)


    ##
    # Get number of disk failures
    #
    def get_num_failed_disks(self):
        return self.num_failed_disks


    ##
    # Return a list of disk IDs of failed disks
    #
    def get_failed_disks(self):
        return bm_to_list(self.failed_disks)


    ##
    # Return the current system state
    #
    def get_sys_state(self):
        self.logger.debug("get_sys_state(): self.sys_state = %s", self.sys_state)
        return self.sys_state

    ##
    # Return a list of disk IDs of available disks
    #
    def get_avail_disks(self):
        return bm_to_list(self.avail_disk)

    ##
    # Return a list of disk IDs of available disks
    #
    def get_avail_nodes(self):
        return bm_to_list(self.avail_nodes)

    ##
    # Get number of failed nodes
    #
    def get_num_failed_nodes(self):
        return self.num_failed_nodes

    ##
    # Return a list of disk IDs of failed disks
    #
    def get_failed_nodes(self):
        return bm_to_list(self.failed_disks)
