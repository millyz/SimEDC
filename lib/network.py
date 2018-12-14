import logging

##
# Print debug messages
#
# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.ERROR)


class Network:

    def __init__(self, num_racks, nodes_per_rack, network_setting):
        self.num_racks = num_racks
        self.nodes_per_rack = nodes_per_rack
        self.num_nodes = self.num_racks * self.nodes_per_rack
        self.max_cross_rack_repair_bwth = network_setting[0]
        self.max_intra_rack_repair_bwth = network_setting[1]
        self.avail_cross_rack_repair_bwth = self.max_cross_rack_repair_bwth
        self.avail_intra_rack_repair_bwth = [self.max_intra_rack_repair_bwth] * num_racks


    def update_avail_cross_rack_repair_bwth(self, updated_value):
        if updated_value <= self.max_cross_rack_repair_bwth and updated_value >= 0:
            self.avail_cross_rack_repair_bwth = updated_value
        else:
            logging.error('Wrong updated_value in update_avail_cross_rack_repair_bwth!')


    def update_avail_intra_rack_repair_bwth(self, rack_id, updated_value):
        if updated_value <= self.max_intra_rack_repair_bwth and updated_value >= 0:
            self.avail_intra_rack_repair_bwth[rack_id] = updated_value
        else:
            logging.error('Wrong updated_value in update_avail_intra_rack_repair_bwth!')


    def get_avail_cross_rack_repair_bwth(self):
        return self.avail_cross_rack_repair_bwth


    def  get_avail_intra_rack_repair_bwth(self, rack_id):
        return self.avail_intra_rack_repair_bwth[rack_id]
