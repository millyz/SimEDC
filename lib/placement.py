import random
import logging

##
# Print debug messages
formatter = logging.Formatter('%(asctime)-15s - %(name)s - %(levelname)s - %(message)s')
console = logging.StreamHandler()
console.setFormatter(formatter)


class Placement:
    CODE_TYPE_RS = "Reed-Solomon Codes"
    CODE_TYPE_LRC = "Locally Repairable Codes"
    CODE_TYPE_DRC = "Double Regenerating Codes"

    # Erasure codes' placement
    PLACE_TYPE_FLAT = "FLAT" #"Each chunk of a stripe resides in different rack"
    PLACE_TYPE_HIERARCHICAL = "HIERARCHICAL" #"More than one chunk of a stripe resides in a rack"

    def __init__(self, num_racks, nodes_per_rack, disks_per_node, capacity_per_disk,
                 num_stripes, chunk_size, code_type, code_n, code_k, place_type,
                 chunk_rack_config=None, code_l=0):
        self.num_racks = num_racks
        self.nodes_per_rack = nodes_per_rack
        self.disks_per_node = disks_per_node
        self.num_disks = self.num_racks * self.nodes_per_rack * self.disks_per_node
        self.capacity_per_disk = capacity_per_disk
        self.num_stripes = num_stripes
        self.chunk_size = chunk_size
        self.code_type = code_type
        self.n = code_n # number of chunks in a stripe
        self.k = code_k # number of data chunks in a stripe
        self.m = self.n - self.k
        self.num_chunks = self.n * self.num_stripes
        self.num_data_chunks = self.k * self.num_stripes
        self.place_type = place_type
        self.chunk_rack_config = chunk_rack_config

        # for LRC code
        self.l = code_l
        self.lrc_data_group = [[0,1,2,3,4,5],[8,9,10,11,12,13]]
        self.lrc_local_parity = [6,14]
        self.lrc_global_parity = [7,15]

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.ERROR)
        # self.logger.setLevel(logging.INFO)
        self.logger.addHandler(console)
        self.logger.propagate = False

        # check weather the configuration is valid
        if self.chunk_rack_config != None:
            sum = 0
            for each in self.chunk_rack_config:
                sum += each
            if sum != self.n:
                logging.error('The chunk_rack_config is NOT valid!')

        # stripes_location, keeps record of the disks that each stripe resides in
        # E.g., stripes_location[0], is the list of disk index that stripe0 locates on
        self.stripes_location = []
        self.stripes_per_disk = [[] for i in xrange(self.num_disks)]
        if self.generate_placement():
            self.logger.debug('Generate placement successfully!')
        else:
            self.logger.error('Fail to generate placement!')

        self.num_chunks_per_disk = self.generate_num_chunks_per_disk()


    # Generate placement for different code_type
    def generate_placement(self):
        if self.code_type == Placement.CODE_TYPE_RS or self.code_type == Placement.CODE_TYPE_LRC:
            if self.k < 1 or self.n <= self.k:
                self.logger.error('Invalid value of code_n and code_k for erasure coding!')
                return False
            if self.code_type == Placement.CODE_TYPE_LRC and self.l == 0:
                self.logger.error('l should not be 0 for LRC!')
                return False
            return self.generate_placement_ec()

        elif self.code_type == Placement.CODE_TYPE_DRC:
            if self.n == 9 and (self.k == 6 or self.k == 5):
                self.chunk_rack_config = [3, 3, 3]
                return self.generate_placement_ec()
            else:
                return False

        else:
            self.logger.error('Wrong code Type in generate_placement()!')
            return False


    ##
    # For each chunk, generate its location
    # @param: chunk_rack_config, the list of number of chunks in each rack
    # E.g., chunk_rack_config = [1, 2]
    # It means that the 1st chunk reside on a rack
    # and the 2nd and 3rd chunk are placed on another rack
    def generate_placement_ec(self):
        if self.place_type == Placement.PLACE_TYPE_FLAT:
            # Put each chunk of a stripe in different rack by default
            if self.chunk_rack_config == None:
                disks_per_rack = self.disks_per_node * self.nodes_per_rack
                if self.num_racks < self.n or disks_per_rack < 1:
                    return False  # Fail to generate placement
                for stripe_id in xrange(self.num_stripes):
                    racks_list = self.get_diff_racks(self.n)
                    disk_list = []
                    for rack_id in racks_list:
                        disk_list.append(self.get_disk_randomly(rack_id))

                    self.stripes_location.append(disk_list)
            else:
                return False

        elif self.place_type == Placement.PLACE_TYPE_HIERARCHICAL:
            if self.chunk_rack_config == None:
                self.logger.error('Please specify chunk_rack_config for PLACE_TYPE_HIERARCHICAL')
                return False
            else:
                if self.num_racks < len(self.chunk_rack_config) or self.nodes_per_rack < max(self.chunk_rack_config):
                    self.logger.error('The current setting is not suitable for PLACE_TYPE_HIERARCHICAL')
                    return False

                for stripe_id in xrange(self.num_stripes):
                    racks_list = self.get_diff_racks(len(self.chunk_rack_config))
                    disk_list = []
                    for i in xrange(len(racks_list)):
                        disk_list += self.get_diff_disks(racks_list[i], self.chunk_rack_config[i])
                    self.stripes_location.append(disk_list)

        else:
            return False

        return True


    # Randomly choose a disk from a rack with rack_id
    def get_disk_randomly(self, rack_id):
        min_disk = rack_id * self.nodes_per_rack * self.disks_per_node
        max_disk = min_disk + self.nodes_per_rack * self.disks_per_node - 1
        if min_disk == max_disk:
            return min_disk
        else:
            return random.randint(min_disk, max_disk)


    # Randomly choose different disks from a rack
    # Every disk resides on a different node
    def get_diff_disks(self, rack_id, num_diff_disks):
        nodes_list = self.get_diff_nodes(rack_id, num_diff_disks)
        if self.disks_per_node == 1:
            return nodes_list
        else:
            disks_list = []
            for each in nodes_list:
                disks_list.append(each*self.disks_per_node + random.randint(0, self.disks_per_node-1))
            return disks_list


    ##
    # Randomly choose num_diff_nodes from rack with rack_id
    #
    def get_diff_nodes(self, rack_id, num_diff_nodes):
        if self.nodes_per_rack < num_diff_nodes:
            self.logger.error('Wrong num_diff_nodes in get_diff_nodes()')
            return None

        nodes_list = random.sample(range(self.nodes_per_rack), num_diff_nodes)
        for i in xrange(len(nodes_list)):
            nodes_list[i] += rack_id * self.nodes_per_rack

        return nodes_list


    ##
    # Randomly choose num_diff_racks different racks from all of the racks in the system
    #
    def get_diff_racks(self, num_diff_racks):
        if self.num_racks < num_diff_racks:
            self.logger.error('Wrong num_diff_racks in get_diff_racks()!')

        return random.sample(range(self.num_racks), num_diff_racks)


    ##
    # Generate num_chunks_per_disk
    # Count the number of chunks on each disk
    def generate_num_data_chunks_per_disk(self):
        num_data_chunks_per_disk = [0] * (self.num_racks * self.nodes_per_rack * self.disks_per_node)
        for stripe_location in self.stripes_location:
            chunk_id = 0
            for disk_id in stripe_location:
                if chunk_id < self.k:
                    num_data_chunks_per_disk[disk_id] += 1
                else:
                    break
                chunk_id += 1

        return num_data_chunks_per_disk


    ##
    # Generate num_chunks_per_disk
    # Count the number of chunks on each disk
    #
    def generate_num_chunks_per_disk(self):
        num_chunks_per_disk = [0] * (self.num_racks * self.nodes_per_rack * self.disks_per_node)
        for stripe_id in xrange(self.num_stripes):
            for disk_id in self.stripes_location[stripe_id]:
                num_chunks_per_disk[disk_id] += 1
                self.stripes_per_disk[disk_id].append(stripe_id)

        return num_chunks_per_disk


    ##
    # get num_data_chunks in one disk
    #
    def get_num_chunks_per_disk(self, disk_id):
        return self.num_chunks_per_disk[disk_id]


    ##
    # Check whether there is data loss
    #
    def check_data_loss(self, failed_disks_list):
        failed_disks_set = set(failed_disks_list)

        # Get all the stripes reside on the failed disks
        stripe_id_set = []
        for failed_disk in failed_disks_list:
            stripe_id_set += self.get_stripes_to_repair(failed_disk)
        stripe_id_set = set(stripe_id_set)

        if self.code_type == Placement.CODE_TYPE_LRC:
            for stripe_id in stripe_id_set:
                stripe_failed_disks_num = [0] * self.l # self.l groups in total
                global_failed_disks_num = 0
                idx = 0
                for stripe_disk_id in self.stripes_location[stripe_id]:
                    if stripe_disk_id in failed_disks_set:
                        if idx in self.lrc_global_parity:
                            # global parity
                            global_failed_disks_num += 1
                        elif idx not in self.lrc_local_parity:
                            for gid in xrange(self.l):
                                if idx in self.lrc_data_group[gid]:
                                    # data chunk of group gid
                                    stripe_failed_disks_num[gid] += 1
                                    break
                    else:
                        for gid in xrange(self.l):
                            if idx == self.lrc_local_parity[gid] and stripe_failed_disks_num[gid] > 0:
                                stripe_failed_disks_num[gid] -= 1
                                break
                    idx += 1
                sum = global_failed_disks_num
                for each_num in stripe_failed_disks_num:
                    sum += each_num
                if sum > (self.n-self.k-self.l):
                    return True

        else:
            # For each stripe, check the failed disks respectively
            # for stripe_id in xrange(self.num_stripes):
            for stripe_id in stripe_id_set:
                stripe_failed_disks_num = 0
                # Get the number of failed disks for this stripe
                for stripe_disks_id in self.stripes_location[stripe_id]:
                    if stripe_disks_id in failed_disks_set:
                        stripe_failed_disks_num += 1
                if (stripe_failed_disks_num > self.m):
                    return True

        return False



    ##
    # Get the number of failed stripes and data chunks
    # Here we consider the first k chunks are data chunks
    #
    def get_num_failed_status(self, failed_disks_list):
        if len(failed_disks_list) == 0:
            return (0, 0)

        # Get all the stripes reside on the failed disks
        stripe_id_set = []
        for failed_disk in failed_disks_list:
            stripe_id_set += self.get_stripes_to_repair(failed_disk)
        stripe_id_set = set(stripe_id_set)

        failed_disks_set = set(failed_disks_list)
        num_failed_stripes = 0
        num_lost_chunks = 0

        if self.code_type == Placement.CODE_TYPE_LRC:
            for stripe_id in stripe_id_set:
                cur_stripe_lost_chunks_num = 0
                stripe_failed_disks_num = [0] * self.l
                global_failed_disks_num = 0
                idx = 0
                for stripe_disks_id in self.stripes_location[stripe_id]:
                    if stripe_disks_id in failed_disks_set:
                        cur_stripe_lost_chunks_num += 1
                        if idx in self.lrc_global_parity:
                            # global parity
                            global_failed_disks_num += 1
                        elif idx not in self.lrc_local_parity:
                            for gid in xrange(self.l):
                                if idx in self.lrc_data_group[gid]:
                                    # data chunk of group gid
                                    stripe_failed_disks_num[gid] += 1
                                    break
                    else:
                        # local parity
                        for gid in xrange(self.l):
                            if idx == self.lrc_local_parity[gid] and stripe_failed_disks_num[gid] > 0:
                                stripe_failed_disks_num[gid] -= 1
                                break
                    idx += 1
                sum = global_failed_disks_num
                for each_num in stripe_failed_disks_num:
                    sum += each_num
                if sum > (self.n-self.k-self.l):
                    num_failed_stripes += 1
                    num_lost_chunks += cur_stripe_lost_chunks_num
        else:
            # For each stripe, check the failed disks respectively
            # for stripe_id in xrange(self.num_stripes):
            for stripe_id in stripe_id_set:
                cur_stripe_failed_disks_num = 0 # the number of failed disks to check whether can reconstruct or not
                cur_stripe_lost_chunks_num = 0 # the lost data chunks
                # Get the number of failed disks for this stripe
                for stripe_disks_id in self.stripes_location[stripe_id]:
                    if stripe_disks_id in failed_disks_set:
                        cur_stripe_failed_disks_num += 1
                        cur_stripe_lost_chunks_num += 1

                if (cur_stripe_failed_disks_num > self.m):
                    num_failed_stripes += 1
                    num_lost_chunks += cur_stripe_lost_chunks_num

        return (num_failed_stripes, num_lost_chunks)


    ##
    # Get the location of a stripe
    #
    def get_stripe_location(self, stripe_id):
        if stripe_id < 0 or stripe_id >= self.num_stripes:
            self.logger.error('Invalid stripe_id in get_stripe_location!')
        return self.stripes_location[stripe_id]


    ##
    # Get the list of stripe_id of the chunks stored on failed_disk_id
    #
    def get_stripes_to_repair(self, failed_disk_id):
        if failed_disk_id < 0 or failed_disk_id >= self.num_racks * self.nodes_per_rack * self.disks_per_node:
            self.logger.error('Wrong failed_disk_id in get_stripes_to_repair!')

        return self.stripes_per_disk[failed_disk_id]


    ##
    # Get the number of stripes to repair given the failed disk id
    #
    def get_num_stripes_to_repair(self, failed_disk_id):
        if failed_disk_id < 0 or failed_disk_id >= self.num_racks * self.nodes_per_rack * self.disks_per_node:
            self.logger.error('Wrong failed_disk_id in get_num_stripes_to_repair!')

        return len(self.stripes_per_disk[failed_disk_id])
