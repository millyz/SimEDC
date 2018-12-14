##
# This module conatins classes and functions used to
# support the simulation of a semi-Markov process.
#
# The underlying failure/repair distributions are assumed
# to be Weibull, which is Exponential when shape=1.
#
# Kevin Greenan (kmgreen@cs.ucsc.edu)
#
#

import mpmath
from mpmath import mpf
from mpmath import ln
from mpmath import findroot
import random


##
# Set precision used by the MP math lib
#
mpmath.mp.prec += 100
mpmath.mp.dps = 100

##
# Contains parameters, distribution functions and hazard rate function
# for a 3-parameter Weibull distribution based on shape, scale and location.
#
class Weibull:
    ##
    # Construct a Weibull object by specifying shape, scale and location.
    #
    # Note: when shape == 1, this is an Exponential distribution
    #
    def __init__(self, shape=1, scale=1, location=0):
        self.shape = float(shape)
        self.scale = float(scale)
        self.location = float(location)


    ##
    # Get the probability density of Weibull(shape, scale, location) at x
    #
    # @param x: random variable, most likely a time
    # @return density of Weibull(shape, scale, location) at x
    #
    def pdf_eval(self, x):
        if x < 0:
            return 0
        elif x < self.location:
            return 0
        else:
            x = float(x)
            a = self.shape/self.scale
            b = (x-self.location)/self.scale
            b = mpmath.power(b, self.shape-1)
            c = mpmath.exp(-mpmath.power(((x-self.location)/self.scale), self.shape))

            return a * b *c


    ##
    # Return the probability P(X <= x) or the probability that a
    # random variable is less than or equal to the parameter x.
    #
    # The returned value represents the probability that there is
    # a 'failure' at or before x, which is most likely a time.
    #
    # @param x: variable, most likely a time
    # @return: probability of failure before or at x
    #
    def cdf_eval(self, x):
        x = float(x)

        if x < self.location:
            return 0

        return float(1) - mpmath.exp(-mpmath.power(((x-self.location)/self.scale), self.shape))


    ##
    # Return the hazard rate at x.  The hazard rate is interpreted
    # as the instantaneous failure rate at x.
    #
    # Note: If shape == 1, then this value will be constant for all x
    #
    # @param x: variable, most likely a time
    # @return instantaneous failure rate at x
    #
    def hazard_rate(self, x):
        if x < self.location:
            return 0
        elif self.shape == 1:
            return float(1) / self.scale
        else:
            return float(abs(self.pdf_eval(x) / (float(1) - self.cdf_eval(x))))


    ##
    # When the shape parameter is not 1, then the hazard rate will
    # change with time.  When simulating a semi-Markov process using
    # uniformization (which is a method we use), the maximum failure
    # rate for every distribution over all possible times is required.
    # This function evaluates the hazard rate at discrete points and
    # returns the maximum hazard rate for a given mission time (i.e.
    # max simulation time).
    #
    # @param mission_time: expected maximum simulation time
    # @return maximum possible hazard rate for [0, mission_time]
    #
    #
    def get_max_hazard_rate(self, mission_time):
        max = float(0)

        if self.shape == 1:
            return float(1) / self.scale
        print 0.1 * mission_time
        for i in range(1, int(mission_time), int(float(0.1) * mission_time)):
            curr_h_rate = self.hazard_rate(i)
            if curr_h_rate > max:
                max = curr_h_rate
            elif curr_h_rate == float('nan'):
                break

        return max


    ##
    # Get min hazard rate
    #
    def get_min_hazard_rate(self, mission_time):
        min = float(1)

        if self.shape == 1:
            return float(1) / self.scale

        for i in range(0, mission_time, int(0.1 * mission_time)):
            curr_h_rate = self.hazard_rate(i)
            if curr_h_rate < min:
                min = curr_h_rate
            elif curr_h_rate == float('nan'):
                break

        return min


    ##
    # Draw a random value from this distribution
    #
    def draw(self):
        return random.weibullvariate(self.scale, self.shape) + self.location


    ##
    # Draw from a lower truncated Weibull distribution
    # (Reject all samples less than 'lower')
    #
    def draw_truncated(self, lower):
        val = self.draw()
        while val <= lower:
            print "%.2f %.2f" % (lower, val)
            val = self.draw()

        return val


    ##
    # Draw using the inverse transform method.
    # This method draws from the waiting time
    # based on the CDF built from the distribution's
    # hazard rates
    #
    def draw_inverse_transform(self, curr_time):
        U = random.uniform(0,1)
        while U == 0:
            U = random.uniform(0,1)
        draw = ((-(self.scale**self.shape)*ln(U)+((curr_time)**self.shape))**(1/self.shape) - (curr_time))

        return abs(draw)



class Rack:

    ##
    # Three possible states of rack
    #
    STATE_RACK_NORMAL = "rack is normal"
    STATE_RACK_UNAVAILABLE = "rack is unavailable"
    STATE_RACK_CRASHED = "rack is crashed"

    ##
    # Possible failure events
    #
    EVENT_RACK_FAIL = "transient rack failure"
    EVENT_RACK_REPAIR = "repair for transient rack failure"


    def __init__(self, rack_fail_distr, rack_repair_distr):
        # Current state
        self.state = self.STATE_RACK_NORMAL
        # Transient failure distribution
        self.rack_fail_distr = rack_fail_distr
        # Repair distribution for transient rack failure
        self.rack_rapair_distr = rack_repair_distr


    def init_state(self):
        self.state = self.STATE_RACK_NORMAL


    ##
    # Transient rack failure
    #
    def fail_rack(self, curr_time):
        self.state = self.STATE_RACK_UNAVAILABLE


    ##
    # Repair for transient rack failure
    #
    def repair_rack(self):
        self.state = self.STATE_RACK_NORMAL


    ##
    # Get current state of this rack
    #
    def get_curr_state(self):
        return self.state



class Node:

    ##
    # Three possible states
    #
    STATE_NODE_NORMAL = "node is normal"
    STATE_NODE_UNAVAILABLE = "node is unavailable"
    STATE_NODE_CRASHED = "node is crashed"

    ##
    # Possible failure events
    #
    EVENT_NODE_FAIL = "node failure"
    EVENT_NODE_REPAIR = "node repair"
    EVENT_NODE_TRANSIENT_FAIL = "node transient failure"
    EVENT_NODE_TRANSIENT_REPAIR = "node transient repair"


    def __init__(self, node_fail_distr, node_transient_fail_distr, node_transient_repair_distr,
                 node_fail_trace=None, node_transient_fail_trace=None, node_transient_repair_trace=None):
        # Current state
        self.state = self.STATE_NODE_NORMAL

        # Failure distribution
        self.node_fail_distr = node_fail_distr
        self.node_transient_fail_distr = node_transient_fail_distr
        self.node_transient_repair_distr = node_transient_repair_distr

        # Add node fail events for trace
        self.node_fail_trace = node_fail_trace
        # Add node transient failure events from trace
        self.node_transient_fail_trace = node_transient_fail_trace
        self.node_transient_repair_trace = node_transient_repair_trace

        # The following is for importance sampling
        self.last_time_update = mpf(0)
        # Global begin time of this disk
        self.begin_time = mpf(0)
        # Local (relative) clock of this disk
        self.clock = mpf(0)
        # Local repair time of this disk
        self.repair_clock = mpf(0)
        self.repair_start = mpf(0)


    def init_clock(self, curr_time):
        self.last_time_update = curr_time
        self.begin_time = curr_time
        self.clock = mpf(0)
        self.repair_clock = mpf(0)
        self.repair_start = mpf(0)


    def init_state(self):
        self.state = self.STATE_NODE_NORMAL


    ##
    # Update node clocks.  There are three main clocks to update:
    #   the node clock
    #   the repair clock (if there is an ongoing repair)
    #   the time of the last clock update (used to update the node clock)
    #
    # The clock member variable is used to get instantaneous failure
    # rate, while the repair clock is used to get the instantaneous "repair" rate.
    #
    # @param curr_time: current simulation time
    #
    def update_clock(self, curr_time):
        self.clock += (curr_time - self.last_time_update)
        if self.state == self.STATE_NODE_CRASHED:
            self.repair_clock = (curr_time - self.repair_start)
        else:
            self.repair_clock = mpf(0)

        self.last_time_update = curr_time


    ##
    # Get this node's current time
    #
    # @return current node clock reading
    #
    def read_clock(self):
        return self.clock


    ##
    # Get this node's current repair time
    #
    # @return current node repair clock reading
    #
    def read_repair_clock(self):
        return self.repair_clock


    ##
    # Permanent node failure
    #
    def fail_node(self, curr_time):
        self.state = self.STATE_NODE_CRASHED
        self.repair_clock = mpf(0)
        self.repair_start = curr_time


    ##
    # Repair for permanent node failure
    #
    def repair_node(self):
        self.begin_time = self.last_time_update
        self.clock = mpf(0)  # this is considered as brand-new after repair
        self.repair_clock = mpf(0)
        self.state = self.STATE_NODE_NORMAL


    ##
    # Update the normal node as unavailable
    #
    def offline_node(self):
        if self.state == self.STATE_NODE_NORMAL:
            self.state = self.STATE_NODE_UNAVAILABLE


    ##
    # Update the unavailable disk as normal
    #
    def online_node(self):
        if self.state == self.STATE_NODE_UNAVAILABLE:
            self.state = self.STATE_NODE_NORMAL


    ##
    # Get current state of this node
    #
    def get_curr_state(self):
        return self.state

    ##
    # Get instantaneous failure rate of this node
    #
    # @return instantaneous whole-component failure rate
    #
    def curr_node_fail_rate(self):
        if self.state == self.STATE_NODE_CRASHED:
            return float(0)

        return self.node_fail_distr.hazard_rate(self.clock)


##
# This class encapsulates the state of a disk (i.e., disk) under simulation.
# Each disk is given failure and repair distributions for the entire disk.
#
# A disk may be in one of three states:
# NORMAL (operational, no failures) or
# UNAVAILABLE (unavailable due to transient failures) or
# CRASHED (entire disk is failed)
#
class Disk:

    ##
    # The three possible states
    #
    STATE_NORMAL = "state normal"
    STATE_UNAVAILABLE = "state unavailable"
    STATE_CRASHED = "state crashed"

    ##
    # Possible failure events
    #
    EVENT_DISK_FAIL = "disk failure"
    EVENT_DISK_REPAIR = "disk repair"


    ##
    # A disk is constructed by specifying the appropriate failure/repair distributions.
    # The disk fail/repair distributions must be specified.
    # This function will set the disk state to NORMAL and set all clocks to 0.
    # init_clock *must* first be called in order to use this object in simulation.
    #
    def __init__(self, disk_fail_distr, disk_repair_distr):
        # Current state
        self.state = self.STATE_NORMAL

        # keep record of the unavailable time of this disk
        self.unavail_start = mpf(0)
        self.unavail_clock = mpf(0)

        # Failure and repair distributions
        self.disk_fail_distr = disk_fail_distr
        self.disk_repair_distr = disk_repair_distr

        # The following clocks are mainly for importance sampling
        # Last "global" clock update
        self.last_time_update = mpf(0)
        # Global begin time of this disk
        self.begin_time = mpf(0)
        # Local (relative) clock of this disk
        self.clock = mpf(0)
        # Local repair time of this disk
        self.repair_clock = mpf(0)
        self.repair_start = mpf(0)


    ##
    # Set the last clock update to the current simulation time and initialize
    # t_0 for this disk (begin_time).
    #
    # @param curr_time: t_0 of this disk
    #
    def init_clock(self, curr_time):
        self.unavail_start = float(0)
        self.unavail_clock = float(0)
        self.last_time_update = curr_time
        self.begin_time = curr_time
        self.clock = mpf(0)
        self.repair_clock = mpf(0)
        self.repair_start = mpf(0)


    ##
    # Set the state of this disk to NORMAL
    #
    def init_state(self):
        self.state = self.STATE_NORMAL


    ##
    # Update disk clocks.  There are three main clocks to update:
    # the disk clock, the repair clock (if there is an ongoing repair)
    # and the time of the last clock update (used to update the disk clock)
    #
    # The clock member variable is used to get instantaneous failure
    # rate, while the repair clock is used to get the instantaneous "repair" rate.
    #
    # @param curr_time: current simulation time
    #
    def update_clock(self, curr_time):
        self.clock += (curr_time - self.last_time_update)
        if self.state == self.STATE_CRASHED:
            self.repair_clock = (curr_time - self.repair_start)
        else:
            self.repair_clock = float(0)
        self.last_time_update = curr_time


    ##
    # Get this disk's current time
    #
    # @return current disk clock reading
    #
    def read_clock(self):
        return self.clock


    ##
    # Get this disk's current repair time
    #
    # @return current disk repair clock reading
    #
    def read_repair_clock(self):
        return self.repair_clock


    ##
    # Get disk state
    #
    # @return disk state
    #
    def get_curr_state(self):
        return self.state


    ##
    # Fail this disk.  Reset list of failed sub-disks
    #
    def fail_disk(self, curr_time):
        if self.state == self.STATE_NORMAL:
            self.unavail_start = curr_time
        self.state = self.STATE_CRASHED
        self.repair_clock = float(0)
        self.repair_start = curr_time


    ##
    # Repair this disk.
    #
    def repair_disk(self, curr_time):
        self.state = self.STATE_NORMAL
        self.unavail_clock += curr_time - self.unavail_start
        self.begin_time = self.last_time_update
        self.clock = float(0)
        self.repair_clock = float(0)


    ##
    # Update the normal disk as unavailable
    #
    def offline_disk(self, curr_time):
        if self.state == self.STATE_NORMAL:
            self.state = self.STATE_UNAVAILABLE
            self.unavail_start = curr_time


    ##
    # Update the unavailable disk as normal
    #
    def online_disk(self, curr_time):
        if self.state == self.STATE_UNAVAILABLE:
            self.state = self.STATE_NORMAL
            self.unavail_clock += curr_time - self.unavail_start


    ##
    # Return the unavailable time of this disk
    #
    def get_unavail_time(self, curr_time):
        if self.state == self.STATE_NORMAL:
            return self.unavail_clock
        else:
            return self.unavail_clock + (curr_time - self.unavail_start)


    ##
    # Get instantaneous failure rate of this disk
    #
    # @return instantaneous whole-disk failure rate
    #
    def curr_disk_fail_rate(self):
        if self.state == self.STATE_CRASHED:
            return float(0)

        return self.disk_fail_distr.hazard_rate(self.clock)


    ##
    # Get instantaneous repair rate of this disk
    #
    # @return instantaneous whole-disk repair rate
    #
    def curr_disk_repair_rate(self):
        if self.state == self.STATE_NORMAL:
            return float(0)

        return self.disk_repair_distr.hazard_rate(self.repair_clock)


    ##
    # Return sum of instantaneous fail/repair rates
    #
    def inst_rate_sum(self):
        return self.curr_disk_fail_rate() + self.curr_disk_repair_rate()


def test():
    # Basic test of the Weibull functions
    w = Weibull(shape=float(2.0), scale=float(12), location=6)

    print "Weibull(%s,%s,%s): " % (w.shape, w.scale, w.location)

    sum = 0

    for i in range(10000):
        sum += w.draw_truncated(6)

    print "MEAN: ", float(sum) / 10000.

    print w.draw_inverse_transform(0)
    print w.draw_inverse_transform(0)
    print w.draw_inverse_transform(0)
    print w.draw_inverse_transform(0)

    print "Max hazard rate is %e\n" % w.get_max_hazard_rate(100)

    for i in range(0,200,5):
        print "CDF at time %d is %f\n" % (i, w.cdf_eval(i))

    w = Weibull(shape=float(1.0), scale=float(120000))

    print "Bunch of draws:"
    for i in range(10):
        print w.draw_inverse_transform(1000000)

    print "Weibull(%s,%s,%s): " % (w.shape, w.scale, w.location)

    print "Max hazard rate is %e\n" % w.get_max_hazard_rate(1000)

    for i in range(0,1000,100):
        print "Hazard rate at time %d is %e\n" % (i, w.hazard_rate(i))


if __name__ == "__main__":
    test()
