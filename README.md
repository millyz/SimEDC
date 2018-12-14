# SimEDC

SimEDC is a discrete-event simulator that characterizes the reliability of an
erasure-coded data center via simulation. It reports the reliability metrics
based on the configurable inputs of the data center topology, erasure codes,
redundancy placements, failure/repair patterns of different subsystems obtained
from statistical models or production traces.

We build SimEDC on the High-Fidelity Reliability Simulator
([HFRS](http://www.kaymgee.com/Kevin_Greenan/Software_files/hfrs.tar))
developed by [Kevin
Greenan](http://www.kaymgee.com/Kevin_Greenan/Welcome.html).

## Prerequisite

Please install [mpmath](http://code.google.com/p/mpmath/), [numpy](http://www.numpy.org/) and [scipy](http://www.scipy.org/).

## Usage

### Basics 

```
python simedc.py
-n <code_n> [--code_n <code_n>]
-k <code_k> [--code_k <code_k>]
-t <code_type> [--code_type <code_type>]
-T <place_type> [--place_type <place_type>]
-g <chunk_rack_config [--chunk_rack_config <chunk_rack_config>]
```
For more details, please run `python simedc.py -h`.  

### Examples

Set a data center with 16 racks and 8 nodes per rack. 

1. RS(9,6) in flat placement 

	`python simedc.py -n 9 -k 6 -t rs -T flat`  

	*Results:*

	- PDL = 0.000000e+00
	- RE = 0.0%
	- NOMDL (bytes/byte) = 0.000000e+00
	- BR = 4.430000e-04

2. RS(9,6) in hierarchical placement

	`python simedc.py -n 9 -k 6 -t rs -T hie -g 3,3,3`

	*Results:* 

	- PDL = 0.000000e+00
	- RE = 0.0%
	- NOMDL (bytes/byte) = 0.000000e+00
	- BR = 4.175000e-04

## Documentation

### Files

- simedc.py: the main command line interface of SimEDC

- README: this file

- lib: the library of SimEDC

### Library of SimEDC
- simulation.py: contains *class Simulation* and its functions

- regular_simulation.py: contains *class RegularSimulation* which is inherited from *class Simulation*

- network.py: contains *class Network* and its functions to keep track of the network bandwidth

- placement.py: contains *class Placement*, including 
	* different erasure codes (i.e., Reed-Solomon Code, Locally Repairable Codes, and Double Regenerating Codes)
	* different placement policies (i.e., flat placement and hierarchical placement)

- smp\_data\_structures.py: contains
 
  * *class Disk, Node* and *Rack* and their functions 
  * *class Weibull* and its functions 

- state.py: encapsulates the system state

- bm_ops.py: contains functions of bitmap for different subsystems

- sim\_analysis\_functions.py: contains *class Samples* which encapsulates a set of statistics operations
- tracelib: the library for using traces
  * trace.py: contains *class Parser* and *Trace* to parse traces and obtain
  node failure/repair events (i.e., node permanent failures, node transient
  failures/repairs)

	* data: contains trace.csv

## Contact
Please email to Mi Zhang (mzhang@cse.cuhk.edu.hk) if you have any questions.

## Publication
Mi Zhang, Shujie Han, and [Patrick P.C. Lee](http://www.cse.cuhk.edu.hk/~pclee). 

"A Simulation Analysis of Reliability in Erasure-Coded Data Centers". *SRDS 2017*
