# CS 425 MP3: MapleJuice

## Project Setup Requirement

Please install the following tools with correct version first:

1. Java 8 (Openjdk)
2. Apache Maven

## Source Code path

1. MP1: `MP1-archive/distributed-group-membership/src/main/java/edu/cs425/mp1` 
2. MP2: `MP2-archive/src/main/java/edu/cs425/mp2`
3. MP3: `src/main/java/edu/cs425/mp3`
4. MP3 Report: root path

## Steps to build & run project

1. Change to the root of our project directory: `cd distributed-group-membership/`

2. Start the server process by issuing the command: `./start.sh <node number> <deployment mode>` 

* 1st argument: Node number (range: [1, 10], according to vm number)
* 2th argument: 1 if you run the process in your local environment; 2 if you run the process on our group's vm.

3. After you start the server process, you can begin testing using the following command that our command line interface (CLI) supports:
* `maple [maple_exe] [num_maples] [sdfs_intermediate_filename_prefix] [sdfs_src_directory]`
    
    * `maple_exe`: a user-specified executable that takes as input one file and outputs a series of (key, value) pairs. `maple_exe` is the file name local to the file system of wherever the command is executed (alternately, store it in SDFS).

    * `num_maples`: number of maple tasks.

    * `sdfs_intermediate_filename_prefix`: the prefix of the maple output files. For a key K, all (K, any_value) pairs output by any Maple task must be appended to the file sdfs_intermediate_filename_prefix_K.

    * `sdfs_src_directory`: a directory that specifies the location of the input files.

* `juice <juice_exe> <num_juices> <sdfs_intermediate_filename_prefix> <sdfs_dest_filename> delete_input={0,1} shuffle_option={1,2}`

    * `juice_exe`:  a user-specified executable that takes as input multiple(key, value) input lines, processes groups of (key, any_values) input lines together(sharing the same key, just like Reduce), and outputs (key, value) pairs. `juice_exe` is the file name local to the file system of wherever the command is executed (we also store it in SDFS).

    * `num_juices`:  the number of Juice tasks (typically the same as the number of machines

    * `sdfs_intermediate_filename_prefix`: a prefix of file indicating what intermediate files to read and preocess

    * `sdfs_dest_filename`: the name of the juice output file.

    * `delete_input`: 0 indicating not to delete the input file, 1 indicating delete the input file.

    * `shuffle_option`: 1 indicating hash partitioning, 2 indicating range partitioning.

* `put [localfilename] [sdfsfilename]`

    Insert/Update a local file named `localfilename` into a file in the SDFS file system named `sdfsfilename`.

* `get [sdfsfilename] [localfilename]`

    Fetch a file named `sdfsfilename` from the SDFS file system to the local directory, and name it `localfilename`.

* `delete [sdfsfilename]`

    Delete the file named `sdfsfilename` from the SDFS file system.

*  `ls [sdfsfilename]`

    List all machine (VM) addresses where this file is currently being stored.

*   `store`

    List all files currently being stored at the current machine.

*   `global`

    List all files currently being stored at the ALL machines.

* `info`

    Print basic information about the current running node: `NodeId`, `Heartbeat Count`, `Unix Timestamp when node was created`, `Converted timestamp (CDT)`, `Status`.

* `ls`

    Print information of all nodes in the membership list. Information includes: `NodeId`, `Heartbeat Count`, `Latest timestamp (Timestamp when last heartbeat was received)`, `Converted timestamp (CDT)`, `Status`. If the node is not in the group, it will not print membership list. Introducer will always be in the group automatically.

* `join`

    Join the current running node to the group. Optional argument: 1 (All-to-All), 2 (Gossip).

* `leave`

    Let the node leave the group.

* `switch`

    Switch the failure detection protocol.

* `stop`

    Stop the both SDFS Server and Membership Server process. You can also use `Ctrl C` to kill the server process.

## Contact

[Dayue Bai](mailto:dayueb2@illinois.edu), [Yitan Ze](mailto:yitanze2@illinois.edu) (in alphabetical order)
