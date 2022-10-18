UDF Discovery and Communication
===============================

===============
Local Discovery
===============

*********
Overview:
*********

- UDP Broadcast for the initial PingMessage with connection information for the receiving socket of the reliable network
- After receiving PingMessage:

  - Create sending socket for peer to its receiving socket of the reliable network
  - Send PongMessage with our connection information for the receiving socket
    over our sending socket for it to inform the peer that we found it

- After receiving PongMessage on our receiving socket:

  - If not yet existing, create sending socket for peer to its receiving socket of the reliable network
  - Send PongMessage with our connection information for the receiving socket
    over our sending socket for it, in case we didn't get its PingMessage.
  - Send ReadyForReceivingMessage back, to signal that we are ready for communication
    - Necessary, because PingMessage maybe never arrived and PongMessage was the first we got, so we need to signalize,
      that we are ready to handle message from this peer

- After receiving ReadyForReceivingMessage:

  - Add peer to the list of peers we can send messages to.

********
Details:
********

We seperated the Local Discovery into two components. The component LocalDiscovery implements
the discovery via UDPBroadcast. The component PeerCommunicator handles the reliable network.

If the LocalDiscovery recieves a PingMessage via UDP it registers the connection info for
the reliable network of the peer with the PeerCommunicator.

The peer communicator then handles the reliable network communication.
This includes sending and recieving the PongMessage and ReadyForReceivingMessage.
It also provides an interface for the user of the library to check if all peers are connected, which peers are there
and to send and receive message from these peers.

The peer communicator can be used with different discovery strategies.
The LocalDiscovery is one, but the GlobalDiscovery can use it as well to form the reliable networks between the leaders.

The current implmentation of the peer communicator use ZMQ for the reliable communication,
because it abstracts away the low-level network. It provides:

- A message-based interface, instead the stream-based inteface of TCP.
- Asynchronous message queue, instead of synchronous TCP socket

  - Being asynchronous means that the timings of the physical connection setup and tear down,
    reconnect and effective delivery are transparent to the user and organized by ZeroMQ itself.
  - Further, messages may be queued in the event that a peer is unavailable to receive them.

-


================
Global Discovery
================

- Local Leader send UDP Message at known host and port
- Needs Local Discovery and Local Leader Election:,

  - If all UDF Instances would send messages to the one host, we might flood the network.

    - For example, with 64 Node and 100 Cores per Node, we can have over 6000 UDF Instances

  - Furthermore, we would like to have a single UDF instance listening on the known host and port
    which then can handle Global Discovery and Leader Election. Using a single instance, simplifies the implementation.


===================
Local Communication
===================

====================
Global Communication
====================

