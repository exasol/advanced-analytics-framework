UDF Discovery and Communication
===============================

===================
Establish Connection
===================

* We use a protocol similar to the TCP Handshake for establishing the connection
* However, our protocol has two different requirements compared to TCP:

  * Two peers can establish the connection at the same time
  * In case of lost messages, one of the peers in a connection can successful terminate

* To handle, these two requirements, we add the following modification:

  * We allow both peers to send a synchronize at the same time
  * When a peer receives the `SynchronizeConnectionMessage` from the second peer

    * It sends first a `SynchronizeConnectionMessage` and `AcknowledgeConnectionMessage` back
    * It can mark the second peer as ready, after it waited for the peer_is_ready_wait_time

* Both peers register each other:

.. image:: establish_connection/sequence/both_peers_receive_register_peer.png

* One peer registers the other peer:

.. image:: establish_connection/sequence/one_peer_receives_register_peer.png

* Both peers register each other, one peer loses the `SynchronizeConnectionMessage`:

.. image:: establish_connection/sequence/both_peers_receive_register_peer_one_peer_loses_synchonize.png

* Both peers register each other, both lose the `SynchronizeConnectionMessage`:

.. image:: establish_connection/sequence/both_peers_receive_register_peer_both_peers_lose_synchonize.png

* State diagram:

.. image:: establish_connection/state_diagram.png

========================
Local Discovery Strategy
========================

- The Local Discovery Strategy sends `PingMessage` with connection information
  for establishing the connection via UDP Broadcast.
- When a peer receives a `PingMessage` from another peer.
  it registers the other peer and treis to establish a connection
- The strategy sends and receives UDP Broadcast messages until all other peers are connected

* Both peers receive `PingMessage`:

.. image:: establish_connection/both_peer_receive_ping.png

* One peer receive `PingMessage`:

.. image:: establish_connection/one_peer_receives_ping.png

