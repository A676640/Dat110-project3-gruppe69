package no.hvl.dat110.middleware;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;

/**
 * Distributed Mutual Exclusion implementation.
 * Implements a primary-based remote write protocol that also handles distributed mutual exclusion.
 */
public class MutualExclusion {
    
    private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
    
    /** Lock variables */
    private boolean CS_BUSY = false;         // true if the node is currently in the critical section (CS)
    private boolean WANTS_TO_ENTER_CS = false; // true if the node wants to enter CS
    private List<Message> queueack;          // list to accumulate acknowledgements from other peers
    private List<Message> mutexqueue;        // queue for storing requests that cannot be granted immediately
    
    private LamportClock clock;              // Lamport clock
    private Node node;                       // our node reference
    
    public MutualExclusion(Node node) throws RemoteException {
        this.node = node;
        clock = new LamportClock();
        queueack = new ArrayList<>();
        mutexqueue = new ArrayList<>();
    }
    
    /**
     * Indicate that we've obtained the lock (entered the CS)
     */
    public synchronized void acquireLock() {
        CS_BUSY = true;
    }
    
    /**
     * Release the lock so that this node is no longer in the CS.
     */
    public void releaseLocks() {
        WANTS_TO_ENTER_CS = false;
        CS_BUSY = false;
    }
    
    /**
     * Main entry point for the mutual exclusion request.
     * This method is invoked when the node wishes to update a file.
     * It multicasts its request, waits for acknowledgements from all unique active peers,
     * enters the critical section, broadcasts the update, and then clears its request state.
     *
     * @param message the request message (should include node name, node ID, and initial clock)
     * @param updates the update (in bytes) to be applied to the file
     * @return true if the node gained access to the critical section and performed the update
     * @throws RemoteException
     */
    public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
        
        logger.info(node.nodename + " wants to access the critical section (CS).");
        
        // Clear previous acknowledgements and queued requests.
        synchronized(queueack) {
            queueack.clear();
        }
        mutexqueue.clear();
        
        // Increment clock and set the timestamp on the request message.
        clock.increment();
        int timestamp = clock.getClock();
        message.setClock(timestamp);
        
        // Indicate that this node now wants to enter CS.
        WANTS_TO_ENTER_CS = true;
        
        // Remove duplicate peers from the active nodes list before voting.
        List<Message> uniquePeers = removeDuplicatePeersBeforeVoting();
        
        // Multicast the mutex request to all peers (including self).
        multicastMessage(message, uniquePeers);
        
        // Wait for acknowledgments using wait/notify.
        long timeoutMillis = 5000; // Total wait timeout of 5 seconds.
        long endTime = System.currentTimeMillis() + timeoutMillis;
        synchronized(queueack) {
            while (queueack.size() < uniquePeers.size()) {
                long remaining = endTime - System.currentTimeMillis();
                if (remaining <= 0) {
                    logger.error("Timeout while waiting for mutex acknowledgments.");
                    return false;
                }
                try {
                    queueack.wait(remaining);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            // Optionally clear the queue after successful waiting.
            queueack.clear();
        }
        
        // All acks are received: acquire the lock (enter the critical section).
        acquireLock();
        
        // Now broadcast the update to all peers holding a replica.
        node.broadcastUpdatetoPeers(updates);
        
        // Clear the queued requests for later processing.
        mutexqueue.clear();
        
        // Return that permission was granted and the update was performed.
        return true;
    }

    
    /**
     * Multicasts a mutex request message to a list of active peers.
     */
    private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {
        logger.info("Multicasting mutex request to " + activenodes.size() + " peers.");
        for (Message m : activenodes) {
            NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());
            if (stub != null) {
                logger.info("Sending request to: " + m.getNodeName());
                stub.onMutexRequestReceived(message);
            }
        }
    }

    
    /**
     * Handler invoked when a peer receives a mutex request.
     */
    public void onMutexRequestReceived(Message message) throws RemoteException {
        // Increment local clock.
        clock.increment();
        
        int caseid = -1;
        
        /* Determine which case applies:
         * Case 0: Receiver is not in CS AND does not want to enter -> send OK immediately.
         * Case 1: Receiver is already in CS -> queue the request.
         * Case 2: Receiver wants CS but hasn't entered -> compare timestamps and decide.
         */
        if (!CS_BUSY && !WANTS_TO_ENTER_CS) {
            caseid = 0;
        } else if (CS_BUSY) {
            caseid = 1;
        } else if (WANTS_TO_ENTER_CS) {
            Message myRequest = node.getMessage(); // assume this returns our own request message.
            if (message.getClock() < myRequest.getClock()) {
                caseid = 0;
            } else if (message.getClock() == myRequest.getClock()) {
                // If timestamps are equal, the node with the lower node ID wins.
                if (node.getNodeID().compareTo(message.getNodeID()) < 0) {
                    caseid = 1;
                } else {
                    caseid = 0;
                }
            } else {
                caseid = 1;
            }
        }
        
        doDecisionAlgorithm(message, mutexqueue, caseid);
    }
    
    /**
     * Implements the decision algorithm.
     * - If condition is 0, send an acknowledgement immediately.
     * - If condition is 1, queue the request.
     */
    public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {
        String senderName = message.getNodeName();
        int senderPort = message.getPort();
        
        switch(condition) {
            case 0: {
                // Not in CS and not requesting: send OK immediately.
                NodeInterface stub = Util.getProcessStub(senderName, senderPort);
                if (stub != null) {
                    Message ack = new Message();
                    ack.setClock(clock.getClock());
                    ack.setNodeName(node.nodename);
                    ack.setNodeID(node.getNodeID());
                    // Send acknowledgement back.
                    stub.onMutexAcknowledgementReceived(ack);
                }
                break;
            }
            case 1: {
                // Already in CS or waiting: queue the request.
                queue.add(message);
                break;
            }
            default:
                break;
        }
    }
    
    /**
     * Handler invoked when an acknowledgement is received.
     */
    public void onMutexAcknowledgementReceived(Message message) throws RemoteException {
        synchronized(queueack) {
            queueack.add(message);
            logger.info("Ack received from " + message.getNodeName() +
                        "; queueack size = " + queueack.size());
            queueack.notifyAll();
        }
    }


    
    /**
     * Multicasts a release lock message to all peers (so they can process any queued requests).
     */
    public void multicastReleaseLocks(Set<Message> activenodes) {
        logger.info("Releasing locks at " + activenodes.size() + " peers.");
        for (Message m : activenodes) {
            NodeInterface stub = Util.getProcessStub(m.getNodeName(), m.getPort());
            if (stub != null) {
                try {
                    stub.releaseLocks();
                } catch (RemoteException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    /**
     * Checks if acknowledgements have been received from all voters.
     * Returns true if the number of acknowledgments equals the number of voters.
     */
    private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
        logger.info(node.nodename + ": queueack size = " + queueack.size());
        boolean allReturned = (queueack.size() == numvoters);
        if (allReturned) {
            queueack.clear();
            return true;
        }
        return false;
    }

    
    /**
     * Removes duplicate peers from the list of active nodes to ensure each node votes only once.
     */
    private List<Message> removeDuplicatePeersBeforeVoting() {
        List<Message> uniquePeers = new ArrayList<>();
        for (Message p : node.activenodesforfile) { // assuming node.activenodesforfile is available
            boolean found = false;
            for (Message p1 : uniquePeers) {
                if (p.getNodeName().equals(p1.getNodeName())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                uniquePeers.add(p);
            }
        }
        return uniquePeers;
    }
}
