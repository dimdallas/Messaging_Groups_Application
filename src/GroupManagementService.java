import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class GroupManagementService{
    private static boolean gmsRun = true;

    //thread for multicast listening
    private static MulticastReceiver multicastReceiver = null;
    //thread for adding new client sockets
    private static SocketAcceptingThread serverThread;

    private static ServerSocket gmsSocket;

    //client id - udp socket
    private static Map<Integer, SocketAddress> knownUdpSockets;
    //tcp sockets
    private static List<Socket> activeSockets;

    //mutex for multicast receiving
    private static ReentrantLock udpMutex;

    public static void main(String[] args) {
        activeSockets = new CopyOnWriteArrayList<>();
        knownUdpSockets = new TreeMap<>();

        udpMutex = new ReentrantLock();

        Map<String, Group> groupMap = new LinkedHashMap<>();
        Map<Socket, ObjectInputStream> objInStreams = new LinkedHashMap<>();
        Map<Socket, ObjectOutputStream> objOutStreams = new LinkedHashMap<>();


        try {
            gmsSocket = new ServerSocket(0);
            serverThread = new SocketAcceptingThread();
            serverThread.start();
            System.out.println("my socket " + gmsSocket.getInetAddress().getHostName()+ ":" + gmsSocket.getLocalPort());
            multicastReceiver = new MulticastReceiver();
            multicastReceiver.start();

        } catch (IOException e) {
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
            multicastReceiver.Stop();
            gmsRun = false;

            try {
                gmsSocket.close();

                for (Socket socket: activeSockets) {
                    if(objInStreams.containsKey(socket))
                        objInStreams.get(socket).close();
                    if(objOutStreams.containsKey(socket))
                        objOutStreams.get(socket).close();
                    socket.close();
                }

                multicastReceiver.join(5000);
                serverThread.join(5000);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
            System.out.println("The server is shut down!");
        }});

        while (gmsRun){
                for (Socket socket : activeSockets) {
//                System.out.println(socket.getRemoteSocketAddress());
                    String tcpReceived = null;
                    ObjectOutputStream toGroupie = null;
                    ObjectInputStream fromGroupie = null;

                    try {
                        if(objInStreams.containsKey(socket)){
                            fromGroupie = objInStreams.get(socket);
                        }
                        else {
                            toGroupie = new ObjectOutputStream(socket.getOutputStream());
                            fromGroupie = new ObjectInputStream(socket.getInputStream());

                            objOutStreams.put(socket, toGroupie);
                            objInStreams.put(socket, fromGroupie);
                        }

                        //RECEIVE
                        //received "clientId - op(j/l) - groupId"
                        tcpReceived = (String)fromGroupie.readObject();
//                        System.out.println(socket.getRemoteSocketAddress() + " sent " + tcpReceived);

                        //!TIMEOUT && !EOF
                        int clientId = GetFirstInt(tcpReceived);
                        char op = tcpReceived.charAt(String.valueOf(clientId).length());
                        String groupName = tcpReceived.substring(String.valueOf(clientId).length() + 1);

                        String groupViewMsg;

                        String operation;
                        if(op == 'j') {
                            operation = "join";
                            groupViewMsg = "User " + clientId + " joined group " + groupName;
                        }
                        else if(op == 'l') {
                            operation = "leave";
                            groupViewMsg = "User " + clientId + " left group " + groupName;
                        }
                        else {
                            System.err.println("wrong op");
                            operation = "wrong";
                            groupViewMsg = "Unknown action from user " + clientId + " for group " + groupName;
                        }

                        System.out.println("clientId " + clientId);
                        System.out.println("op " + operation);
                        System.out.println("groupname " + groupName);

                        udpMutex.lock();
                        SocketAddress udpId = knownUdpSockets.get(clientId);
                        udpMutex.unlock();

                        Group group = null;
                        //group exists
                        if(groupMap.containsKey(groupName)){
                            group = groupMap.get(groupName);
                            System.out.println("Exists " + group.GetGroupName());

                            //not in group
                            if (!group.ContainsClient(clientId)) {
                                System.out.println("Not in group, client " + clientId);
                                //join
                                if(op == 'j') {
                                    //add him
                                    group.AddGroupie(clientId, socket, udpId);
                                    System.out.println(clientId + " client added to " + groupName);
                                }
                            }
                            //client in group
                            else {
                                if(op == 'l'){

                                    //remove him
                                    group.RemoveGroupie(clientId);
                                    System.out.println(clientId + " client removed from " + groupName);

                                    //if empty
                                    if(group.IsEmpty()) {
                                        //remove group from map
                                        groupMap.remove(groupName);
                                        System.out.println(groupName + " group deleted");
                                    }
                                    else {
                                        if(group.IsCoordinator(clientId))
                                            group.SetCoordinator();
                                    }
                                }
                            }
                        }
                        //group does not exist
                        else{
                            if(op == 'j') {
                                //create group
                                group = new Group(groupName, clientId);
                                //add client
                                group.AddGroupie(clientId, socket, udpId);
                                groupMap.put(groupName, group);
                                System.out.println(groupName + " group created");
                            }
                        }

                        //UPDATE GROUP VIEWS FOR ALL
                        if(group != null){
                            GroupView groupView = new GroupView(group, groupViewMsg);
                            for(Socket groupieTcp : group.GetTcpMap().values()){
                                if(groupieTcp.equals(socket))
                                    continue;
                                toGroupie = objOutStreams.get(groupieTcp);

                                toGroupie.writeObject("groupView");
                                toGroupie.writeObject(groupView);
                                toGroupie.flush();
                            }

                            //FINALLY INFORM NEW MEMBER
                            toGroupie = objOutStreams.get(socket);

                            toGroupie.writeObject(operation);
                            toGroupie.writeObject(groupView);
                            toGroupie.flush();
                        }
                    }
                    //GROUPIE FAILED, SOCKET CLOSED
                    catch (EOFException e){
                        //REMOVE FROM ALL GROUPS
                        List<Group> groups =  new LinkedList<>(groupMap.values());
                        for(Group group : groups) {
                            //client in group
                            if (group.ContainsClient(socket)) {
                                Integer userId = group.UserIdFromTcp(socket);
                                String groupViewMsg = "User " + userId + " failed, removed from group " + group.GetGroupName();
                                //remove him
                                group.RemoveGroupie(userId);
                                System.out.println(socket.getRemoteSocketAddress() + " client removed from " + group.GetGroupName());

                                //if empty
                                if (group.IsEmpty()) {
                                    //keep group removals
//                                        toBeRemoved[groups.indexOf(group)] = group.GetGroupName();
                                    groupMap.remove(group.GetGroupName());
                                    System.out.println(group.GetGroupName() + " group deleted");
                                }
                                //not empty
                                else {
                                    if(group.IsCoordinator(userId))
                                        group.SetCoordinator();
                                    //inform all in group
                                    GroupView newGroupView = new GroupView(group,groupViewMsg);
                                    for(Socket groupieSocket : group.GetTcpMap().values()){
                                        toGroupie = objOutStreams.get(groupieSocket);

                                        try {
                                            toGroupie.writeObject("groupView");
                                            toGroupie.writeObject(newGroupView);
                                            toGroupie.flush();
                                        } catch (IOException ex) {
//                                                ex.printStackTrace();
                                            System.err.println(ex.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
                                        }
                                    }
                                }
                            }
                        }

                        //XREIAZETAI? AN ANOIKSEI IDIA UDP PAIRNEI IDIO ID
//                        knownUdpSockets.remove()
                        activeSockets.remove(socket);
                        try {
                            objInStreams.get(socket).close();
                            objOutStreams.get(socket).close();
                            objInStreams.remove(socket);
                            objOutStreams.remove(socket);
                            socket.close();
                            System.out.println("removed " + socket.getRemoteSocketAddress());
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                    }
                    //TIMEOUT, INFORM HIM/ALL? IT WILL SLOW DOWN MUCHO
                    catch (IOException e) {
                        if (e.getMessage().equals("Read timed out")) {
                            //inform client for his groups
                        } else {
                            e.printStackTrace();
                        }
//                        System.err.println("Socket timeout");
//                    continue;
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                }
        }

        try {
            multicastReceiver.join(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static int GetFirstInt(String msg){

        String[] n = msg.split(""); //array of strings
        StringBuffer intBuffer = new StringBuffer(); // buffer to store numbers

        for (int i = 0; i < n.length; i++) {
            if((n[i].matches("[0-9]+"))) {// validating numbers
                intBuffer.append(n[i]); //appending
            }else {
                //parsing to int and returning value
                return Integer.parseInt(intBuffer.toString());
            }
        }
        return 0;
    }

    //THREAD FOR MULTICAST LISTENING
    static class MulticastReceiver extends Thread {
        private MulticastSocket multicastSocket = null;
        private InetAddress group = null;
        private boolean multicastRun = true;
        //hostname
        private String gmsAddress;
        //local port
        private int gmsPort;

        public MulticastReceiver() throws IOException {
            multicastSocket = new MulticastSocket(10000);
            group = InetAddress.getByName("224.0.0.1");
            multicastSocket.joinGroup(group);
            multicastSocket.setSoTimeout(5000);

            this.gmsAddress = gmsSocket.getInetAddress().getHostName();
            this.gmsPort = gmsSocket.getLocalPort();
        }

        //accepting multicast and replying [len-tcpAddress-len-Port-clientId]
        public void run(){
            byte[] rcvPacketBytes = new byte[30];

            //unique assignment
            int groupieId = 0;

            while (multicastRun) {
                DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);

                try {
                    multicastSocket.receive(rcv_packet);
                } catch (IOException e) {
//                e.printStackTrace();
                    continue;
                }

//                String received = new String(rcv_packet.getData(), 0, rcv_packet.getLength());
//                System.out.println("received" + rcv_packet.getAddress() + rcv_packet.getPort());

                SocketAddress udpSocket = rcv_packet.getSocketAddress();
                System.out.println("received udp " + udpSocket);

                byte[] replyBytes;

                udpMutex.lock();
                //if already received multicast from this client
                if(knownUdpSockets.containsValue(udpSocket)){
                    //find id by udp
                    int knownId = knownUdpSockets.entrySet().stream()
                            .filter(entry -> udpSocket.equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .findFirst().get();

                    //add groupieId to replyBytes
                    replyBytes = MulticastReply(knownId);
                }else{
                    groupieId++;
                    knownUdpSockets.put(groupieId, udpSocket);

                    int knownId = knownUdpSockets.entrySet().stream()
                            .filter(entry -> udpSocket.equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .findFirst().get();
                    System.out.println("known id " + knownId);
                    //add groupieId to replyBytes
                    replyBytes = MulticastReply(groupieId);
                }
                udpMutex.unlock();

                DatagramPacket packet = new DatagramPacket(replyBytes, replyBytes.length, rcv_packet.getSocketAddress());

                try {
                    multicastSocket.send(packet);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        public void Stop(){
            multicastRun = false;
            try {
                multicastSocket.leaveGroup(group);
            } catch (IOException e) {
                e.printStackTrace();
            }
            multicastSocket.close();
        }

        //reply bytes [len-tcpAddress-len-Port-clientId]
        private byte[] MulticastReply(int groupieId){
            byte[] addrBytes = gmsAddress.getBytes();
            int addrBytesLen = addrBytes.length;
            byte[] addrBytesLenBytes = ByteBuffer.allocate(4).putInt(addrBytesLen).array();
            byte[] portBytes = Integer.toString(gmsPort).getBytes();
            int portBytesLen = portBytes.length;
            byte[] portBytesLenBytes = ByteBuffer.allocate(4).putInt(portBytesLen).array();
            byte[] replyBytes = new byte[3 + addrBytesLen + portBytesLen];
//            System.out.println("addrLen"+addrBytesLen+"addr"+gmsAddress+"portLen"+portBytesLen+"port"+gmsPort+"id"+groupieId);
            System.arraycopy(addrBytesLenBytes, 3, replyBytes, 0, 1);
            System.arraycopy(addrBytes, 0, replyBytes, 1, addrBytesLen);
            System.arraycopy(portBytesLenBytes, 3, replyBytes, addrBytes.length+1, 1);
            System.arraycopy(portBytes, 0, replyBytes, addrBytes.length+2, portBytesLen);

            byte[] groupieIdBytes = ByteBuffer.allocate(4).putInt(groupieId).array();
            System.arraycopy(groupieIdBytes, 3, replyBytes, replyBytes.length-1, 1);

            return replyBytes;
        }
    }

    //THREAD FOR TCP ACCEPTING
    static class SocketAcceptingThread extends Thread {
        public void run(){
            while (gmsRun){
                try {
//                    System.out.println("Waiting accept");

                    //block until new client accept
                    Socket clientSocket = gmsSocket.accept();
                    //set timeout for list circulation
                    clientSocket.setSoTimeout(500);
                    activeSockets.add(clientSocket);

                    System.out.println("accepted " + clientSocket.getRemoteSocketAddress());

                } catch (IOException e) {
//                    System.err.println("Svc terminated");
//                    e.printStackTrace();
                }
            }
            System.err.println("\nSvc terminated");
        }
    }
}




