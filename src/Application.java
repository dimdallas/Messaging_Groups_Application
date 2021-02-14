import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings("restriction")
public class Application {
    private static AppMiddleware myMiddleware = null;
    private static List<Integer> groupSockets;
    private static List<String> groupNames;
    public static boolean appRun = true; //flag for application termination
    public static Integer gsock; //for general use
    public static List<Thread> threads;
    public static ArrayList<Long> startTimes;
    public static ArrayList<Long> endTimes;

    public static boolean withTotal =false;

    public static void main(String[] args) {
        threads = new LinkedList<>();
        groupSockets = new LinkedList<>();
        groupNames = new LinkedList<>();

        Scanner scanner = new Scanner(System.in);
        String userInput;

        System.out.print("Total communication(y/n): ");
        userInput = scanner.nextLine();
        withTotal = userInput.equals("y");

        try {
            myMiddleware = new AppMiddleware();
            myMiddleware.start();
        } catch (IOException e) {
            System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(){public void run(){
            appRun = false;
            myMiddleware.Terminate();
            try {
                myMiddleware.join(5000);
                for(Thread thread : threads){
                    thread.join(5000);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("The client is shut down!");
        }});

        boolean endUI = false;
        boolean inFile = false;
        boolean fromFile = false;
        System.out.print("Write on file(y/n): ");
        userInput = scanner.nextLine();
        inFile = userInput.equals("y");

        System.out.print("Read from file(y/n): ");
        userInput = scanner.nextLine();
        fromFile = userInput.equals("y");

        while (appRun){
            System.out.println("(j) Join Group\n(l) Leave Group\n(s) Send Message\n(r) Receive Messages\n(p) Print Groups-Gsocks\n(e) Exit");
            System.out.println("Your choice: ");
            userInput = scanner.nextLine();

            switch (userInput){
                case "j":
                    System.out.print("GroupName: ");
                    userInput = scanner.nextLine();
                    GrpJoin(userInput);
                    break;
                case "l":
                    System.out.print("Gsock or GroupName: ");
                    userInput = scanner.nextLine();
                    try {
                        gsock = Integer.valueOf(userInput);
                        if(!groupSockets.contains(gsock)){
                            System.out.println("Not in group from gsock " + gsock);
                            break;
                        }
                    }catch (NumberFormatException e){
                        if(groupNames.contains(userInput))
                            gsock = groupSockets.get(groupNames.indexOf(userInput));
                        else {
                            System.out.println("Not in group from gsock " + userInput);
                            break;
                        }
                    }

                    GrpLeave(gsock);
                    break;
                case "s":
                    System.out.print("Gsock or GroupName: ");
                    userInput = scanner.nextLine();
                    try {
                        gsock = Integer.valueOf(userInput);
                        if(!groupSockets.contains(gsock)){
                            System.out.println("Not in group from gsock " + gsock);
                            break;
                        }
                    }catch (NumberFormatException e){
                        if(groupNames.contains(userInput))
                            gsock = groupSockets.get(groupNames.indexOf(userInput));
                        else {
                            System.out.println("Not in group from gsock " + userInput);
                            break;
                        }
                    }
                    if(fromFile){
                        try {
                            File myObj = new File("input.txt");
                            Scanner myReader = new Scanner(myObj);
                            while (myReader.hasNextLine()) {
                                String message = myReader.nextLine();
                                GrpSend(gsock, message);
                            }
                            myReader.close();
                        } catch (FileNotFoundException e) {
                            System.out.println("An error occurred.");
                            e.printStackTrace();
                        }
                    }else {
                        System.out.print("Message: ");
                        String message = scanner.nextLine();

                        GrpSend(gsock, message);
                    }
                    break;
                case "r":
                    System.out.print("Gsock or GroupName: ");
                    userInput = scanner.nextLine();
                    try {
                        gsock = Integer.valueOf(userInput);
                        if(!groupSockets.contains(gsock)){
                            System.out.println("Not in group from gsock " + gsock);
                            break;
                        }
                    }catch (NumberFormatException e){
                        if(groupNames.contains(userInput))
                            gsock = groupSockets.get(groupNames.indexOf(userInput));
                        else {
                            System.out.println("Not in group from gsock " + userInput);
                            break;
                        }
                    }

                    System.out.println("With block?(y/n) ");
                    userInput = scanner.nextLine();
                    boolean withBlock = userInput.equals("y");

                    if(withBlock){
                        Thread thread;
                        if(inFile) {
                            thread = new Thread() {
                                int myGsock = gsock;

                                public void run() {
                                    GrpReceive(myGsock, true, true);
                                }
                            };
                        }
                        else {
                            thread = new Thread() {
                                int myGsock = gsock;

                                public void run() {
                                    GrpReceive(myGsock, true, false);
                                }
                            };
                        }
                        threads.add(thread);
                        thread.start();
                    }else {
                        GrpReceive(gsock, false, false);
                    }
                    break;
                case "p":
                    PrintGroupsGsocks();
                    break;
                case "e":
                    endUI = true;
                    break;

                default:
            }
            if(endUI)
                break;
        }
        LinkedList<Integer> myGsocks = new LinkedList<>(groupSockets);
        for(Integer gs : myGsocks){
            GrpLeave(gs);
        }

        appRun = false;
        myMiddleware.Terminate();
    }

    //API METHODS
    public static void GrpJoin(String groupname) {
        Integer gsock = myMiddleware.JoinGroup(groupname);

        if(gsock == null){
            System.out.println("gsock error");
        }else if(!groupSockets.contains(gsock)) {
            groupNames.add(groupname);
            groupSockets.add(gsock);
            System.out.println("App Joined group " + groupname + " with gsock " + gsock);
        }
        else {
            System.out.println("Already joined " + groupname + " with gsock " + gsock);
        }
    }

    public static void GrpLeave(Integer gsock) {
        if(myMiddleware.LeaveGroup(gsock)){
            int index = groupSockets.indexOf(gsock);
            groupNames.remove(index);
            groupSockets.remove(gsock);
            System.out.println("Removed from gsock " + gsock);
        }else{
            System.out.println("Not in group from gsock " + gsock);
        }
    }

    public static void GrpSend(Integer gsock, String msg){
        if(myMiddleware.SendGroup(gsock, msg)){
//            System.out.println("Message sent to gsock " + gsock);
        }
        else{
            System.out.println("Not in group from gsock " + gsock);
        }
    }

    public static void GrpReceive(Integer gsock, boolean block, boolean inFile){
        if(!groupSockets.contains(gsock)){
            System.out.println("Not in group from gsock " + gsock);
            return;
        }
        Object notification;

        do {
            notification = myMiddleware.ReceiveGroup(gsock);

            if(notification == null){
                System.out.println("Removed from group");
            }
            /*else if(notification.getClass() == Integer.class){
                System.out.println("No new messages for group");
            }*/
            else if (notification.getClass() == GroupView.class) {
                GroupView groupView = (GroupView) notification;
                //FOR WRITING GMS NOTIFICATIONS IN FILES
                /*if(withTotal){
                    if (inFile) {
                        WriteFilesTotal(gsock, groupView);
                    } else {
                        System.out.println(groupView.GetViewMsg());
                    }
                }
                else {
                    if (inFile) {
                        WriteFiles(gsock, groupView);
                    } else {
                        System.out.println(groupView.GetViewMsg());
                    }
                }*/
                System.out.println(groupView.GetViewMsg());
            /*System.out.println("View of group " + groupView.groupName+", gsock " + gsock +":");
            for(Map.Entry<Integer, SocketAddress> memberEntry : groupView.udpMap.entrySet()){
                System.out.println("id " + memberEntry.getKey() + " at " + memberEntry.getValue());
            }*/
            } else if (notification.getClass() == AppMiddleware.Message.class) {
                AppMiddleware.Message message = (AppMiddleware.Message) notification;
                if(withTotal){
                    if (inFile) {
                        WriteFilesTotal(gsock, message);
                    } else {
                        System.out.println("Read gsock "+ gsock + " total "+message.GetTotalSeqNo()+ " id " + message.GetIdentifier() + " with msg: " + message.GetMessage());
                    }
                }
                else {
                    if (inFile) {
                        WriteFiles(gsock, message);
                    } else {
                        System.out.println("Read gsock "+ gsock + " id "  + message.GetIdentifier() + " with msg: " + message.GetMessage());
                    }
                }
            }
        }while (block && notification != null);
    }

    public static void WriteFiles(Integer gsock, Object notification){
        if(notification.getClass() == GroupView.class){
            GroupView groupView = (GroupView)notification;
            LinkedList<Integer> clients = new LinkedList<>(groupView.GetClientIds());

            for(Integer id : clients){
                try {
                    File myObj = new File("screen"+myMiddleware.myGroupieId+"_gsock"+gsock+"_user"+id);
                    if (myObj.createNewFile()) {
//                        System.out.println("File created: " + myObj.getName());
                    } else {
//                        System.out.println("File already exists.");
                    }
                } catch (IOException e) {
                    System.out.println("An error occurred. gv creating");
                    e.printStackTrace();
                }
            }

            for(Integer id : clients){
                try {
                    FileWriter myWriter = new FileWriter("screen"+myMiddleware.myGroupieId+"_gsock"+gsock+"_user"+id,true);
                    BufferedWriter writer = new BufferedWriter(myWriter);
                    writer.append(groupView.GetViewMsg() + "\n");
                    writer.close();
                } catch (IOException e) {
                    System.out.println("An error occurred. gv writing");
                    e.printStackTrace();
                }
            }
        }
        else{
            AppMiddleware.Message message = (AppMiddleware.Message) notification;

            try {
                File myObj = new File("screen"+myMiddleware.myGroupieId+"_gsock"+gsock+"_user"+message.GetClientId());
                if (myObj.createNewFile()) {
//                    System.out.println("File created: " + myObj.getName());
                } else {
//                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred. msg creating");
                e.printStackTrace();
            }

            try {
                FileWriter myWriter = new FileWriter("screen"+myMiddleware.myGroupieId+"_gsock"+gsock+"_user"+message.GetClientId(),true);
                BufferedWriter writer = new BufferedWriter(myWriter);
                writer.append("Id " + message.GetIdentifier()+" "+message.GetMessage() + "\n");
                writer.close();
            } catch (IOException e) {
                System.out.println("An error occurred. msg writing");
                e.printStackTrace();
            }
        }
    }

    public static void WriteFilesTotal(Integer gsock, Object notification){
        if(notification.getClass() == GroupView.class){
            GroupView groupView = (GroupView)notification;
            LinkedList<Integer> clients = new LinkedList<>(groupView.GetClientIds());

            try {
                File myObj = new File("total_screen"+myMiddleware.myGroupieId+"_gsock"+gsock/*+"user"+id*/);
                if (myObj.createNewFile()) {
//                        System.out.println("File created: " + myObj.getName());
                } else {
//                        System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred. gv creating");
                e.printStackTrace();
            }

            try {
                FileWriter myWriter = new FileWriter("total_screen"+myMiddleware.myGroupieId+"_gsock"+gsock/*+"user"+id*/,true);
                BufferedWriter writer = new BufferedWriter(myWriter);
                writer.append(groupView.GetViewMsg()+"\n");
                writer.close();
                /*myWriter.write(groupView.GetViewMsg());
                myWriter.close();*/
//                    System.out.println("Successfully wrote to the file.");
            } catch (IOException e) {
                System.out.println("An error occurred. gv writing");
                e.printStackTrace();
            }

        }
        //IF IT IS A MESSAGE TO WRITE IN FILE
        else{
            AppMiddleware.Message message = (AppMiddleware.Message) notification;

            try {
                File myObj = new File("total_screen"+myMiddleware.myGroupieId+"_gsock"+gsock);
                if (myObj.createNewFile()) {
//                    System.out.println("File created: " + myObj.getName());
                } else {
//                    System.out.println("File already exists.");
                }
            } catch (IOException e) {
                System.out.println("An error occurred. msg creating");
                e.printStackTrace();
            }

            try {
                FileWriter myWriter = new FileWriter("total_screen"+myMiddleware.myGroupieId+"_gsock"+gsock,true);
                BufferedWriter writer = new BufferedWriter(myWriter);
                writer.append("Seq "+message.GetTotalSeqNo() + " id " + message.GetIdentifier()+" "+message.GetMessage()+"\n");
                writer.close();
            } catch (IOException e) {
                System.out.println("An error occurred. msg writing");
                e.printStackTrace();
            }
        }
    }

    public static void PrintGroupsGsocks(){
        for(int i = 0; i <groupSockets.size(); i++){
            System.out.println(groupSockets.get(i) + " for " + groupNames.get(i));
        }
    }

    public static void GetDelayTime(){
        long delayTime = 0;
        long startTime = 0;
        long endTime = 0;

        for(int i=0; i < startTimes.size(); i++){
            delayTime += endTimes.get(i) - startTimes.get(i);
//            long delaySeconds = TimeUnit.SECONDS.convert(delayTime, TimeUnit.NANOSECONDS);
        }

        double seconds = (double)delayTime / 1_000_000_000.0;

        seconds = seconds/startTimes.size();

        System.out.println("Average Delay time " + String.format("%.2f", seconds));
    }
}

class AppMiddleware extends Thread{
    //GMS COMMUNICATION
    String multicastMessage = "254"; //set multicast payload
    private InetAddress group = InetAddress.getByName("224.0.0.1"); //for all-hosts
    private Socket tcpSocket;
    private ObjectOutputStream toGMS;
    //thread for gms listening
    private TcpListener myTcpListener;

    //USERS COMMUNICATION
    private DatagramSocket udpSocket; //group communication socket
    public int myGroupieId;  //my unique user id
    private Map<String, Integer> myCoordSeqs; //seqNos for groups i coordinate
    private boolean withTotal = Application.withTotal;  //communication protocol, default FIFO Reliable Multicast

    //GROUPS
    private Map<String, GroupView> groupViewMap;  //known groupViews
    private Map<Integer, String> gsockNameMap;  //sock -> name
    private Map<String, Integer> nameGsockMap;  //name -> sock

    //MESSAGE BUFFERS
    private Map<String, Integer> groupSeqNumbers;  //seqNos for all my groups
    private Map<Integer, LinkedList<Message>> sendingBuffer;  //saving messages for sending, remove when acked
    private Map<Integer, LinkedList<Message>> messageBuffer;  //saving messages for replying acks
    private Map<Integer, LinkedList<Message>> ackedMessages;  //messages reliably casted

    //APP BUFFER
    private Map<String, LinkedList<Object>> deliveredBuffer;  //add only to specify toBeDelivered
    private Map<String, LinkedList<Object>> toBeDelivered;  //poll lists for app delivery

    //THREAD CONCURRENCY
    private ReentrantLock tcpMutex;  //mutex for gms-related buffers
    private ReentrantLock udpMutex;  //mutex for group-related buffers
    private ReentrantLock appMutex;  //mutex for app-related buffers

    //METRICS
    private int UDPsTotal =0;


    //CONSTRUCTOR
    public AppMiddleware() throws IOException {
        udpSocket = new DatagramSocket();
        System.out.println("my udp port " + udpSocket.getLocalPort());
        myTcpListener = new TcpListener();
        tcpMutex = new ReentrantLock();
        udpMutex = new ReentrantLock();
        appMutex = new ReentrantLock();
        groupViewMap = new LinkedHashMap<>();
        gsockNameMap = new LinkedHashMap<>();
        nameGsockMap = new LinkedHashMap<>();
        groupSeqNumbers = new LinkedHashMap<>();
        sendingBuffer = new LinkedHashMap<>();
        messageBuffer = new LinkedHashMap<>();
        ackedMessages = new LinkedHashMap<>();
        deliveredBuffer = new LinkedHashMap<>();
        toBeDelivered = new LinkedHashMap<>();
        myCoordSeqs = new LinkedHashMap<>();


        try {
            Multicast(multicastMessage);
            myTcpListener.start();
        } catch (IOException e) {
            Terminate();
            throw e;
        }
    }

    //API METHODS
    public Integer JoinGroup(String groupname){
        Integer gsock = null;

        if(gsockNameMap.containsValue(groupname)) {
            gsock = gsockNameMap.entrySet().stream()
                    .filter(entry -> groupname.equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .findFirst().get();
            return gsock;
        }

        String request = myGroupieId + "j" + groupname;

        SendGMS(request);

        udpMutex.lock();
        while(gsock == null){
            tcpMutex.lock();
            if(groupViewMap.containsKey(groupname)){
                gsock = groupname.hashCode();
            }
            tcpMutex.unlock();
        }


        sendingBuffer.put(gsock, new LinkedList<>());
        messageBuffer.put(gsock, new LinkedList<>());
        ackedMessages.put(gsock, new LinkedList<>());
        nameGsockMap.put(groupname, gsock);
        udpMutex.unlock();

        gsockNameMap.put(gsock, groupname);
        return gsock;
    }

    public boolean LeaveGroup(Integer gsock){
        if(!gsockNameMap.containsKey(gsock)) {
            return false;
        }


        String request = myGroupieId + "l" + gsockNameMap.get(gsock);

        SendGMS(request);

        boolean wait = true;
        while(wait){
            tcpMutex.lock();
            if(!groupViewMap.containsKey(gsockNameMap.get(gsock))){
                wait = false;
            }
            tcpMutex.unlock();
        }

        udpMutex.lock();
        sendingBuffer.remove(gsock);
        messageBuffer.remove(gsock);
        ackedMessages.remove(gsock);
        nameGsockMap.remove(gsockNameMap.get(gsock));
        udpMutex.unlock();

        gsockNameMap.remove(gsock);
        return true;
    }

    public boolean SendGroup(Integer gsock, String message) {
        if(!gsockNameMap.containsKey(gsock)) {
            return false;
        }

        tcpMutex.lock();
        GroupView currGroup = groupViewMap.get(gsockNameMap.get(gsock));
        LinkedList<SocketAddress> groupSockets = new LinkedList<>(currGroup.GetUdps());

        int mySeqNo = groupSeqNumbers.get(currGroup.groupName);
        mySeqNo++;
        groupSeqNumbers.replace(currGroup.groupName, mySeqNo);

        int myTotalSeqNo = 0;
        if(withTotal){
            if(myCoordSeqs.containsKey(currGroup.groupName)) {
                myTotalSeqNo = myCoordSeqs.get(currGroup.groupName);
                myTotalSeqNo++;
                myCoordSeqs.replace(currGroup.groupName,myTotalSeqNo);
            }
        }

        Message newMsg = new Message(myGroupieId, mySeqNo, message);
        if(withTotal)
            if(myCoordSeqs.containsKey(currGroup.groupName))
                newMsg.SetTotalSeqNo(myTotalSeqNo);
        tcpMutex.unlock();


        udpMutex.lock();
        sendingBuffer.get(gsock).add(newMsg);
        udpMutex.unlock();

        for(SocketAddress socket : groupSockets){
            if(withTotal) {
                SendTotalPacket(gsock, myGroupieId, mySeqNo, message, myTotalSeqNo, socket);
            }else {
                SendPacket(gsock, myGroupieId, mySeqNo, message, socket);
            }
        }

        return true;
    }

    public Object ReceiveGroup(Integer gsock){
        if(!gsockNameMap.containsKey(gsock)) {
            return null;
        }

        appMutex.lock();
        LinkedList<Object> forDelivery = toBeDelivered.get(gsockNameMap.get(gsock));
        appMutex.unlock();

        if(forDelivery == null)
            return null;

        Object notification = forDelivery.pollFirst();

        if(notification == null)
            return 1;

        return notification;
    }

    //close sockets, wait tcpListener thread
    public void Terminate(){
        if(udpSocket != null) {
            udpSocket.close();
            try {
                if(tcpSocket!=null) {
                    tcpSocket.close();
                }
            } catch (IOException e) {
                System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
            }
        }
        if(myTcpListener != null) {
            try {
                myTcpListener.join(2000);
            } catch (InterruptedException e) {
                System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
            }
        }
    }

    public void PrintTcpSocket(){
        if(tcpSocket != null) {
//            System.out.println(tcpSocket.getPort());
//            System.out.println(tcpSocket.getRemoteSocketAddress());
//            System.out.println("my tcp host address " + tcpSocket.getInetAddress().getHostAddress());
//            System.out.println("my tcp port " + tcpSocket.getLocalPort());
            System.out.println("my tcp " + tcpSocket.getLocalSocketAddress());
//            System.out.println(tcpSocket.getLocalSocketAddress());
        }
        else
            System.out.println("tcpSocket not established");
    }

    //Find new server to begin communication
    //get client id
    //create tcp socket on server socket
    private void Multicast(String multicastMessage) throws IOException {
        byte[] payloadBytes = multicastMessage.getBytes();

        int initialPort = 10000;
        DatagramPacket packet = new DatagramPacket(payloadBytes, payloadBytes.length, group, initialPort);
        byte[] rcvPacketBytes = new byte[30];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);

        for(int i = 0; i<5; i++){
            udpSocket.setSoTimeout((i+1)*1000); //increase listening endurance
            try {
                udpSocket.send(packet);
                udpSocket.receive(rcv_packet);
                break;
            } catch (IOException e) { //multicast not received
                System.err.println("Multicast delay");
            }
            if(i==4) { //no server available, reset timeout
//                udpSocket.setSoTimeout(200);
                throw new IOException("Multicast failed");
            }
        }

        udpSocket.setSoTimeout(50);

        String received = new String(rcv_packet.getData(), 0, rcv_packet.getLength());


        int addrLen = Byte.toUnsignedInt(rcv_packet.getData()[0]);
        int portLen = Byte.toUnsignedInt(rcv_packet.getData()[addrLen+1]);
        String addr = new String(rcv_packet.getData(), 1, addrLen);
        String port = new String(rcv_packet.getData(), addrLen+2, portLen);

        myGroupieId = Byte.toUnsignedInt(rcv_packet.getData()[addrLen+portLen+2]);
        System.out.println("my id " + myGroupieId);

        //get communication identifier
        /*multicastSocket = rcv_packet.getSocketAddress();
        System.out.println(rcv_packet.getSocketAddress());*/

        tcpSocket = new Socket(addr, Integer.parseInt(port));
        System.out.println("my tcp " + tcpSocket.getLocalSocketAddress());
        toGMS = new ObjectOutputStream(tcpSocket.getOutputStream());
        myTcpListener.ConfigureSocket();
    }

    //Group Management Service send
    private void SendGMS(String tcpMsg){
        try {
            toGMS.writeObject(tcpMsg);
            toGMS.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Send acknowledgment for existing message
    //packet = isAck,gsock,groupieId,seqNo
    private void SendAck(int gsock, int groupieId, int seqNo, SocketAddress groupieSocket){
        //1 is ACK
        byte[] isAck = ByteBuffer.allocate(4).putInt(1).array();

        String payload = gsock+","+groupieId+","+seqNo;
        byte[] payloadBytes = payload.getBytes();

        byte[] packetBytes = new byte[payloadBytes.length + 1];

        //Construct packet
        System.arraycopy(isAck, 3, packetBytes, 0, 1);
        System.arraycopy(payloadBytes, 0, packetBytes, 1, payloadBytes.length);

        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, groupieSocket);

        try {
            udpSocket.send(packet);
            UDPsTotal++;
        } catch (IOException e) {
            System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
        }
    }
    //packet = isAck,gsock,groupieId,seqNo,totalSeqNo
    private void SendTotalAck(int gsock, int groupieId, int seqNo, int totalSeqNo, SocketAddress groupieSocket){
        //1 is ACK
        byte[] isAck = ByteBuffer.allocate(4).putInt(1).array();

        String payload = gsock+","+groupieId+","+seqNo+ "," + totalSeqNo;

        byte[] payloadBytes = payload.getBytes();

        byte[] packetBytes = new byte[payloadBytes.length + 1];

        //Construct packet
        System.arraycopy(isAck, 3, packetBytes, 0, 1);
        System.arraycopy(payloadBytes, 0, packetBytes, 1, payloadBytes.length);

        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, groupieSocket);

        try {
            udpSocket.send(packet);
            UDPsTotal++;
        } catch (IOException e) {
            System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
        }
    }

    //packet = noAck,gsock,groupieId,seqNo,msg
    private void SendPacket(int gsock, int groupieId, int seqNo, String msg, SocketAddress groupieSocket){
        //0 is not ACK
        byte[] isAck = ByteBuffer.allocate(4).putInt(0).array();

        String payload = gsock+","+groupieId+","+seqNo+","+ msg;
        byte[] payloadBytes = payload.getBytes();
        byte[] packetBytes = new byte[payloadBytes.length + 1];

        //Construct packet
        System.arraycopy(isAck, 3, packetBytes, 0, 1);
        System.arraycopy(payloadBytes, 0, packetBytes, 1, payloadBytes.length);

        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, groupieSocket);

        try {
            udpSocket.send(packet);
            UDPsTotal++;
        } catch (IOException e) {
            System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
        }
    }
    //packet = noAck,gsock,groupieId,seqNo,msg,totalSeqNo
    private void SendTotalPacket(int gsock, int groupieId, int seqNo, String msg, int totalSeqNo, SocketAddress groupieSocket){
        //0 is not ACK
        byte[] isAck = ByteBuffer.allocate(4).putInt(0).array();

        String payload = payload = gsock + "," + groupieId + "," + seqNo + "," + msg + "," + totalSeqNo;

        byte[] payloadBytes = payload.getBytes();
        byte[] packetBytes = new byte[payloadBytes.length + 1];

        //Construct packet
        System.arraycopy(isAck, 3, packetBytes, 0, 1);
        System.arraycopy(payloadBytes, 0, packetBytes, 1, payloadBytes.length);

        DatagramPacket packet = new DatagramPacket(packetBytes, packetBytes.length, groupieSocket);

        try {
            udpSocket.send(packet);
            UDPsTotal++;
        } catch (IOException e) {
            System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
        }
    }

    //Receiving new messages, ACKS and replying to ACKS
    //adding to acks to existing messages
    //adding new messages to buffer
    //common packet part = isAck,gsock,groupieId,seqNo
    private void ReceivePacket(){
        byte[] rcvPacketBytes = new byte[50];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);
        try {
            udpSocket.receive(rcv_packet);

            boolean isAck = (Byte.toUnsignedInt(rcv_packet.getData()[0]) == 1);

            int nonNullBytes = rcv_packet.getData().length;
            while (nonNullBytes-- > 0 && rcv_packet.getData()[nonNullBytes] == 0) {}

            String payload = new String(rcv_packet.getData(), 1, nonNullBytes);
            String[] partedPayload = payload.split(",");

            Integer gsock = Integer.valueOf(partedPayload[0]);
            Integer clientId = Integer.valueOf(partedPayload[1]);
            /*System.out.println("DEBUG seqNo = " + partedPayload[2]);
            for (int i = 0; i < partedPayload[2].getBytes().length; i++){
                System.out.println("DEBUG byte seqNo["+i+"] = " + Byte.toUnsignedInt(partedPayload[2].getBytes()[i]));
            }*/
            Integer clientSeqNo = Integer.valueOf(partedPayload[2]);

            LinkedList<Message> rcvMsgList = null;
            SocketAddress udpId = rcv_packet.getSocketAddress();

            udpMutex.lock();
            if(messageBuffer.containsKey(gsock)) {
                rcvMsgList = messageBuffer.get(gsock);
            }
            udpMutex.unlock();

            if(rcvMsgList != null) {
                Message existingMessage = null;

                for (Message msg : rcvMsgList) {
                    if (msg.HasIdentifier(clientId, clientSeqNo)) {
                        existingMessage = msg;
                    }
                }

                //RECEIVED ACK
                if (isAck) {
                    //MESSAGE EXISTS
                    if (existingMessage != null) {
                        //IF HAS NOT RECEIVED ACK
                        if (!existingMessage.ContainsAck(udpId)) {
                            existingMessage.AddAck(udpId);
                        }
                    } else {
                        System.out.println("Received ACK " + clientId + "." + clientSeqNo + " for gsock " + gsock + " from " + udpId + " while msg does not exist");
                    }
                }
                //RECEIVED MESSAGE
                else {
                    String message = partedPayload[3];

                    if (existingMessage == null) {
                        Message newMsg = new Message(clientId, clientSeqNo, message);
                        rcvMsgList.add(newMsg);
                    }
                    SendAck(gsock, clientId, clientSeqNo, udpId);
                }
            }
            else{
                System.out.println("Received"+clientId+"."+clientSeqNo+ " for gsock " + gsock+ " from " + udpId +" while not in group");
            }
        } catch (IOException ignored) {
//            ignored.printStackTrace();
        }
    }
    //common packet part = isAck,gsock,groupieId,seqNo,totalSeqNo
    private void ReceiveTotalPacket(){
        byte[] rcvPacketBytes = new byte[50];
        DatagramPacket rcv_packet = new DatagramPacket(rcvPacketBytes, rcvPacketBytes.length);
        try {
            udpSocket.receive(rcv_packet);

            boolean isAck = (Byte.toUnsignedInt(rcv_packet.getData()[0]) == 1);

            int nonNullBytes = rcv_packet.getData().length;
            while (nonNullBytes-- > 0 && rcv_packet.getData()[nonNullBytes] == 0) {}

            String payload = new String(rcv_packet.getData(), 1, nonNullBytes);
            String[] partedPayload = payload.split(",");

            Integer gsock = Integer.valueOf(partedPayload[0]);
            Integer clientId = Integer.valueOf(partedPayload[1]);
            Integer clientSeqNo = Integer.valueOf(partedPayload[2]);
            Integer totalSeqNo = Integer.valueOf(partedPayload[partedPayload.length -1]);

            SocketAddress udpId = rcv_packet.getSocketAddress();

            LinkedList<Message> rcvMsgList = null;

            udpMutex.lock();
            if(messageBuffer.containsKey(gsock)) {
                rcvMsgList = messageBuffer.get(gsock);
            }
            udpMutex.unlock();

            if(rcvMsgList != null) {
                Message existingMessage = null;

                for (Message msg : rcvMsgList) {
                    if (msg.HasIdentifier(clientId, clientSeqNo)) {
                        existingMessage = msg;
                    }
                }

                //RECEIVED ACK
                if (isAck) {
                    //MESSAGE EXISTS
                    if (existingMessage != null) {
                        if(totalSeqNo!=0){
                            if(existingMessage.GetTotalSeqNo() != totalSeqNo)
                                existingMessage.SetTotalSeqNo(totalSeqNo);
                            if(!existingMessage.ContainsAckTotal(udpId)){
                                existingMessage.AddAckTotal(udpId);
                            }
                        }
                    } else {
                        System.out.println("Received ACK " + clientId + "." + clientSeqNo + " for gsock " + gsock + " from " + udpId + " while msg does not exist");
                    }
                }
                //RECEIVED MESSAGE
                else {
                    String message = partedPayload[3];

                    //IF NEW MESSAGE
                    if (existingMessage == null) {
                        Message newMsg = new Message(clientId, clientSeqNo, message);
                        //IF IT IS SEQUENCED
                        if(totalSeqNo!=0) {
                            newMsg.SetTotalSeqNo(totalSeqNo);
                        }
                        //IF NOT SEQUENCED
                        else{
                            tcpMutex.lock();
                            String groupName = gsockNameMap.get(gsock);
                            int myTotalSeqNo = 0;
                            //CHECK IF THIS IS APP IS COORDINATOR
                            if(myCoordSeqs.containsKey(groupName)) {
                                myTotalSeqNo = myCoordSeqs.get(groupName);
                                myTotalSeqNo++;
                                myCoordSeqs.replace(groupName,myTotalSeqNo);
                            }
                            tcpMutex.unlock();
                            //IF THIS IS COORD
                            if(myTotalSeqNo!=0) {
                                newMsg.SetTotalSeqNo(myTotalSeqNo);
                            }
                        }
                        rcvMsgList.add(newMsg);
                    }
                    //IF MESSAGE EXISTS
                    else{
                        //IF RECEIVED SEQUENCED
                        if(totalSeqNo!=0) {
                            if (existingMessage.GetTotalSeqNo() == 0) {
                                existingMessage.SetTotalSeqNo(totalSeqNo);
                            }
                            SendTotalAck(gsock, clientId, clientSeqNo, totalSeqNo, udpId);
                        }
                        //IF RECEIVED UNSEQUENCED
                        else{
                            if (existingMessage.GetTotalSeqNo() != 0) {
                                totalSeqNo = existingMessage.GetTotalSeqNo();
                                SendTotalAck(gsock, clientId, clientSeqNo, totalSeqNo, udpId);
                            }
                        }
                    }
                }
            }
            else{
                System.out.println("Received"+clientId+"."+clientSeqNo+ " for gsock " + gsock+ " from " + udpId +" while not in group");
            }
        } catch (IOException ignored) {
//            ignored.printStackTrace();
        }
    }

    //FOR UDP LISTENING
    public void run(){
        int minimumReceives = 5;
        int receiveCounter = minimumReceives;
        while (Application.appRun){
            tcpMutex.lock();
            LinkedList<GroupView> myGroupViews = new LinkedList<>(groupViewMap.values());
            tcpMutex.unlock();

            for(GroupView groupView : myGroupViews){
                LinkedList<SocketAddress> groupSockets = new LinkedList<>(groupView.GetUdps());

                udpMutex.lock();
                Integer gsock = nameGsockMap.get(groupView.groupName);
                LinkedList<Message> sendingMessages = sendingBuffer.get(gsock);

                if(sendingMessages != null) {
                    for (Message msg : sendingMessages) {
                        for (SocketAddress socket : groupSockets) {
                            if(withTotal) {
                                SendTotalPacket(gsock, msg.GetClientId(), msg.GetSeqNo(), msg.GetMessage(), msg.GetTotalSeqNo(), socket);
                            }else {
                                SendPacket(gsock, msg.GetClientId(), msg.GetSeqNo(), msg.GetMessage(), socket);
                            }
                        }
                    }
                }
                udpMutex.unlock();

                for (int i = 0; i < receiveCounter; i++) {
                    if(withTotal){
                        ReceiveTotalPacket();
                    }else {
                        ReceivePacket();
                    }
                }

                udpMutex.lock();
                LinkedList<Message> groupMessages = messageBuffer.get(gsock);
                udpMutex.unlock();

                LinkedList<SocketAddress> udpAddresses = new LinkedList<>(groupView.GetUdps());
                if(groupMessages != null) {
                    for (Message msg : groupMessages) {
                        //CHECK FOR ACKS
                        if (!msg.IsAppReady()) {
                            int collectedAcks = 0;
                            for (SocketAddress udpId : udpAddresses) {
                                if(withTotal){
                                    if(msg.ContainsAckTotal(udpId)){
                                        collectedAcks++;
                                    }
                                    else{
                                        SendTotalPacket(gsock, msg.GetClientId(), msg.GetSeqNo(), msg.GetMessage(), msg.GetTotalSeqNo(), udpId);
                                    }
                                }else {
                                    if (msg.ContainsAck(udpId)) {
                                        collectedAcks++;
                                    }
                                    else {
                                        SendPacket(gsock, msg.GetClientId(), msg.GetSeqNo(), msg.GetMessage(), udpId);
                                    }
                                }
                            }

                            if (collectedAcks == udpAddresses.size()) {
                                msg.AppReady();
                                udpMutex.lock();
                                ackedMessages.get(gsock).add(msg);
                                udpMutex.unlock();
                            }
                        }
                    }
                }

                udpMutex.lock();
                LinkedList<Message> appGroupMessages = ackedMessages.get(gsock);
                udpMutex.unlock();

                appMutex.lock();
                LinkedList<Object> groupDelivered = deliveredBuffer.get(groupView.groupName);
                LinkedList<Object> forDelivery = toBeDelivered.get(groupView.groupName);
                appMutex.unlock();

                ArrayList<Message> matched = new ArrayList<>();
                if(appGroupMessages != null) {
                    for (Message aMsg : appGroupMessages) {
                        if(withTotal) {
                            if (aMsg.GetTotalSeqNo() == 1) {
                                matched.add(aMsg);
                                continue;
                            }
                        }
                        else {
                            if (aMsg.GetSeqNo() == 1) {
                                matched.add(aMsg);
                                continue;
                            }
                        }
                        for (Object object : groupDelivered) {
                            if (object.getClass() == Message.class) {
                                Message dMsg = (Message) object;
                                if(withTotal){
                                    if (aMsg.IsNextTotal(dMsg)) {
                                        matched.add(aMsg);
                                        break;
                                    }
                                }
                                else {
                                    if (aMsg.IsNext(dMsg)) {
                                        matched.add(aMsg);
                                        break;
                                    }
                                }
                            }
                        }
                    }

                    for (Message msg : matched) {
                        appGroupMessages.remove(msg);
                        groupDelivered.add(msg);
                        forDelivery.add(msg);
                    }
                }

                ArrayList<Message> removeSent = new ArrayList<>();
                udpMutex.lock();
                if(sendingMessages != null) {
                    for (Message sent : sendingMessages) {
                        for (Message msg : matched) {
                            if (msg.IsSame(sent))
                                removeSent.add(sent);
                        }
                    }

                    for (Message msg : removeSent) {
                        sendingMessages.remove(msg);
                    }
                }
                udpMutex.unlock();
            }
        }
        System.out.println("Total UDP/IP packets sent " + UDPsTotal);
    }

    //EXTERNAL THREAD FOR GMS LISTENING
    private class TcpListener extends Thread{
        private ObjectInputStream fromGMS;

        public void run(){
            while (Application.appRun) {
                String type = null;
                try {
                    type = (String) fromGMS.readObject();

                    //updated groupView
                    //add groupView to app delivery
                    if(type.equals("groupView")) {
                        GroupView groupView = (GroupView) fromGMS.readObject();

                        tcpMutex.lock();
                        groupViewMap.replace(groupView.groupName, groupView);
                        tcpMutex.unlock();

                        appMutex.lock();
                        LinkedList<Object> objectList = deliveredBuffer.get(groupView.groupName);
                        LinkedList<Object> forDelivery = toBeDelivered.get(groupView.groupName);
                        //MAYBE NEW OBJECT GROUP VIEW
                        objectList.add(groupView);
                        forDelivery.add(groupView);
                        appMutex.unlock();
                    }
                    //add new groupView
                    //add groupView to app delivery
                    else if(type.equals("join")){
                        GroupView groupView = (GroupView) fromGMS.readObject();

                        tcpMutex.lock();
                        groupViewMap.put(groupView.groupName, groupView);
                        groupSeqNumbers.put(groupView.groupName, 0);
                        if(groupView.GetCoordinator() == myGroupieId)
                            myCoordSeqs.put(groupView.groupName, 0);
                        tcpMutex.unlock();

                        appMutex.lock();
                        deliveredBuffer.computeIfAbsent(groupView.groupName, k -> new LinkedList<>());
                        toBeDelivered.computeIfAbsent(groupView.groupName, k -> new LinkedList<>());
                        LinkedList<Object> objectList = deliveredBuffer.get(groupView.groupName);
                        LinkedList<Object> forDelivery = toBeDelivered.get(groupView.groupName);
                        //MAYBE NEW OBJECT GROUP VIEW
                        objectList.add(groupView);
                        forDelivery.add(groupView);
                        appMutex.unlock();
                    }
                    //remove old groupView
                    else if(type.equals("leave")){
                        GroupView groupView = (GroupView) fromGMS.readObject();

                        tcpMutex.lock();
                        groupViewMap.remove(groupView.groupName);
                        groupSeqNumbers.remove(groupView.groupName);
                        tcpMutex.unlock();
                    }
                    else{  //FOR ERROR MESSAGES
                        //to flush buffer
                        GroupView groupView = (GroupView) fromGMS.readObject();
                        System.out.println("wrong type of action");
                        System.out.println("GroupView msg " + groupView.GetViewMsg());
                    }
                }
                //if GMS died
                catch (EOFException e){
                    try {
                        toGMS.close();
                        fromGMS.close();
                        tcpSocket.close();
                        Multicast(multicastMessage);
                    } catch (IOException ex) {
                        if (ex.getMessage().equals("Multicast failed")) {
                            System.err.println("Continue with group communication");
                            return;
                        }
                        else{
                            System.err.println(ex.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
                        }
                    }
                }
                catch (IOException e) {
                    if (e.getMessage().equals("Multicast failed")) {
                        System.err.println("Continue with group communication");
                        return;
                    }
                    else{
//                        System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
//            System.err.println("TcpListener terminated!");
        }

        public void ConfigureSocket(){
            try {
                fromGMS = new ObjectInputStream(tcpSocket.getInputStream());
            } catch (IOException e) {
//                e.printStackTrace();
                System.err.println(e.getMessage() + " " + new Throwable().getStackTrace()[0].getLineNumber());
            }
        }
    }

    //CUSTOM CLASS FOR MESSAGE BUFFERS
    public class Message{
        private int clientId;
        private int seqNo;
        private String message;
        private int totalSeqNo = 0;

        //clients that have received same message
        private List<SocketAddress> acksUdp;
        //clients that have received same message withTotal
        private List<SocketAddress> acksTotal;

        //flag for delivering to application
        private boolean inAppBuffer = false;

        public Message(int clientId, int seqNo, String message) {
            this.clientId = clientId;
            this.seqNo = seqNo;
            this.message = message;

            acksUdp = new LinkedList<>();
            acksTotal = new LinkedList<>();
        }

        public int GetClientId() {
            return clientId;
        }

        public int GetSeqNo() {
            return seqNo;
        }

        public String GetMessage() {
            return message;
        }

        public int GetTotalSeqNo(){ return totalSeqNo;}

        public void SetTotalSeqNo(Integer totalSeqNo){ this.totalSeqNo = totalSeqNo;}

        public boolean HasIdentifier(int clientId, int seqNo){
            return this.clientId == clientId && this.seqNo == seqNo;
        }

        public String GetIdentifier(){
            return clientId+"."+seqNo;
        }

        public void AddAck(SocketAddress udpId){
            acksUdp.add(udpId);
        }

        public void AddAckTotal(SocketAddress udpId){
            acksTotal.add(udpId);
        }

        public boolean ContainsAck(SocketAddress udpId){
            return acksUdp.contains(udpId);
        }

        public boolean ContainsAckTotal(SocketAddress udpId){
            return acksTotal.contains(udpId);
        }

        public void AppReady(){
            inAppBuffer = true;
        }

        public boolean IsAppReady(){
            return inAppBuffer;
        }

        public boolean IsNext(Message checkMsg){
            if(clientId == checkMsg.GetClientId()){
                if(seqNo == checkMsg.GetSeqNo() + 1)
                    return true;
            }
            return false;
        }

        public boolean IsNextTotal(Message checkMsg){
            return totalSeqNo == checkMsg.GetTotalSeqNo() + 1;
        }

        public boolean IsSame(Message checkMsg){
            return clientId == checkMsg.GetClientId() && seqNo == checkMsg.GetSeqNo() && message.equals(checkMsg.GetMessage());
        }
    }
}