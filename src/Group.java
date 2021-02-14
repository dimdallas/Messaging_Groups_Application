/*Custom class for group information
* only to be used by GMS */

import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;

public class Group {
    private String groupName;

    //client id - tcp socket
    private Map<Integer, Socket> tcpMap;
    //client id - udp address
    private Map<Integer, SocketAddress> udpMap;

    // coordinator client id
    Integer coordinator;

    //CONSTRUCTOR
    public Group(String groupName, Integer coordinator) {
        this.groupName = groupName;
        this.coordinator = coordinator;

        tcpMap = new LinkedHashMap<>();
        udpMap = new LinkedHashMap<>();
    }

    public boolean IsCoordinator(Integer clientId){
        return coordinator.equals(clientId);
    }

    public Integer GetCoordinator() {
        return coordinator;
    }

    public void SetCoordinator(){
        LinkedList<Integer> ids = new LinkedList<>(tcpMap.keySet());
        coordinator = ids.getFirst();
    }

    public String GetGroupName() {
        return groupName;
    }

    public Map<Integer, Socket> GetTcpMap() {
        return tcpMap;
    }

    public Map<Integer, SocketAddress> GetUdpMap() {
        return udpMap;
    }

    public int GetGroupSize(){
        return tcpMap.size();
    }

    public boolean ContainsClient(Integer clientId){
        return tcpMap.containsKey(clientId);
    }

    public boolean ContainsClient(Socket clientSocket){
        return tcpMap.containsValue(clientSocket);
    }

    public boolean IsEmpty(){
        return tcpMap.isEmpty();
    }

    public void AddGroupie(Integer groupieId, Socket tcpSocket, SocketAddress udpSocket){
        tcpMap.put(groupieId, tcpSocket);
        udpMap.put(groupieId, udpSocket);
//        System.out.println("Group " +  groupName + " added " + groupieId + " tcp " + tcpSocket.getRemoteSocketAddress() + " udp " + udpSocket);
    }

    public void RemoveGroupie(Integer groupieId){
        tcpMap.remove(groupieId);
        udpMap.remove(groupieId);
    }

    public Integer UserIdFromTcp(Socket userTcp){
        return tcpMap.entrySet().stream()
                .filter(entry -> userTcp.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .findFirst().get();
    }

    public Integer UserIdFromUdp(SocketAddress userUdp){
        return udpMap.entrySet().stream()
                .filter(entry -> userUdp.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .findFirst().get();
    }

    public SocketAddress UserUdpFromTcp(Socket userTcp){
        return null;
    }

    public Socket UserTcpFromUdp(SocketAddress userUdp){
        return null;
    }

    public Integer UserIdFromIndex(int index){
        return (Integer)udpMap.keySet().toArray()[index];
    }

    public SocketAddress UserUdpFromId(Integer id){
        return udpMap.get(id);
    }
}
