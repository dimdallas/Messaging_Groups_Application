/*Custom class for users to know basic information of a group*/

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Set;

public class GroupView implements Serializable {
    String groupName;

    //clientId - udp socket
    LinkedHashMap<Integer, SocketAddress> udpMap;

    //group view update message
    String groupViewMsg;

    //coordinator client id
    Integer coordinator;

    //CONSTRUCTOR
    public GroupView(Group group, String groupViewMsg){
        groupName = group.GetGroupName();
        this.udpMap = new LinkedHashMap<>(group.GetUdpMap());

        this.groupViewMsg = groupViewMsg;
        this.coordinator = group.GetCoordinator();
    }

    //METHODS
    public Collection<SocketAddress> GetUdps(){
        return udpMap.values();
    }

    public Set<Integer> GetClientIds(){
        return udpMap.keySet();
    }

    public String GetViewMsg(){
        return groupViewMsg;
    }

    public Integer GetCoordinator() {
        return coordinator;
    }
}
