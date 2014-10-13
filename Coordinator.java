package edu.buffalo.cse.cse486586.simpledynamo;

public class Coordinator {
    
    String mDestinationPort;
    String mFirstReplicaPort;
    String mSecondReplicaPort;             
    
    
    Coordinator(String destinationPort,String firstReplicaPort,String secondReplicaPort){
        
        this.mDestinationPort=destinationPort;
        this.mFirstReplicaPort=firstReplicaPort;
        this.mSecondReplicaPort=secondReplicaPort;
    }     
    
    public String getmDestinationPort() {
        return mDestinationPort;
    }

    public void setmDestinationPort(String mDestinationPort) {
        this.mDestinationPort = mDestinationPort;
    }

    public String getmFirstReplicaPort() {
        return mFirstReplicaPort;
    }

    public void setmFirstReplicaPort(String mFirstReplicaPort) {
        this.mFirstReplicaPort = mFirstReplicaPort;
    }

    public String getmSecondReplicaPort() {
        return mSecondReplicaPort;
    }

    public void setmSecondReplicaPort(String mSecondReplicaPort) {
        this.mSecondReplicaPort = mSecondReplicaPort;
    }

    
    }
