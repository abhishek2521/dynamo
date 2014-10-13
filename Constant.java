package edu.buffalo.cse.cse486586.simpledynamo;

public class Constants {
    
    //Port Details
    public static final String REMOTE_PORT0 = "11108";
    public static final String REMOTE_PORT1 = "11112";
    public static final String REMOTE_PORT2 = "11116";
    public static final String REMOTE_PORT3 = "11120";
    public static final String REMOTE_PORT4 = "11124";
    public static final int    SERVER_PORT  = 10000;
    
    //Database Details
    public static final String TABLE_DYNAMO     = "dynamo";
    public static final String COLUMN_KEY       = "key";
    public static final String COLUMN_VALUE     = "value";
    public static final String DATABASE_NAME    = "DynamoDatabase.db";
    public static final String DATABASE_CREATE  = "create table " + Constants.TABLE_DYNAMO  + "(" + Constants.COLUMN_KEY + " text not null, " + Constants.COLUMN_VALUE + " text not null" + ");";
    public static final int    DATABASE_VERSION = 1;
    public static final String DB_PATH          = "/data/data/edu.buffalo.cse.cse486586.simpledynamo/databases/";
    
    //TAG
    public static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    
    //Protocol Details
    public static final String INSERT_PROTOCOL    = "INSERTPROTOCOL";
    public static final String PIPE_DELIMITER     = "|";
    public static final String DELETE_PROTOCOL    = "DELETEPROTOCOL";
    public static final String DELETE_RESPONSE    = "DELETERESPONSE";
    public static final String QUERY_PROTOCOL     = "QUERYPROTOCOL";
    public static final String QUERY_RESPONSE     = "QUERYRESPONSE";
    public static final String RECOVERY_PROTOCOL  = "RECOVERYPROTOCOL";
    public static final String RECOVERY_RESPONSE  = "RECOVERYRESPONSE";
    
    //Timeout
    public static final int TIMEOUT= 4000;
    public static final int WAIT_TIMEOUT = 500;
    public static final int QUERY_TIMEOUT = 800;
    
}
