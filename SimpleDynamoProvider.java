
package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class SimpleDynamoProvider extends ContentProvider {

    // Global Variables

    // Genhashes of all the co-ordinators
    static String sGenHash5554;

    static String sGenHash5556;

    static String sGenHash5558;

    static String sGenHash5560;

    static String sGenHash5562;

    // Emulator Details
    static TelephonyManager sTel;

    static String sPortStr;

    static String sMyPort;

    static String sMyID;

    // Database Variables
    static DynamoDatabaseHelper sDbHelper;

    static SQLiteDatabase sqlDB;

    static SQLiteQueryBuilder sQueryBuilder;

    static Uri sUri;

    // Delete Function variables
    static int sGlobalRowsDeleted;

    static int sTotalDeleteResponse;

    static boolean sIsDeleteResponseAwaited;

    static String sDeleteKey;

    static boolean sIsDeleteInProgress;

    // Query Function Variables
    static Cursor sGlobalCursor;

    static HashMap<String, String> sGlobalMap;

    static int sTotalQueryResponse;

    static AtomicBoolean sIsQueryResponseAwaited;

    static String sQueryKey;

    static AtomicBoolean sIsQueryInProgress;

    // Recovery Function Variables
    static int sTotalRecoveryResponse;

    static boolean sIsRecoveryInProgress;

    static String sRecoverymessage;

    // static HashMap<String, String> recoveryMap;

    @Override
    public boolean onCreate() {

        // Step 1 : Define local Variables
        String msg;
        SQLiteDatabase existingDBReference;
        boolean isDatabaseExisting = false;

        // Step 2 : Check if database already exists
        try {
            existingDBReference = SQLiteDatabase.openDatabase(Constants.DB_PATH
                    + Constants.DATABASE_NAME, null, SQLiteDatabase.OPEN_READONLY);
            existingDBReference.close();
            isDatabaseExisting = true;
        } catch (SQLiteException e) {
            Log.e(Constants.TAG, e.toString()
                    + "Database Doesnot exist. Application is being started for the first time \n");
            Log.e(Constants.TAG, getContext().getFilesDir().getPath() + "\n");
            isDatabaseExisting = false;
        }

        // Step 3 : Get the databaseHelper instance
        sDbHelper = new DynamoDatabaseHelper(getContext());
        sqlDB = sDbHelper.getWritableDatabase();
        sQueryBuilder = new SQLiteQueryBuilder();
        sUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

        // Step 4 : Initialize Delete Function Variables
        sIsDeleteResponseAwaited = false;
        sIsDeleteInProgress = false;
        sGlobalRowsDeleted = 0;
        sTotalDeleteResponse = 0;

        // Step 5 : Initialize and Instantiate query function variables
        sIsQueryResponseAwaited = new AtomicBoolean();
        sIsQueryInProgress = new AtomicBoolean();
        sTotalQueryResponse = 0;
        sGlobalCursor = null;
        sGlobalMap = new HashMap<String, String>();

        // Step 6 : Initialize Recovery Variables
        sTotalRecoveryResponse = 0;
        sIsRecoveryInProgress = false;
        // recoveryMap = new HashMap<String, String>();

        // Step 6 :Get my port
        sTel = (TelephonyManager)this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        sPortStr = sTel.getLine1Number().substring(sTel.getLine1Number().length() - 4);
        sMyPort = String.valueOf((Integer.parseInt(sPortStr) * 2));
        Log.e(Constants.TAG, "My Port:" + sMyPort + "\n");

        // Step 7 : Get the hash value of my port
        try {
            sMyID = genHash(sPortStr);
            Log.e(Constants.TAG, "My ID:" + sMyID + "\n");
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to get genhash of self port \n");
        }

        // Step 8 : Get the hash Value of all the coordinator
        getGenhashCoordinator();

        // Step 9 : Empty The database so that if there were any deletes when
        // the node was down , it will be taken care of
        localDelete(sUri, "@", null);

        // Step 10 : Invoke the server Task
        try {
            ServerSocket serverSocket = new ServerSocket(Constants.SERVER_PORT);
            SimpleDynamoActivity.mServerTaskReference = new ServerTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(Constants.TAG, e.toString() + "  Unable to create server socket\n");
        }

        // Step 11 : If Database Exists , send recovery message to all the four
        // nodes and wait for their responses
        if (isDatabaseExisting) {

            // Step 12 : Set isRecoveryInProgress to True
            // isRecoveryInProgress = true;

            // Step 13 : Send Recovery Request to all the remaining nodes one by
            // one and wait for their response
            if (!sMyPort.equals(Constants.REMOTE_PORT0)) {
                msg = Constants.RECOVERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT0
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Recovery Request" + " is being sent to "
                        + Constants.REMOTE_PORT0 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT1)) {
                msg = Constants.RECOVERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT1
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Recovery Request" + " is being sent to "
                        + Constants.REMOTE_PORT1 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT2)) {
                msg = Constants.RECOVERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT2
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Recovery Request" + " is being sent to "
                        + Constants.REMOTE_PORT2 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT3)) {
                msg = Constants.RECOVERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT3
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Recovery Request" + " is being sent to "
                        + Constants.REMOTE_PORT3 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT4)) {
                msg = Constants.RECOVERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT4
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Recovery Request" + " is being sent to "
                        + Constants.REMOTE_PORT4 + "\n");
                new ClientThread(msg);
            }

            // Step 14 : Wait for 4 seconds for responses to arrive

            /*
             * try { Thread.sleep(Constants.TIMEOUT); } catch (Exception e) {
             * Log.e(Constants.TAG, "Unable to sleep the thread \n"); }
             */

            // Step 15 : ADD the data from Recovery Map to database
            /*
             * synchronized (recoveryMap) { insertMapIntoDatabase(recoveryMap);
             * recoveryMap.clear(); }
             */

            // Step 15 : Wait for atleast 3 Responses
            // Log.e(Constants.TAG, "In Recovery Phase : isRecoveryInProgress: "
            // + isRecoveryInProgress + " totalRecoveryResponse " +
            // totalRecoveryResponse + "\n") ;

            // while (totalRecoveryResponse < 3)
            // ;

            // Step 16 : Set isRecoveryInProgress to False
            // isRecoveryInProgress = false;
            // totalRecoveryResponse=0;
        }

        return false;
    }

    /*
     * public class ProcessRecoveryResponseThread implements Runnable { Thread
     * t; String msgRead; ProcessRecoveryResponseThread(String msgRead) { t =
     * new Thread(this); this.msgRead = msgRead; t.start(); } public void run()
     * { int i; String msgReadArray[] = msgRead.split("\\|"); ContentValues
     * contentValues = new ContentValues(); try { for (i = 3; i <
     * msgReadArray.length; i += 2) { if (isKeyPartOfPartition(msgReadArray[i]))
     * { contentValues.put(Constants.COLUMN_KEY, msgReadArray[i]);
     * contentValues.put(Constants.COLUMN_VALUE, msgReadArray[i + 1]);
     * Log.e(Constants.TAG, "Insert as part of Recovery being done at port : " +
     * myPort + " key " + msgReadArray[i] + " genHashKey " +
     * genHash(msgReadArray[i]) + " value " + msgReadArray[i + 1] + "\n");
     * localInsert(mUri, contentValues); } } } catch (Exception e) { } } }
     */
    private class ClientThread implements Runnable {

        Thread t;

        String msg;

        String msgArray[];

        Socket socket;

        PrintWriter out;

        ClientThread(String msg) {
            t = new Thread(this);
            this.msg = msg;
            t.start();
        }

        public void run() {

            try {

                // Step 1 : Split the message
                msgArray = msg.split("\\|");

                // Step 2 : create a socket
                socket = new Socket(InetAddress.getByAddress(new byte[] {
                        10, 0, 2, 2
                }), Integer.parseInt(msgArray[2]));

                // Step 3: Create an output stream and send the message
                out = new PrintWriter(socket.getOutputStream(), true);
                out.println(msg);

            } catch (UnknownHostException e) {
                Log.e(Constants.TAG, e.toString() + " : ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(Constants.TAG, e.toString() + " : ClientTask socket IOException");
            } catch (Exception e) {
                Log.e(Constants.TAG, e.toString() + " : Emulator is not available. Cannot join \n");
            }
        }
    }

    public class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {

            ServerSocket serverSocket = sockets[0];
            Socket socket;
            BufferedReader in;
            String msgRead, key, value, responseMessage;
            String[] msgReadArray;
            int localRowsDeleted;
            Cursor localCursor;
            ContentValues cv;
            int i;

            // Step 1 : Instantiate mUri and mCv
            cv = new ContentValues();

            try {
                while (true && !isCancelled()) {

                    // Step 2: Accept connections at the socket
                    socket = serverSocket.accept();

                    // Step 3 : Get the reader
                    in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                    // Step 4 : Read the message
                    msgRead = in.readLine();
                    msgReadArray = msgRead.split("\\|");

                    // Step 5 :Insert locally
                    if (msgReadArray[0].equalsIgnoreCase(Constants.INSERT_PROTOCOL)) {
                        key = msgReadArray[3];
                        value = msgReadArray[4];
                        cv.put(Constants.COLUMN_KEY, key);
                        cv.put(Constants.COLUMN_VALUE, value);
                        Log.e(Constants.TAG, "Data is being inserted Locally at " + sMyPort
                                + " .Request received from port : " + msgReadArray[1] + " key : "
                                + key + " genHashKey " + genHash(key) + " value " + value + "\n");
                        localInsert(sUri, cv, false);
                    }
                    // Step 6 : Delete locally and send response message
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.DELETE_PROTOCOL)) {
                        localRowsDeleted = localDelete(sUri, msgReadArray[3], null);
                        Log.e(Constants.TAG, "Delete is being executed locally  at port : "
                                + sMyPort + " .Request received from port : " + msgReadArray[1]
                                + " rows deleted " + localRowsDeleted + " key: " + msgReadArray[3]
                                + " genHash " + genHash(msgReadArray[3]) + "\n");
                        /*
                         * responseMessage = Constants.DELETE_RESPONSE +
                         * Constants.PIPE_DELIMITER + myPort +
                         * Constants.PIPE_DELIMITER + msgReadArray[1] +
                         * Constants.PIPE_DELIMITER + msgReadArray[3] +
                         * Constants.PIPE_DELIMITER + localRowsDeleted; new
                         * ClientThread(responseMessage);
                         */
                    }
                    // Step 7 : On receiving delete response increment rows
                    // deleted and number of response received.
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.DELETE_RESPONSE)) {
                        // Step 8 : We check that we are waiting for delete
                        // response and the key on which we are waiting
                        if (sIsDeleteResponseAwaited && msgReadArray[3].equals(sDeleteKey)) {
                            sGlobalRowsDeleted += Integer.parseInt(msgReadArray[4]);
                            sTotalDeleteResponse += 1;
                            Log.e(Constants.TAG, "Delete Response received at port : " + sMyPort
                                    + "  from port " + msgReadArray[1] + " rows deleted "
                                    + msgReadArray[3] + "\n");
                        }
                    }
                    // Step 8 : Query locally and send response message
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.QUERY_PROTOCOL)) {
                        localCursor = localQuery(sUri, null, msgReadArray[3], null, null);
                        Log.e(Constants.TAG, "Query has been  executed locally  at port : "
                                + sMyPort + " Request received from port : " + msgReadArray[1]
                                + " key: " + msgReadArray[3] + " hashKey: "
                                + genHash(msgReadArray[3]) + "\n");
                        if (localCursor.moveToFirst() || msgReadArray[3].compareTo("@") == 0
                                || msgReadArray[3].compareTo("*") == 0) {
                            responseMessage = Constants.QUERY_RESPONSE + Constants.PIPE_DELIMITER
                                    + sMyPort + Constants.PIPE_DELIMITER + msgReadArray[1]
                                    + Constants.PIPE_DELIMITER + msgReadArray[3]
                                    + Constants.PIPE_DELIMITER + cursorToString(localCursor);
                            new ClientThread(responseMessage);
                        }
                    }
                    // Step 9 : On receiving query response put the value in the
                    // global map and increment number of response received.
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.QUERY_RESPONSE)) {
                        // Step 10 : We check that we are waiting for query
                        // response and the key on which we are waiting
                        Log.e(Constants.TAG, "Query Response received at port : " + sMyPort
                                + "  from port " + msgReadArray[1] + " key " + msgReadArray[3]
                                + " isQueryResponseAwaited " + sIsQueryResponseAwaited + "\n");
                        if (sIsQueryResponseAwaited.get() == true
                                && msgReadArray[3].equals(sQueryKey)) {
                            insertIntoGlobalMap(msgRead);
                            sTotalQueryResponse += 1;
                            Log.e(Constants.TAG, "Query Response received at port : " + sMyPort
                                    + "  from port " + msgReadArray[1] + " key " + msgReadArray[3]
                                    + "\n");
                        }
                    }
                    // Step 11 : On Receiving Recovery Protocol Message
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.RECOVERY_PROTOCOL)) {
                        localCursor = localQuery(sUri, null, "@", null, null);
                        Log.e(Constants.TAG,
                                "Recovery Request has been  executed locally  at port : " + sMyPort
                                        + " Request received from port : " + msgReadArray[1] + "\n");
                        responseMessage = Constants.RECOVERY_RESPONSE + Constants.PIPE_DELIMITER
                                + sMyPort + Constants.PIPE_DELIMITER + msgReadArray[1]
                                + Constants.PIPE_DELIMITER + cursorToString(localCursor);
                        new ClientThread(responseMessage);
                    }
                    // Step 12 : On Receiving Recovery Response Message
                    else if (msgReadArray[0].equalsIgnoreCase(Constants.RECOVERY_RESPONSE)) {
                        Log.e(Constants.TAG, "Inside server task .Recovery response received"
                                + "  isRecoveryInProgress: " + sIsRecoveryInProgress
                                + "  totalRecoveryResponse: " + sTotalRecoveryResponse + "\n");
                        /* if(totalRecoveryResponse<3){ */
                        Log.e(Constants.TAG, "Recovery Response received at port : " + sMyPort
                                + "  from port " + msgReadArray[1] + "\n");
                        // insertIntoRecoveryMap(msgRead);
                        // new ProcessRecoveryResponseThread(msgRead);

                        for (i = 3; i < msgReadArray.length; i += 2) {
                            if (isKeyPartOfPartition(msgReadArray[i])) {
                                cv.put(Constants.COLUMN_KEY, msgReadArray[i]);
                                cv.put(Constants.COLUMN_VALUE, msgReadArray[i + 1]);
                                Log.e(Constants.TAG,
                                        "Insert as part of Recovery being done at port : "
                                                + sMyPort + " key " + msgReadArray[i]
                                                + " genHashKey " + genHash(msgReadArray[i])
                                                + " value " + msgReadArray[i + 1] + "\n");
                                localInsert(sUri, cv, true);
                            }
                        }

                        // totalRecoveryResponse += 1;
                        /*
                         * } else{ Log.e(Constants.TAG,
                         * "Recovery Response received at port : " + myPort +
                         * "  from port " + msgReadArray[1] + " but ignored " +
                         * "\n"); }
                         */
                    }
                }

                serverSocket.close();

            } catch (Exception e) {
                Log.e(Constants.TAG, e.getMessage());
            }
            return null;
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {

        String key, value, genHashKey = "";
        Coordinator coordinator;
        String msg;

        // Step 1 : Wait for the coordinator to recover
        // while (isRecoveryInProgress)
        // ;

        // Step 2 : Get the key to be inserted
        key = values.get(Constants.COLUMN_KEY).toString();

        // Step 3: Get the value to be inserted
        value = values.get(Constants.COLUMN_VALUE).toString();

        // Step 4 : Find the hash code of the key to be inserted
        try {
            genHashKey = genHash(key);
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to generate the hash Key");

        }

        // Step 5 : Get the coordinator where the key has to be inserted
        coordinator = getCoordinator(genHashKey);

        // Step 6 : Check if the Destination is self . If yes insert locally
        // else send to destination
        if (sMyPort.compareTo(coordinator.getmDestinationPort()) == 0) {
            Log.e(Constants.TAG, " Local Insert is being performed at port : " + sMyPort + " Key:"
                    + key + " genHashKey: " + genHashKey + " value " + value + "\n");
            localInsert(uri, values, false);
        } else {
            msg = Constants.INSERT_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                    + Constants.PIPE_DELIMITER + coordinator.getmDestinationPort()
                    + Constants.PIPE_DELIMITER + key + Constants.PIPE_DELIMITER + value;
            Log.e(Constants.TAG, "Insert Request for genHash " + genHashKey
                    + "is being forwarded to " + coordinator.getmDestinationPort());
            new ClientThread(msg);
        }

        // Step 7 : Check if first replica is self . If yes insert locally else
        // send it to first replica
        if (sMyPort.compareTo(coordinator.getmFirstReplicaPort()) == 0) {
            Log.e(Constants.TAG, " Local Insert is being performed at port : " + sMyPort + " Key:"
                    + key + " genHashKey: " + genHashKey + " value " + value + "\n");
            localInsert(uri, values, false);
        } else {
            msg = Constants.INSERT_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                    + Constants.PIPE_DELIMITER + coordinator.getmFirstReplicaPort()
                    + Constants.PIPE_DELIMITER + key + Constants.PIPE_DELIMITER + value;
            Log.e(Constants.TAG, "Insert Request for key " + key + " genHash " + genHashKey
                    + "is being forwarded to " + coordinator.getmFirstReplicaPort());
            new ClientThread(msg);
        }

        // Step 8 : Check if second replica is self . If yes insert locally else
        // send it to second replica
        if (sMyPort.compareTo(coordinator.getmSecondReplicaPort()) == 0) {
            Log.e(Constants.TAG, " Local Insert is being performed at port : " + sMyPort + " Key:"
                    + key + " genHashKey: " + genHashKey + " value " + value + "\n");
            localInsert(uri, values, false);
        } else {
            msg = Constants.INSERT_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                    + Constants.PIPE_DELIMITER + coordinator.getmSecondReplicaPort()
                    + Constants.PIPE_DELIMITER + key + Constants.PIPE_DELIMITER + value;
            Log.e(Constants.TAG, "Insert Request for key " + key + " genHash " + genHashKey
                    + "is being forwarded to " + coordinator.getmSecondReplicaPort());
            new ClientThread(msg);
        }

        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {

        int rowsUpdated;
        rowsUpdated = sqlDB.update(Constants.TABLE_DYNAMO, values, Constants.COLUMN_KEY + "=?",
                selectionArgs);
        getContext().getContentResolver().notifyChange(uri, null);
        return rowsUpdated;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {

        String key = selection;
        String genHashKey = "";
        String msg;
        Coordinator coordinator;

        // Step 1 : Enter Delete functionality only if isDeleteInProgress=false
        // and isRecoveryInProgress = false
        while (sIsDeleteInProgress /* || isRecoveryInProgress */)
            ;

        // Step 2 : Set globalRowsDeleted and totalDeleteResponse to 0 and
        sIsDeleteInProgress = true;
        sGlobalRowsDeleted = 0;
        sTotalDeleteResponse = 0;
        sIsDeleteResponseAwaited = false;

        // Step 3 : Find the hash code of the key to be deleted
        try {
            genHashKey = genHash(key);
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to generate Hash Key " + "\n");
        }

        // Step 4 : Handling Request for @
        if (selection.compareTo("@") == 0) {
            sGlobalRowsDeleted = localDelete(uri, "@", selectionArgs);
            Log.e(Constants.TAG, "Delete Performed locally  at port: " + sMyPort + " Rows deleted "
                    + sGlobalRowsDeleted + " key " + key + " genHashKey " + genHashKey + "\n");
        }
        // Step 5 : Handling Requests for *
        else if (selection.compareTo("*") == 0) {

            // Step 6 : Perform Local Delete and increment globalRowsDeleted and
            // totalDeleteResponse
            sGlobalRowsDeleted += localDelete(uri, "@", selectionArgs);
            sTotalDeleteResponse += 1;
            Log.e(Constants.TAG, "Delete Performed locally  at port: " + sMyPort + " Rows deleted "
                    + sGlobalRowsDeleted + " key " + key + " genHashKey " + genHashKey + "\n");

            // Step 7 : Set the condition variable
            sIsDeleteResponseAwaited = true;
            sDeleteKey = "@";

            // Step 8 : Send the Delete Request to all the remaining nodes one
            // by one and wait for their response
            if (!sMyPort.equals(Constants.REMOTE_PORT0)) {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT0
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT0 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT1)) {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT1
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT1 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT2)) {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT2
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT2 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT3)) {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT3
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT3 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT4)) {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT4
                        + Constants.PIPE_DELIMITER + "@";
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT4 + "\n");
                new ClientThread(msg);
            }

            // Step 9 : Wait for timeout to receive responses
            /*
             * try { Thread.sleep(Constants.TIMEOUT); } catch (Exception e) {
             * Log.e(Constants.TAG, "Unable to set thread to sleep for timeout "
             * + "\n"); } // Step 10 : Wait for atleast 4 responses while
             * (totalDeleteResponse < 4) ;
             */

            // Step 11 : Set isDeleteResponseAwaited to false so that no more
            // responses are processed
            sIsDeleteResponseAwaited = false;
        } else {

            // Step 12 : Get the co-ordinator of the genHash key
            coordinator = getCoordinator(genHashKey);

            // Step 13 : Set the condition variable
            sIsDeleteResponseAwaited = true;
            sDeleteKey = key;

            // Step 14 : Check if the Destination is self . If yes delete
            // locally else send to destination
            if (sMyPort.compareTo(coordinator.getmDestinationPort()) == 0) {
                Log.e(Constants.TAG, " Local Delete is being performed at port : " + sMyPort
                        + " Key:" + key + " genHashKey: " + genHashKey + "\n");
                sGlobalRowsDeleted += localDelete(uri, key, selectionArgs);
                sTotalDeleteResponse += 1;
            } else {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmDestinationPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmDestinationPort());
                new ClientThread(msg);
            }

            // Step 15 : Check if first replica is self . If yes delete locally
            // else send it to first replica
            if (sMyPort.compareTo(coordinator.getmFirstReplicaPort()) == 0) {
                Log.e(Constants.TAG, " Local Delete is being performed at port : " + sMyPort
                        + " Key:" + key + " genHashKey: " + genHashKey + "\n");
                sGlobalRowsDeleted += localDelete(uri, key, selectionArgs);
                sTotalDeleteResponse += 1;
            } else {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmFirstReplicaPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmFirstReplicaPort());
                new ClientThread(msg);
            }

            // Step 16 : Check if second replica is self . If yes delete locally
            // else send it to second replica
            if (sMyPort.compareTo(coordinator.getmSecondReplicaPort()) == 0) {
                Log.e(Constants.TAG, " Local Delete is being performed at port : " + sMyPort
                        + " Key:" + key + " genHashKey: " + genHashKey + "\n");
                sGlobalRowsDeleted += localDelete(uri, key, selectionArgs);
                sTotalDeleteResponse += 1;
            } else {
                msg = Constants.DELETE_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmSecondReplicaPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Delete Request for genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmSecondReplicaPort());
                new ClientThread(msg);
            }

            // Step 17 : Wait for timeout to receive responses
            /*
             * try { Thread.sleep(Constants.TIMEOUT); } catch (Exception e) {
             * Log.e(Constants.TAG, "Unable to set thread to sleep for timeout "
             * + "\n"); } // Step 18 :Wait for atleast 1 delete response while
             * (totalDeleteResponse < 1) ;
             */

            // Step 19 : Set isDeleteResponseAwaited to false so that no more
            // responses are processed
            sIsDeleteResponseAwaited = false;
        }

        // Step 20 : Return total Number Of rows deleted
        Log.e(Constants.TAG, "Total Number Of Delete Response Received : " + sTotalDeleteResponse
                + " Total Number Of Rows Deleted " + sGlobalRowsDeleted + "\n");
        // returnValue = globalRowsDeleted / totalDeleteResponse;
        sGlobalRowsDeleted = 0;
        sTotalDeleteResponse = 0;
        sIsDeleteInProgress = false;
        return 0;

    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {

        String genHashKey = "";
        String key = selection;
        String msg;
        Cursor localCursor;
        Coordinator coordinator;

        // Step 1 : Wait for isQueryInProgress flag and isRecoveryInProgress
        // flag
        try {
            while (!sIsQueryInProgress.compareAndSet(false, true)) {
                Thread.sleep(Constants.WAIT_TIMEOUT);
            }
        } catch (Exception e) {

        }

        // Step 2 : Define the conditions and Initialize the variables
        sGlobalCursor = null;
        sGlobalMap.clear();
        sTotalQueryResponse = 0;
        sIsQueryResponseAwaited.set(true);

        // Step 3 : Find the hash code of the key to be searched
        try {
            genHashKey = genHash(key);
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to generate genHashkey\n");
        }

        /*
         * // Step 4 : If selection Criteria is @ just perform a local Query if
         * (key.equals("@")) { globalCursor = localQuery(uri, null, selection,
         * null, null); Log.e(Constants.TAG,
         * "Query has been performed locally  at port: " + sMyPort + " key: " +
         * key + " genHashkey: " + genHashKey + "\n"); globalMap =
         * cursorToMap(globalCursor); }
         */
        // Step 5 : If selection criteria is * perform local query and send
        // request to all the co-ordinators
        if (key.equals("*") || key.equals("@")) {
            // Step 6 : Perform the query locally first
            sGlobalCursor = localQuery(uri, null, "@", null, null);
            sGlobalMap = cursorToMap(sGlobalCursor);
            sTotalQueryResponse += 1;
            Log.e(Constants.TAG, "Query has been performed locally  at port: " + sMyPort + " key:"
                    + key + " genHashkey: " + genHashKey + "\n");

            // Step 7 : Set the condition variable
            sQueryKey = key;

            // Step 8 : Send the Query Request to all the remaining nodes one by
            // one and wait for their response
            if (!sMyPort.equals(Constants.REMOTE_PORT0)) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT0
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT0 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT1)) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT1
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT1 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT2)) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT2
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT2 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT3)) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT3
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT3 + "\n");
                new ClientThread(msg);
            }
            if (!sMyPort.equals(Constants.REMOTE_PORT4)) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + Constants.REMOTE_PORT4
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + Constants.REMOTE_PORT4 + "\n");
                new ClientThread(msg);
            }

            // Step 9 : Wait for timeout to receive responses
            /*
             * try { Thread.sleep(Constants.TIMEOUT); } catch (Exception e) {
             * Log.e(Constants.TAG, "Unable to set thread to sleep for timeout "
             * + "\n"); }
             */

            // Step 10 : Wait for atleast 4 query Responses
            try {
                while (sTotalQueryResponse < 4) {
                    Thread.sleep(Constants.WAIT_TIMEOUT);
                }

            } catch (Exception e) {

            }

            // Step 11 : Set isDeleteResponseAwaited to false so that no more
            // responses are processed
            sIsQueryResponseAwaited.set(false);
        }
        // Step 12 : If the query is for a particular key
        else {

            // Step 13 : Get the co-ordinator of the genHash key
            coordinator = getCoordinator(genHashKey);

            // Step 14 : Set the condition variable
            sQueryKey = key;

            // Step 15 : Check if the Destination is self . If yes query locally
            // else send to destination
            if (sMyPort.compareTo(coordinator.getmDestinationPort()) == 0) {
                Log.e(Constants.TAG, " Local Query performed at port : " + sMyPort + " Key:" + key
                        + " genHashKey: " + genHashKey + "\n");
                sGlobalCursor = localQuery(uri, null, selection, null, null);
                sGlobalMap = cursorToMap(sGlobalCursor);
                if (sGlobalCursor.moveToFirst()) {
                    sTotalQueryResponse += 1;
                }
            } else if (sMyPort.compareTo(coordinator.getmFirstReplicaPort()) == 0) {
                Log.e(Constants.TAG, " Local Query is being performed at port : " + sMyPort
                        + " Key:" + key + " genHashKey: " + genHashKey + "\n");
                sGlobalCursor = localQuery(uri, null, selection, null, null);
                sGlobalMap = cursorToMap(sGlobalCursor);
                if (sGlobalCursor.moveToFirst()) {
                    sTotalQueryResponse += 1;
                }
            } else if (sMyPort.compareTo(coordinator.getmSecondReplicaPort()) == 0) {
                Log.e(Constants.TAG, " Local Query is being performed at port : " + sMyPort
                        + " Key:" + key + " genHashKey: " + genHashKey + "\n");
                sGlobalCursor = localQuery(uri, null, selection, null, null);
                sGlobalMap = cursorToMap(sGlobalCursor);
                if (sGlobalCursor.moveToFirst()) {
                    sTotalQueryResponse += 1;
                }
            }

            if (sMyPort.compareTo(coordinator.getmDestinationPort()) != 0
                    && sTotalQueryResponse < 1) {

                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmDestinationPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmDestinationPort() + "\n");
                new ClientThread(msg);
            }

            // Waiting for some response
            try {
                Thread.sleep(Constants.QUERY_TIMEOUT);
            } catch (Exception e) {
                Log.e(Constants.TAG, "Unable to set thread to sleep for timeout " + "\n");
            }

            if (sMyPort.compareTo(coordinator.getmFirstReplicaPort()) != 0
                    && sTotalQueryResponse < 1) {
                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmFirstReplicaPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmFirstReplicaPort() + "\n");
                new ClientThread(msg);
            }

            // Waiting for some response
            try {
                Thread.sleep(Constants.QUERY_TIMEOUT);
            } catch (Exception e) {
                Log.e(Constants.TAG, "Unable to set thread to sleep for timeout " + "\n");
            }

            if (sMyPort.compareTo(coordinator.getmSecondReplicaPort()) != 0
                    && sTotalQueryResponse < 1) {

                msg = Constants.QUERY_PROTOCOL + Constants.PIPE_DELIMITER + sMyPort
                        + Constants.PIPE_DELIMITER + coordinator.getmSecondReplicaPort()
                        + Constants.PIPE_DELIMITER + key;
                Log.e(Constants.TAG, "Query Request for key " + key + " genHash " + genHashKey
                        + "is being forwarded to " + coordinator.getmSecondReplicaPort() + "\n");
                new ClientThread(msg);
            }

            // Step 16 : Wait for timeout to receive responses
            /*
             * try { Thread.sleep(Constants.TIMEOUT); } catch (Exception e) {
             * Log.e(Constants.TAG, "Unable to set thread to sleep for timeout "
             * + "\n"); }
             */

            // Step 17 : totalQueryResponse should be atleast 2
            try {
                while (sTotalQueryResponse < 1)
                    Thread.sleep(Constants.WAIT_TIMEOUT);
                ;
            } catch (Exception e) {

            }

            // Step 18 : Set isDeleteResponseAwaited to false so that no more
            // responses are processed
            sIsQueryResponseAwaited.set(false);

        }

        // Step 19 : Convert Global Map to Cursor
        /*
         * Log.e(Constants.TAG, "Total Number Of Query Responses Received :" +
         * totalQueryResponse + "\n");
         */
        sGlobalCursor = mapToCursor(sGlobalMap);
        localCursor = sGlobalCursor;
        sGlobalCursor = null;
        sGlobalMap.clear();
        sTotalQueryResponse = 0;
        sIsQueryInProgress.set(false);

        // Step 20 : Return the cursor
        return localCursor;

    }

    // This function performs local Insert
    public Uri localInsert(Uri uri, ContentValues values, boolean isRecovery) {

        String key;
        String keyArray[];
        Cursor queryResult;
        int keyIndex;
        int valueIndex;
        String returnKey;
        String returnValue;

        // Step 1: Get the key and value to be inserted
        key = values.get(Constants.COLUMN_KEY).toString();
        keyArray = new String[] {
            key
        };

        // Step 2 : Find if already a record exists with the given key
        queryResult = localQuery(uri, null, key, null, null);
        if (queryResult.getCount() > 0 && !isRecovery) {
            update(uri, values, Constants.COLUMN_KEY, keyArray);
        } else if (queryResult.getCount() == 0) {
            sqlDB.insert(Constants.TABLE_DYNAMO, null, values);
            queryResult = localQuery(uri, null, key, null, null);
            if (queryResult != null && queryResult.getCount() > 0 && queryResult.moveToFirst()) {
                keyIndex = queryResult.getColumnIndex(Constants.COLUMN_KEY);
                valueIndex = queryResult.getColumnIndex(Constants.COLUMN_VALUE);
                try {
                    returnKey = queryResult.getString(keyIndex);
                    returnValue = queryResult.getString(valueIndex);
                    // Log.e(Constants.TAG,
                    // " Inside Insert .Query output is : key : " + returnKey +
                    // " value: " + returnValue + "\n");
                    // Log.e(Constants.TAG, "Insert successful for key: " +
                    // returnKey + " value: " + returnValue + "\n");
                } catch (Exception e) {
                    Log.e(Constants.TAG, e.toString());
                }
            }
        }

        // Step 3 : Notify the changes
        getContext().getContentResolver().notifyChange(uri, null);
        return uri;

    }

    // This function performs local Delete
    public int localDelete(Uri uri, String selection, String[] selectionArgs) {

        int numberOfRowsDeleted;
        String args[] = {
            selection
        };
        if (selection.equals("@") || selection.equals("*")) {
            numberOfRowsDeleted = sqlDB.delete(Constants.TABLE_DYNAMO, null, null);
        } else {
            numberOfRowsDeleted = sqlDB.delete(Constants.TABLE_DYNAMO, Constants.COLUMN_KEY + "=?",
                    args);
        }
        getContext().getContentResolver().notifyChange(uri, null);
        Log.e(Constants.TAG, "Delete successful \n");
        return numberOfRowsDeleted;
    }

    // This function performs local Delete
    public Cursor localQuery(Uri uri, String[] projection, String selection,
            String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        Cursor cursor;
        sQueryBuilder.setTables(Constants.TABLE_DYNAMO);
        String argument[] = {
            selection
        };

        // Step 1 : Fire a local Query
        if (selection.equals("@") || selection.equals("*")) {
            cursor = sQueryBuilder.query(sqlDB, projection, null, null, null, null, sortOrder);
        } else {
            cursor = sQueryBuilder.query(sqlDB, projection, Constants.COLUMN_KEY + "=?", argument,
                    null, null, sortOrder);
        }

        // Step 2 : Make sure that potential listeners are getting notified
        cursor.setNotificationUri(getContext().getContentResolver(), uri);
        // Log.e(Constants.TAG ," Local query successful \n");
        return cursor;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    // This function builds the URI
    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    public boolean isKeyPartOfPartition(String key) {

        String genHashKey;
        try {
            genHashKey = genHash(key);
            if (sMyPort.equals(Constants.REMOTE_PORT0)
                    && (genHashKey.compareTo(sGenHash5560) > 0 || genHashKey
                            .compareTo(sGenHash5554) <= 0)) {
                return true;
            } else if (sMyPort.equals(Constants.REMOTE_PORT1)
                    && (genHashKey.compareTo(sGenHash5558) > 0 || genHashKey
                            .compareTo(sGenHash5556) <= 0)) {
                return true;
            } else if (sMyPort.equals(Constants.REMOTE_PORT2)
                    && (genHashKey.compareTo(sGenHash5562) > 0 && genHashKey
                            .compareTo(sGenHash5558) <= 0)) {
                return true;
            } else if (sMyPort.equals(Constants.REMOTE_PORT3)
                    && (genHashKey.compareTo(sGenHash5556) > 0 && genHashKey
                            .compareTo(sGenHash5560) <= 0)) {
                return true;
            } else if (sMyPort.equals(Constants.REMOTE_PORT4)
                    && (genHashKey.compareTo(sGenHash5554) > 0 || genHashKey
                            .compareTo(sGenHash5562) <= 0)) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to get the hash key");

        }
        return false;
    }

    // This function provides the co-ordinator and its replica based on genhash
    // of the key
    public Coordinator getCoordinator(String genHashKey) {

        Coordinator coordinator = null;

        try {
            if (genHashKey.compareTo(sGenHash5562) > 0 && genHashKey.compareTo(sGenHash5556) <= 0) {
                coordinator = new Coordinator("11112", "11108", "11116");
            } else if (genHashKey.compareTo(sGenHash5556) > 0
                    && genHashKey.compareTo(sGenHash5554) <= 0) {
                coordinator = new Coordinator("11108", "11116", "11120");
            } else if (genHashKey.compareTo(sGenHash5554) > 0
                    && genHashKey.compareTo(sGenHash5558) <= 0) {
                coordinator = new Coordinator("11116", "11120", "11124");
            } else if (genHashKey.compareTo(sGenHash5558) > 0
                    && genHashKey.compareTo(sGenHash5560) <= 0) {
                coordinator = new Coordinator("11120", "11124", "11112");
            } else if (genHashKey.compareTo(sGenHash5560) > 0
                    || genHashKey.compareTo(sGenHash5562) <= 0) {
                coordinator = new Coordinator("11124", "11112", "11108");
            }

        } catch (Exception e) {
            Log.e(Constants.TAG, "Algorithm Exception \n");
        }

        return coordinator;

    }

    // This function provided genHash of all the coordinators
    public void getGenhashCoordinator() {

        try {
            sGenHash5554 = genHash("5554");
            sGenHash5556 = genHash("5556");
            sGenHash5558 = genHash("5558");
            sGenHash5560 = genHash("5560");
            sGenHash5562 = genHash("5562");
        } catch (Exception e) {
            Log.e(Constants.TAG, "Algorithm Exception \n");
        }
    }

    // This function provides genHash of a string
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    // This function converts Map to Matrix Cursor
    public MatrixCursor mapToCursor(Map<String, String> cursorMap) {
        Map.Entry<String, String> mapEntry;
        MatrixCursor cursor = new MatrixCursor(new String[] {
                Constants.COLUMN_KEY, Constants.COLUMN_VALUE
        });
        Iterator<Map.Entry<String, String>> iterator = cursorMap.entrySet().iterator();
        while (iterator.hasNext()) {
            mapEntry = (Map.Entry<String, String>)iterator.next();
            cursor.addRow(new String[] {
                    (String)mapEntry.getKey(), (String)mapEntry.getValue()
            });
        }
        return cursor;
    }

    // This function converts cursor to String
    public String cursorToString(Cursor cursor) {
        String output;
        HashMap<String, String> cursorMap = cursorToMap(cursor);
        output = mapToString(cursorMap);
        return output;
    }

    // This function converts cursor to Map
    public HashMap<String, String> cursorToMap(Cursor cursor) {
        int count;
        HashMap<String, String> cursorMap = new HashMap<String, String>();
        cursor.moveToFirst();
        for (count = 0; count < cursor.getCount(); count++) {
            cursorMap.put(cursor.getString(cursor.getColumnIndex(Constants.COLUMN_KEY)),
                    cursor.getString(cursor.getColumnIndex(Constants.COLUMN_VALUE)));
            cursor.moveToNext();
        }
        return cursorMap;
    }

    // This function converts Map to String
    public String mapToString(HashMap<String, String> cursorMap) {
        String output = "";
        Map.Entry<String, String> mapEntry;
        Iterator<Map.Entry<String, String>> iterator = cursorMap.entrySet().iterator();
        while (iterator.hasNext()) {
            mapEntry = (Map.Entry<String, String>)iterator.next();
            output = output + (String)mapEntry.getKey();
            output = output + Constants.PIPE_DELIMITER;
            output = output + (String)mapEntry.getValue();
            output = output + Constants.PIPE_DELIMITER;
        }

        return output;

    }

    // This function converts String to Map
    public HashMap<String, String> stringToMap(String input) {

        int count = 0;
        int temp;
        HashMap<String, String> cursorMap = new HashMap<String, String>();
        String[] stringArray = input.split("\\|");
        for (count = 0; count < stringArray.length; count += 2) {
            temp = count + 1;
            cursorMap.put(stringArray[count], stringArray[temp]);
        }

        return cursorMap;

    }

    // This function inserts into the global Map
    public void insertIntoGlobalMap(String input) {
        int count;
        int temp;
        String[] stringArray = input.split("\\|");
        if (sQueryKey.compareTo("@") == 0) {
            for (count = 4; count < stringArray.length; count += 2) {
                if (isKeyPartOfPartition(stringArray[count])) {
                    temp = count + 1;
                    sGlobalMap.put(stringArray[count], stringArray[temp]);
                }
            }
        } else {
            for (count = 4; count < stringArray.length; count += 2) {
                temp = count + 1;
                sGlobalMap.put(stringArray[count], stringArray[temp]);
            }
        }
    }

    /*
     * public void insertIntoRecoveryMap(String input) { int count; int temp;
     * String[] stringArray = input.split("\\|"); for (count = 3; count <
     * stringArray.length; count += 2) { temp = count + 1;
     * recoveryMap.put(stringArray[count], stringArray[temp]); } }
     */

    /*
     * public void insertMapIntoDatabase(HashMap<String, String> localMap) {
     * Map.Entry<String, String> mapEntry; String key, value;
     * Iterator<Map.Entry<String, String>> iterator =
     * localMap.entrySet().iterator(); ContentValues localContentValues = new
     * ContentValues(); try { while (iterator.hasNext()) { mapEntry =
     * (Map.Entry<String, String>) iterator.next(); key = mapEntry.getKey();
     * value = mapEntry.getValue(); if (isKeyPartOfPartition(key)) {
     * localContentValues.put(Constants.COLUMN_KEY, key);
     * localContentValues.put(Constants.COLUMN_VALUE, value);
     * Log.e(Constants.TAG, "Insert as part of Recovery being done at port : " +
     * myPort + " key " + key + " genHashKey " + genHash(key) + " value " +
     * value + "\n"); localInsert(mUri, localContentValues); } } } catch
     * (Exception e) { Log.e(Constants.TAG, "Unable to generate Hash \n"); } }
     */

}
