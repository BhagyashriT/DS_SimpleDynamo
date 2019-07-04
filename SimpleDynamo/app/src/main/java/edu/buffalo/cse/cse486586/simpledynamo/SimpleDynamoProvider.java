package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {

    static final String TAG = SimpleDynamoProvider.class.getSimpleName();

    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final int SERVER_PORT = 10000;
    boolean lock = false;
    String port1 = "5554";
    String port2 = "5556";
    String port3 = "5558";
    String port4 = "5560";
    String port5 = "5562";
    String[] arr1 = { "5562", "5556", "5554" };
    String[] arr2 = { "5556", "5554", "5558"  };
    String[] arr3 = { "5554", "5558", "5560"  };
    String[] arr4 = { "5558", "5560", "5562"  };
    String[] arr5 = { "5560", "5562", "5556"  };

    List<String[]> list = new ArrayList<String[]>();
    {
        list.add(arr1);
        list.add(arr2);
        list.add(arr3);
        list.add(arr4);
        list.add(arr5);
    }

//            String[] arr1 = { "5562", "5560", "5558" };
//            String[] arr2 = { "5556", "5562", "5550"  };
//            String[] arr3 = { "5554", "5556", "5562"  };
//            String[] arr4 = { "5558", "5554", "5556"  };
//            String[] arr5 = { "5560", "5558", "5554"  };



    String[] portsArray = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};

    HashMap<String, String> hmap = new HashMap<String, String>();

    ArrayList<String> hashList=new ArrayList<String>(){

    };
    ArrayList<String> ring=new ArrayList<String>();
    {
        ring.add("5562");
        ring.add("5556");
        ring.add("5554");
        ring.add("5558");
        ring.add("5560");
    };
    ArrayList<String> ports = new ArrayList<String>();
    {
        ports.add(ring.get(0));
        ports.add(ring.get(1));
        ports.add(ring.get(2));
        ports.add(ring.get(3));
        ports.add(ring.get(4));
    }



    private final Uri myUri  = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
    int myPortAdress = 0;
    String myavd = null;

    //   MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }



    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        try {
            lock=true;

            String myPort = getmyPort();
            String portStr = getmyavd();
            myPortAdress = Integer.parseInt(myPort);
            myavd = portStr;

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
//            System.out.println(path3);
            String path3 = getContext().getFilesDir().getAbsolutePath();

            File fileName3 = new File(path3);
            ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName3.listFiles()));
            for (File file : files) {
                if (file.isFile()) {
                    // Confirmation that this is recovery mode!
                    String filename = file.getName();
                    getContext().deleteFile(filename);

                }
            }
            String first_rep = null;
            String second_rep = null;
            String coordinator = null;
            String subcoordinator = null;

         //   if(lock) {
                for (String[] array : list) {
                    if (myavd.equals(array[0])) {
                        first_rep = array[1];
                        second_rep = array[2];
                        String msg = "getFromreplicas";
                        Log.d("onCreate",msg+" "+first_rep+" "+second_rep);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, first_rep);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, second_rep);
                    }
                    if (myavd.equals(array[2])) {
                        coordinator = array[0];
                        subcoordinator = array[1];
                        String msg = "plsReplicateAgain";

                        Log.d("onCreate",msg+" "+coordinator+" "+subcoordinator);

                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, coordinator);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, subcoordinator);

                    }

                }
                lock=false;
           // }

//          System.out.println("Port is: "+ myPort);

            hmap.put(genHash(port1),port1);
            hashList.add(genHash(port1));
            hmap.put(genHash(port2),port2);
            hashList.add(genHash(port2));
            hmap.put(genHash(port3),port3);
            hashList.add(genHash(port3));
            hmap.put(genHash(port4),port4);
            hashList.add(genHash(port4));
            hmap.put(genHash(port5),port5);
            hashList.add(genHash(port5));
            Collections.sort(hashList);

//            System.out.println("hashList"+hashList+"\n");




        } catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
        }
        return true;
    }

    public void checkLock(){
        while(lock){
            System.out.println("Waiting for recovery to finish at insert");
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        try {
//            while(lock){
//                System.out.println("Waiting for recovery to finish at insert");
//                Thread.sleep(200);
//            }
            checkLock();
            String key = (String) values.get("key");
            String value = (String) values.get("value");
            String hashOfkey = genHash(key);
            String temp = null;
            temp = findPosition(hashOfkey);

            String position = hmap.get(temp);
            System.out.println("Key: " + key + " Value: " + value + ", Belongs to AVD: " + position);

            // Temp is hash, avd is myavd at that temp hash
            //       System.out.println("Position: " + position);

            int current = myPortAdress/2;
            String first_rep = null;
            String second_rep = null;


            if(position.equals(myavd)) {
                Log.d(TAG,"key"+key+", value"+value+" to be stored in:"+myPortAdress);
                osw(key,value);
                for(String[] array:list ){
                    if(position.equals(array[0])){
                        first_rep = array[1];
                        second_rep = array[2];
                        String msg = "replicate";
                        String ports = first_rep + ":::" + key + ":::" + value;
                        String ports2 = second_rep + ":::" + key + ":::" + value;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, ports);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, ports2);
                    }

                }

            }

            else{
                Log.d(TAG,"key" + key + ", value" + value + " to be stored in:" + position);
                for(String[] array:list ){
                    if(position.equals(array[0])){
                        first_rep = array[1];
                        second_rep = array[2];
                        String msg = "replicate";
                        String port = position + ":::" + key + ":::" + value;
                        String ports = first_rep + ":::" + key + ":::" + value;
                        String ports2 = second_rep + ":::" + key + ":::" + value;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, port);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, ports);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, ports2);
                    }

                }

            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }



    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        String[] cols = {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cols);   //https##//developer.android.com/reference/android/database/MatrixCursor

        try {
//            while(lock){
//                System.out.println("Waiting for recovery to finish at query");
//
//                Thread.sleep(200);
//            }
            checkLock();
            Log.d(TAG,"selection in query: "+selection);
            System.out.println("Hash List size is: " + hashList.size());
            Collections.sort(hashList);

            if (selection.equals("@")) {
                String path3 = getContext().getFilesDir().getAbsolutePath();
                File fileName3 = new File(path3);
                ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName3.listFiles()));
                for (File file : files) {
                    if (file.isFile()) {
                        String filename = file.getName();
                        //      String p = callFis(filename);

                        try {
                            FileInputStream fis = getContext().openFileInput(filename);
                            InputStreamReader isr = new InputStreamReader(fis);
                            BufferedReader bufferedReader = new BufferedReader(isr);
                            String str =  bufferedReader.readLine();
                            bufferedReader.close();
                            Object[] eachRow = {filename, str};         //Adding key, value to array of eachRow
                            cursor.addRow(eachRow);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }
            } else if (selection.equals("*")) {
                Log.d(TAG, "Selection is *");
                String path4 = getContext().getFilesDir().getAbsolutePath();
                File fileName4 = new File(path4);

                ArrayList<File> files = new ArrayList<File>(Arrays.asList(fileName4.listFiles()));
                for (File file : files) {
                    if (file.isFile()) {
                        String filename = file.getName();

                        String value = callFis(filename);
                        Object[] eachRow = {filename, value};         //Adding key, value to array of eachRow

                        cursor.addRow(eachRow);
                    }
                }

                String val = String.valueOf(myPortAdress / 2);
                String strreturned;
                if ((strreturned= new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "Star", myavd).get())!=null) {
                    String[] strarray = strreturned.split("-");

                    for (int i = 0; i < strarray.length; i++) {
                        String[] str = strarray[i].split("##");
                        String fname = str[0].trim();
                        String pval = str[1].trim();

                        Object[] eachRow = {fname, pval};         //Adding key, value to array of eachRow

                        cursor.addRow(eachRow);
                    }
                }
            }

            else {
                System.out.println("Normal query opertaion.");
                String filename = selection;
                System.out.println("Selection is " + selection);
                //    String node_Hash = null;
                String current_hash = genHash(filename);
                System.out.println("QueryingFile " + selection + " HashValue " + current_hash);
                //  Collections.sort(hashList);
                String node_Hash = findPosition(current_hash);
                String avd = hmap.get(node_Hash);
                int current = myPortAdress / 2;
                String first_rep = null;
                String second_rep = null;
                for (String[] array : list) {
                    if (myavd.equals(array[0])) {
                        first_rep = array[1];
                        second_rep = array[2];

                    }
                }
                if (!avd.equals(myavd)) {
                    String message2send = "Chknext";
                    String msg2send = "PresentInAvd" + ":::" + avd + ":::"  + filename;
                    Log.d(TAG,"msg2send: "+msg2send);
                    String str = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2send,msg2send).get();
                    if(str!=null) {
                        Log.d(TAG, "str: " + str);
                        String[] strarray = str.split(":::");
                        String fname = strarray[0].trim();
                        String pval = strarray[1].trim();
                        Log.d(TAG, "filename: " + filename + ", " + "pval: " + pval);
                        Object[] eachRow = {filename, pval};         //Adding key, value to array of eachRow
                        cursor.addRow(eachRow);
                    }
                    else{
                        String msg2send1 = "PresentInAvd" + ":::" + first_rep + ":::"  + filename;
                        Log.d(TAG,"msg2send: "+msg2send);

                        String str1 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2send,msg2send).get();
                        if(str1!=null) {
                            Log.d(TAG, "str: " + str);
                            String[] strarray = str1.split(":::");
                            String fname = strarray[0].trim();
                            String pval = strarray[1].trim();
                            Log.d(TAG, "filename: " + filename + ", " + "pval: " + pval);
                            Object[] eachRow = {filename, pval};         //Adding key, value to array of eachRow
                            cursor.addRow(eachRow);
                        }
                        else {
                            String msg2send2 = "PresentInAvd" + ":::" + second_rep + ":::" + filename;
                            Log.d(TAG, "msg2send: " + msg2send);

                            String str2 = new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message2send, msg2send).get();
                            if (str2 != null) {
                                Log.d(TAG, "str: " + str);
                                String[] strarray = str2.split(":::");
                                String fname = strarray[0].trim();
                                String pval = strarray[1].trim();
                                Log.d(TAG, "filename: " + filename + ", " + "pval: " + pval);
                                Object[] eachRow = {filename, pval};         //Adding key, value to array of eachRow
                                cursor.addRow(eachRow);
                            }
                        }

                    }
                }  if (avd.equals(myavd))  {
                    Log.d(TAG,"selection is: "+selection);

                    Log.d(TAG,"Normal querying current avd.");
//                        String line = callFis(selection);
                    try {

                        FileInputStream fis = getContext().openFileInput(filename);

                        InputStreamReader isr = new InputStreamReader(fis);
                        BufferedReader bufferedReader = new BufferedReader(isr);
//                            String line = bufferedReader.readLine();

                        String str = bufferedReader.readLine();
//                            while (line != null) {
//                                p = line;
//                            }
                        Object[] eachRow = {filename, str};         //Adding key, value to array of eachRow
                        cursor.addRow(eachRow);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }


                }
            }
        }  catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return cursor;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection,
                      String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub


        try {
            if (hashList.size() != 1) {
                String filename = selection;
                String current_hash = genHash(filename);
                String node_Hash;
                Log.d(TAG, "delete filename: " + filename);
                node_Hash = findPosition(current_hash);
                String avd = hmap.get(node_Hash);
                int current = myPortAdress / 2;
                String first_rep = null;
                String second_rep = null;

                if (avd.equals(myavd)) {
                    getContext().deleteFile(filename);
                    for (String[] array : list) {
                        first_rep = array[1];
                        second_rep = array[2];
                        String msg = "Delete";
                        String avd_fname1 = first_rep + "##" + filename;
                        String avd_fname2 = second_rep + "##" + filename;
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, avd_fname1);
                        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, avd_fname2);
                    }
                } else {
                    String insert_msg = "Delete";
                    String avd_fname = avd + "##" + filename;
                    String first_repl = null;
                    String second_repl = null;


                    for (String[] array : list) {
                        if (avd.equals(array[0])) {
                            first_repl = array[1];
                            second_repl = array[2];

                            String avd_fname2 = first_repl + "##" + filename;
                            String avd_fname3 = second_repl + "##" + filename;

                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname2);
                            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, insert_msg, avd_fname3);

                        }


                    }
                }


            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return 0;


    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Log.d(TAG,"Server socket established.");
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());
                    BufferedReader reader = new BufferedReader(input);
                    String message = reader.readLine();

                    //          System.out.println("Message at serverTask is: "+message);
                    String msg[]=message.split(":::");
                    //        System.out.println("msg[0]: "+msg[0]);
                    if (message != null) {

                        if(msg[0].equals("ReplicateOn")){
                            Log.d(TAG,"Inside ReplicateOn");
                            System.out.println(message);

                            Log.d(TAG,"In server socket read: key, value: " + msg[1] + ", " + msg[2]);
                            // osw(msg[1], msg[2]);
                            String key =msg[1];
                            String value = msg[2];
                            osw(key,value);
                            Log.d(TAG,"Called osw in ServerTask");
                            PrintStream ps = new PrintStream(socket.getOutputStream());

                            ps.println("End");
                            ps.flush();
                            //              System.out.println("Sent back End.");
                        }
                        if (message.equals("Deletee")) {
                            String fname = reader.readLine();
                            getContext().deleteFile(fname);
                        }
                        if(message.equals("ProvideMyFiles")){
                            lock=true;
                            String appPath = getContext().getFilesDir().getPath();
                            File path = new File(appPath);
                            File[] files = path.listFiles();
                            String avd = reader.readLine();
                            int num = 0;
                            int count = files.length;

                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();
                                    String temp = findPosition(genHash(filename));

                                    String position = hmap.get(temp);

                                    Log.d("Inside Server",avd+" "+position);
                                    if (avd.equals(position)) {
                                        num++;
                                    }
                                }
                            }
                            PrintStream printer = new PrintStream(socket.getOutputStream());
                            int send= num;
                            printer.println(send);
                            printer.flush();

                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();
                                    String temp = findPosition(genHash(filename));

                                    String position = hmap.get(temp);

                                    Log.d("Inside Server",avd+" "+position);
                                    if(avd.equals(position)) {
                                        String p = callFis(filename);

                                        printer.println(filename + "##" + p);
                                        printer.flush();
                                    }
//                                    else{
//                                        String masg = null;
//                                        printer.println(masg);
//                                        printer.flush();
//                                    }
                                }
                            }

                            printer.println("End");
                            printer.flush();
                            lock=false;
                        }
                        if(message.equals("ProvideYourFiles")){
                            lock=true;
                            String appPath = getContext().getFilesDir().getPath();
                            File path = new File(appPath);
                            File[] files = path.listFiles();
                            String avd = reader.readLine();
                            int num = 0;
                       //     int count = files.length;

                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();
                                    String temp = findPosition(genHash(filename));

                                    String position = hmap.get(temp);

                                    Log.d("Inside Server",avd+" "+position);
                                    if (avd.equals(position)) {
                                        num++;
                                    }
                                }
                            }
                            PrintStream printer = new PrintStream(socket.getOutputStream());
                            int send= num;
                            printer.println(send);
                            printer.flush();
                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();
                                    String temp = findPosition(genHash(filename));

                                    String position = hmap.get(temp);

                                    Log.d("Inside Server",avd+" "+position);
                                    if (avd.equals(position)) {
                                        String p = callFis(filename);

                                        printer.println(filename + "##" + p);
                                        printer.flush();
                                    }
//                                    else{
//                                        Log.d("Onserver","Null part");
//                                        String masg = null;
//                                        printer.println(masg);
//                                        printer.flush();
//                                    }
                                }
                            }

                            printer.println("End");
                            printer.flush();
                            lock=false;
                        }

                        if (message.equals("QueryPresent")) {
//                            while (lock){
//                                System.out.println("Waiting for recovery at QueryPresent");
//                                Thread.sleep(200);
//                            }
                            checkLock();
                            String filename = reader.readLine();
                            try {
                                Log.d("TAG", "filename in QueryPresent: " + filename);
                            //                            FileInputStream fis = null;
                            //                            String str = null;
                            //                            try {
                            //                                fis = getContext().openFileInput(filename);
                            //                            } catch (FileNotFoundException e) {
                            //                                e.printStackTrace();
                            //                            }
                            //                            InputStreamReader isr = new InputStreamReader(fis);
                            //                            BufferedReader bufferedReader = new BufferedReader(isr);
                            //                            try {
                            //                                str =  bufferedReader.readLine();
                            //                                bufferedReader.close();
                            //                            } catch (IOException e) {
                            //                                e.printStackTrace();
                            //                            }
                                String str ;
                                if((str= callFis(filename))!=null){
                                    PrintStream printer = new PrintStream(socket.getOutputStream());
                                    String ret = filename + ":::" + str;
                                    printer.println(ret);
                                    printer.flush();

                                    printer.println("End");
                                    printer.flush();
                                }

                            }
                            catch(Exception e){
                                e.printStackTrace();
                            }
                        }

                        if (message.equals("All")) {
                            String appPath = getContext().getFilesDir().getPath();
                            File path = new File(appPath);
                            File[] files = path.listFiles();
                            PrintStream printer = new PrintStream(socket.getOutputStream());
//                            String msgo = "Hello";
//                            printer.println(msgo);
                            printer.flush();
                            for (File file: files) {
                                if (file.isFile()) {
                                    String filename = file.getName();
                                    String p =  callFis(filename);

                                    printer.println(filename + "##" + p);
                                    printer.flush();
                                }
                            }
                            printer.close();

                        }

                    }
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, String> {


        @Override
        protected String doInBackground(String... strings) {
            try{
                //        System.out.println("string[0]: "+strings[0]+", strings[1]: "+strings[1]);


                if (strings[0].equals("getFromreplicas")) {
                    lock=true;
                    int port = Integer.parseInt(strings[1]);
                    Socket socket = createSocket(port*2);
                    String msg = "ProvideMyFiles";
                    PrintStream ps = null;
                    try {
                        ps = new PrintStream(socket.getOutputStream());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    ps.println(msg);
                    ps.flush();
                    Thread.sleep(200);

                    ps.println(myavd);
                    ps.flush();
                    Log.d("inClient","sending "+ myavd);
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());   //Read over the socket
                    BufferedReader reader = new BufferedReader(input);
                    int count = Integer.parseInt(reader.readLine());
                    Log.d(TAG, "count: "+count);

                    for (int i = 0; i < count; i++) {

                        String recv = reader.readLine();
                        if(recv!=null) {
                            Log.d(TAG,"recv: " + recv);
                            String[] recvarr = recv.split("##");
                            String fname = recvarr[0];
                            String p_val = recvarr[1];
                            osw(fname, p_val);
                        }
                    }

                    if(reader.readLine().equals("End")){
                        socket.close();
                    }
                    lock=false;



                }
                if (strings[0].equals("plsReplicateAgain")) {

                    lock= true;
                    int port = Integer.parseInt(strings[1]);
                    Socket socket = createSocket(port*2);
                    String msg = "ProvideYourFiles";
                    PrintStream ps = null;
                    try {
                        ps = new PrintStream(socket.getOutputStream());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    ps.println(msg);
                    ps.flush();
                    Thread.sleep(200);
                    ps.println(strings[1]);

                    Log.d("inClient","sending "+ strings[1]);
                    ps.flush();
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());   //Read over the socket
                    BufferedReader reader = new BufferedReader(input);
                    int count = Integer.parseInt(reader.readLine());
                    Log.d(TAG, "count: "+count);
                    for (int i = 0; i < count; i++) {
                        String recv = reader.readLine();

                        if(recv != null) {
                            Log.d(TAG,"recv: " + recv);
                            String[] recvarr = recv.split("##");
                            String fname = recvarr[0];
                            String p_val = recvarr[1];
                            osw(fname, p_val);
                        }
                    }

                    if(reader.readLine().equals("End")){
                        socket.close();
                    }
                    lock=false;

                }



                if (strings[0].equals("replicate")){
//                    while(lock){
//                        System.out.println("Waiting for recovery to finish at QueryPresent");
//                        Thread.sleep(200);
//                    }
                    String ports[] = strings[1].split(":::");
                    Log.d(TAG,"ports[]"+ports[0]+ports[1]+ports[2]);
                    String next = ports[0];
                    String key = ports[1];
                    String value = ports[2];

                    int nextPort = Integer.parseInt(next);

                    String msg = "ReplicateOn";

                    Socket socket = createSocket(nextPort*2);
                    PrintStream ps = null;
                    try {
                        ps = new PrintStream(socket.getOutputStream());
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    String reply = msg + ":::" + key + ":::" + value;
                    ps.println(reply);
                    ps.flush();

                    Thread.sleep(   100);

                    //                System.out.println("Send message, key, value:" + msg + ", " +key + ", " + value + " to port: "+nextPort*2);
                    InputStreamReader input = new InputStreamReader(socket.getInputStream());
                    BufferedReader reader = new BufferedReader(input);
                    String message = reader.readLine();
                    if (message != null){

                        if (message.equals("End")) {
                            Log.d(TAG,"socket closed of port: "+nextPort*2);
                            socket.close();
                        }
                    }
                    reader.close();
                }
                if(strings[0].equals("Star")) {

                    ports.remove(myavd);

                    for(int i=0;i<ports.size();i++){
                        Log.d("TAG","ports at i: " + i + " " + ports.get(i));

                    }
                            String toreturn="firsttime";
                    for(int j = 0; j < ports.size(); j++) {
                        try {
                        Log.d(TAG, "In" + myavd + ", ports[j] is: " + j + " " + ports.get(j));
                        int port = Integer.parseInt(ports.get(j)) * 2;

                        Socket socket = createSocket(port);


                        PrintStream ps = null;

                            ps = new PrintStream(socket.getOutputStream());

                        String msg = "All";
                        ps.println(msg);
                        ps.flush();
                        Thread.sleep(100);

                        InputStreamReader input = new InputStreamReader(socket.getInputStream());
                        BufferedReader reader = new BufferedReader(input);
                            String salutation;
//                            if ((salutation = reader.readLine()) != null && socket.isConnected()) {
//                                //                            String recv = null;
                               // Log.d(TAG, salutation);
                                while ((salutation = reader.readLine()) != null) {
                                    //String recv = reader.readLine();
                                    Log.d(TAG, salutation);
                                    String[] recvarr = salutation.split("##");
                                    String fname = recvarr[0];
                                    String p_val = recvarr[1];
                                    if (toreturn.equals("firsttime")) {
                                        toreturn = fname + "##" + p_val + "-";
                                    } else {
                                        toreturn = toreturn + fname + "##" + p_val + "-";
                                    }

                                }

//
//                        } else {
//                                continue;
//                            }

                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }//
                    }
                        System.out.println("To return " + toreturn);
                        return toreturn;

                }

                if(strings[0].equals("Chknext")) {
                    try {
//                        while (lock) {
//                            System.out.println("Waiting for recovery to finish at QueryPresent");
//                            Thread.sleep(200);
//                        }
                        checkLock();
                        String first_rep = null;
                        String second_rep = null;
                        String third_rep = null;
                        String fourth_rep = null;


                        for (String[] array : list) {
                            if (myavd.equals(array[0])) {
                                third_rep = array[1];
                                fourth_rep = array[2];

                            }
                        }
                        for (String[] array : list) {
                            if (myavd.equals(array[2])) {
                                first_rep = array[0];
                                second_rep = array[1];

                            }
                        }
                        String msg_query = strings[1].trim();                                                         //Message to be sent
                        String[] msg_query_array = msg_query.split(":::");
                        String avd = msg_query_array[1];
                        String key = msg_query_array[2];

                        String val = dothis(avd, key);
//                        String chk[] = val.split(":::");
//                        String check = chk[0];
//                        Log.d(TAG ,"In client, chknext, check value is: "+check);
                        if (val != null) {
                            return val;
                        } else {
                            String value = dothis(first_rep, key);
//                            String chk1[] = value.split(":::");
//                            String check1 = chk1[0];
                            Log.d(TAG, "In client, chknext, check1 value is: " + value);

                            if (value != null) {
                                return value;
                            } else {

                                String values = dothis(second_rep, key);
//                                String chk2[] = values.split(":::");
//                                String check2 = chk2[0];
                                Log.d(TAG, "In client, chknext, check2 value is: " + values);

                                if (values != null) {
                                    return values;
                                } else {
                                    String valuess = dothis(third_rep, key);
//                            String chk1[] = value.split(":::");
//                            String check1 = chk1[0];
                                    Log.d(TAG, "In client, chknext, check3 value is: " + valuess);

                                    if (valuess != null) {
                                        return valuess;
                                    } else {
                                        String valuesss = dothis(fourth_rep, key);
//                            String chk1[] = value.split(":::");
//                            String check1 = chk1[0];
                                        Log.d(TAG, "In client, chknext, check1 value is: " + valuesss);

                                        if (valuesss != null) {
                                            return valuesss;
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
                if(strings[0].equals("Delete"))
                {
                    String[] avd_fname = strings[1].split("##");
                    String avd = avd_fname[0];
                    String file = avd_fname[1];
                    Socket client = createSocket(Integer.parseInt(avd)*2);

                    PrintStream psc = new PrintStream(client.getOutputStream());
                    String msg_client = "Deletee";
                    psc.println(msg_client);
                    psc.flush();
                    psc.println(file);
                    psc.flush();
                }




            } catch (Exception e){
                e.printStackTrace();

            }

            return null;
        }
    }
    public String dothis(String  avd, String key) {
        String fname = null;
        String p_val = null;
        String finalval = null;
        try {
            Log.e(TAG, "In Chknext");
            int QueryPort = Integer.parseInt(avd) * 2;

            Socket Queryclient = createSocket(QueryPort);
            PrintStream psq = null;

            psq = new PrintStream(Queryclient.getOutputStream());
            String query_client = "QueryPresent";
            psq.println(query_client);
            psq.flush();
            psq.println(key);
            psq.flush();
            Thread.sleep(100);
            InputStreamReader input = new InputStreamReader(Queryclient.getInputStream());   //Read over the socket
            BufferedReader reader = new BufferedReader(input);
            String query_return ;
            if ((query_return= reader.readLine()) != null) {
                String[] query_array = query_return.split(":::");
                fname = query_array[0];
                p_val = query_array[1];
                Log.d(TAG, "in Chknext fname: " + fname + ", p_val: " + p_val);
                String message = reader.readLine();
                if (message != null) {

                    if (message.equals("End")) {
                        Log.d(TAG, "socket closed of port: ");
                        Queryclient.close();
                    }
                }
                reader.close();
                finalval =  fname + ":::" + p_val;
            }
            else
            {
                Log.d(TAG,"In else in dothis.");
                finalval = null;
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return finalval;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    public void osw(String key, String value) {
        OutputStreamWriter outputStreamWriter = null;
        //      System.out.println("Inside OSW.");
        try {
            outputStreamWriter = new OutputStreamWriter(getContext().openFileOutput(key, Context.MODE_PRIVATE));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            outputStreamWriter.write(value);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            outputStreamWriter.close();
            //         System.out.println("outputStreamWriter closed in OSW.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String findPosition (String current_hash) {
        String node_Hash = null;
        for (int i = 0; i < hashList.size(); i++) {
            String locHash = hashList.get(i);


            if (locHash.compareTo(current_hash) >= 0) {
                node_Hash = locHash;
                break;

            } else {
                if (i == (hashList.size() - 1)) {
                    node_Hash = hashList.get(0);
                }
            }

        }
        return node_Hash;
    }


    public String getmyPort(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        return myPort;
    }

    public String getmyavd(){
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return portStr;
    }
//    public void createClientTask(String msg0, String msg1){
//        System.out.println("Inside create Client task.");
//        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg0,msg1);
//    }

    public String callFis(String filename){
        FileInputStream fis = null;
        String str = null;
        try {
            fis = getContext().openFileInput(filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        InputStreamReader isr = new InputStreamReader(fis);
        BufferedReader bufferedReader = new BufferedReader(isr);
        try {
            str =  bufferedReader.readLine();
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    public Socket createSocket (int port) {
        Socket socket = null;
//            while(!socket.isConnected()) {
        byte[] ipAddr = new byte[]{10, 0, 2, 2};
        InetAddress addr = null;
        Log.d(TAG, "Created socket on " + port);
        try {
            addr = InetAddress.getByAddress(ipAddr);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        try {
            socket = new Socket(addr, port);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //          }
        return socket;
    }
//    public String createClientT(String msg0, String msg1){
//        String ret = null;
//        try {
//            ret =  new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg0,msg1).get();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        } catch (ExecutionException e) {
//            e.printStackTrace();
//        }
//        return ret;
//    }

}

