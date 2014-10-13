
package edu.buffalo.cse.cse486586.simpledynamo;

import java.net.ServerSocket;

import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.widget.TextView;
import android.os.AsyncTask;
import android.util.Log;

public class SimpleDynamoActivity extends Activity {

    public static AsyncTask<ServerSocket, String, Void> mServerTaskReference;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dynamo);

        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
    }

    @Override
    protected void onStop() {
        super.onStop();

        try {
            Log.e(Constants.TAG,"serverTaskReference.isCancelled():" + mServerTaskReference.isCancelled() + "\n");
            if (!mServerTaskReference.isCancelled()) {
                mServerTaskReference.cancel(true);
                Log.e(Constants.TAG," Inside IF  : serverTaskReference.isCancelled():" + mServerTaskReference.isCancelled() + "\n");
                
            }
        } catch (Exception e) {
            Log.e(Constants.TAG, "Unable to cancel the server task \n");
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.simple_dynamo, menu);
        return true;
    }

}
