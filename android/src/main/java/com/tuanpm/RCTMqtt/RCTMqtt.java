package com.tuanpm.RCTMqtt;

import androidx.annotation.NonNull;

import android.util.Base64;
import android.util.Log;
import android.content.res.AssetFileDescriptor;

import com.facebook.react.bridge.Arguments;
import com.facebook.react.bridge.ReactApplicationContext;
import com.facebook.react.bridge.ReactContext;
import com.facebook.react.bridge.ReadableMap;
import com.facebook.react.bridge.ReadableArray;
import com.facebook.react.bridge.WritableMap;
import com.facebook.react.bridge.WritableArray;
import com.facebook.react.bridge.WritableNativeMap;
import com.facebook.react.bridge.WritableNativeArray;
import com.facebook.react.modules.core.RCTNativeAppEventEmitter;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;



import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.ByteArrayInputStream;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

public class RCTMqtt implements MqttCallbackExtended {
    private static final String TAG = "RCTMqttModule";
    private final ReactApplicationContext reactContext;
    private final WritableMap defaultOptions;
    private final String clientRef;
    private MqttAsyncClient client;
    private MemoryPersistence memPer;
    private MqttConnectOptions mqttOptions;
    private Map<String, Integer> topics = new HashMap<>();

    public RCTMqtt(@NonNull final String ref, final ReactApplicationContext reactContext, final ReadableMap options) {
        clientRef = ref;
        this.reactContext = reactContext;
        defaultOptions = new WritableNativeMap();
        defaultOptions.putString("host", "localhost");
        defaultOptions.putInt("port", 1883);
        defaultOptions.putString("protocol", "tcp");
        defaultOptions.putBoolean("tls", false);
        defaultOptions.putInt("keepalive", 60);
        defaultOptions.putString("clientId", "react-native-mqtt");
        defaultOptions.putInt("protocolLevel", 4);
        defaultOptions.putBoolean("clean", true);
        defaultOptions.putBoolean("auth", false);
        defaultOptions.putString("user", "");
        defaultOptions.putString("pass", "");
        defaultOptions.putBoolean("will", false);
        defaultOptions.putString("willMsg", "");
        defaultOptions.putString("willtopic", "");
        defaultOptions.putInt("willQos", 0);
        defaultOptions.putBoolean("willRetainFlag", false);
        defaultOptions.putBoolean("automaticReconnect", false);
        defaultOptions.putString("certificate", "");
        defaultOptions.putString("certificatePass","");
        defaultOptions.putString("ca", "");

        createClient(options);
    }

    private void createClient(@NonNull final ReadableMap params) {
        if (params.hasKey("host")) {
            defaultOptions.putString("host", params.getString("host"));
        }
        if (params.hasKey("port")) {
            defaultOptions.putInt("port", params.getInt("port"));
        }
        if (params.hasKey("protocol")) {
            defaultOptions.putString("protocol", params.getString("protocol"));
        }
        if (params.hasKey("tls")) {
            defaultOptions.putBoolean("tls", params.getBoolean("tls"));
        }
        if (params.hasKey("keepalive")) {
            defaultOptions.putInt("keepalive", params.getInt("keepalive"));
        }
        if (params.hasKey("clientId")) {
            defaultOptions.putString("clientId", params.getString("clientId"));
        }
        if (params.hasKey("protocolLevel")) {
            defaultOptions.putInt("protocolLevel", params.getInt("protocolLevel"));
        }
        if (params.hasKey("clean")) {
            defaultOptions.putBoolean("clean", params.getBoolean("clean"));
        }
        if (params.hasKey("auth")) {
            defaultOptions.putBoolean("auth", params.getBoolean("auth"));
        }
        if (params.hasKey("user")) {
            defaultOptions.putString("user", params.getString("user"));
        }
        if (params.hasKey("pass")) {
            defaultOptions.putString("pass", params.getString("pass"));
        }
        if (params.hasKey("will")) {
            defaultOptions.putBoolean("will", params.getBoolean("will"));
        }
        if (params.hasKey("protocolLevel")) {
            defaultOptions.putInt("protocolLevel", params.getInt("protocolLevel"));
        }
        if (params.hasKey("will")) {
            defaultOptions.putBoolean("will", params.getBoolean("will"));
        }
        if (params.hasKey("willMsg")) {
            defaultOptions.putString("willMsg", params.getString("willMsg"));
        }
        if (params.hasKey("willtopic")) {
            defaultOptions.putString("willtopic", params.getString("willtopic"));
        }
        if (params.hasKey("willQos")) {
            defaultOptions.putInt("willQos", params.getInt("willQos"));
        }
        if (params.hasKey("willRetainFlag")) {
            defaultOptions.putBoolean("willRetainFlag", params.getBoolean("willRetainFlag"));
        }
        if (params.hasKey("automaticReconnect")) {
            defaultOptions.putBoolean("automaticReconnect", params.getBoolean("automaticReconnect"));
        }

        if (params.hasKey("certificate")) {
            defaultOptions.putString("certificate", params.getString("certificate"));
        }
        if (params.hasKey("certificatePass")) {
            defaultOptions.putString("certificatePass", params.getString("certificatePass"));
        }
        if (params.hasKey("ca")) {
            defaultOptions.putString("ca", params.getString("ca"));
        }

        ReadableMap options = defaultOptions;

        // Set this wrapper as the callback handler

        mqttOptions = new MqttConnectOptions();

        if (options.getInt("protocolLevel") == 3) {
            mqttOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1);
        }

        mqttOptions.setKeepAliveInterval(options.getInt("keepalive"));
        mqttOptions.setMaxInflight(1000);
        mqttOptions.setConnectionTimeout(10);

        StringBuilder uri = new StringBuilder("tcp://");
        if (options.getBoolean("tls")) {
            uri = new StringBuilder("ssl://");
            String certificateBase64 = options.getString("certificate");
            String caBase64 = options.getString("ca");

            if(certificateBase64.length() > 0 && caBase64.length() > 0) {
                try {

                    KeyStore clientStore = KeyStore.getInstance("PKCS12");

                    byte[] encodedCert = android.util.Base64.decode(certificateBase64, Base64.DEFAULT);
                    ByteArrayInputStream isCertificate  =  new ByteArrayInputStream(encodedCert);
                    clientStore.load(isCertificate, options.getString("certificatePass").toCharArray());

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(clientStore, "testPass".toCharArray());
                    KeyManager[] kms = kmf.getKeyManagers();

                    byte[] encodedCA = android.util.Base64.decode(caBase64, Base64.DEFAULT);
                    ByteArrayInputStream isCA  =  new ByteArrayInputStream(encodedCA);
                    var cf = CertificateFactory.getInstance("X.509");
                    var ca = cf.generateCertificate(isCA);

                    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
                    trustStore.load(null,null);
                    trustStore.setCertificateEntry("ca", ca);

                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(trustStore);
                    TrustManager[] tms = tmf.getTrustManagers();

                    SSLContext sslContext = null;
                    sslContext = SSLContext.getInstance("TLS");
                    sslContext.init(kms, tms, new SecureRandom());
                    mqttOptions.setSocketFactory(sslContext.getSocketFactory());

                } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                try {
                /*
                 * http://stackoverflow.com/questions/3761737/https-get-ssl-with-android-and-
                 * self-signed-server-certificate
                 *
                 * WARNING: for anybody else arriving at this answer, this is a dirty, horrible
                 * hack and you must not use it for anything that matters. SSL/TLS without
                 * authentication is worse than no encryption at all - reading and modifying
                 * your "encrypted" data is trivial for an attacker and you wouldn't even know
                 * it was happening
                 */
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new X509TrustManager[] { new X509TrustManager() {
                    public void checkClientTrusted(X509Certificate[] chain, String authType)
                            throws CertificateException {
                    }

                    public void checkServerTrusted(X509Certificate[] chain, String authType)
                            throws CertificateException {
                    }

                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                } }, new SecureRandom());

                mqttOptions.setSocketFactory(sslContext.getSocketFactory());
            } catch (Exception e) {
                e.printStackTrace();
            }
            }
        }

        uri.append(options.getString("host")).append(":").append(options.getInt("port"));

        if (options.getBoolean("auth")) {
            String user = options.getString("user");
            String pass = options.getString("pass");
            if (user.length() > 0) {
                mqttOptions.setUserName(user);
            }
            if (pass.length() > 0) {
                mqttOptions.setPassword(pass.toCharArray());
            }
        }

        if (options.getBoolean("will")) {
            String topic = options.getString("willtopic");
            log("[ MQTT ] setWill" + topic);
            mqttOptions.setWill(topic, options.getString("willMsg").getBytes(), options.getInt("willQos"),
                    options.getBoolean("willRetainFlag"));
        }
        mqttOptions.setAutomaticReconnect(options.getBoolean("automaticReconnect"));
        memPer = new MemoryPersistence();

        try {
            client = new MqttAsyncClient(uri.toString(), options.getString("clientId"), memPer);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public void setCallback() {
        client.setCallback(this);
    }

    public void reconnect() {
        try {
            log("came here");
            WritableMap params = Arguments.createMap();
            params.putString("event", "reconnecting");
            params.putString("message", "try to reconnect");
            sendEvent(reactContext, "mqtt_events", params);
            client.reconnect();
        } catch (MqttException e) {
            WritableMap params = Arguments.createMap();
            params.putString("event", "error");
            params.putString("message", e.getMessage());
            sendEvent(reactContext, "mqtt_events", params);
        }
    }

    public boolean isConnected() {
        return client.isConnected();
    }
	/*
	 * Returns all connected topics as object
	 */
	public WritableArray getTopics() {
		WritableArray ret = new WritableNativeArray();
		for(Map.Entry<String, Integer> entry : topics.entrySet()) {
			WritableMap tmp = Arguments.createMap();
			tmp.putString("topic", entry.getKey());
			tmp.putInt("qos", entry.getValue());
			ret.pushMap(tmp);
		}
		return ret;
	}

	/*
	 *  Check if listening to a specifc topic
	 */
	public boolean isSubbed(String topic) {
		//log("isSubbed. checking is topic: "+ topic);
		return topics.containsKey(topic);
	}

    public void connect() {
        try {
            WritableMap params = Arguments.createMap();
            params.putString("event", "connecting");
            params.putString("message", "try to connect");
            sendEvent(reactContext, "mqtt_events", params);

            // Connect using a non-blocking connect
            client.connect(mqttOptions, reactContext, new IMqttActionListener() {
                public void onSuccess(IMqttToken asyncActionToken) {
                    // not needed since connectionComplete callback is now implemented
                    // WritableMap params = Arguments.createMap();
                    // params.putString("event", "connect");
                    // params.putString("message", "connected");
                    // sendEvent(reactContext, "mqtt_events", params);
                    // log("Connected");

                    Iterator<String> iterator = topics.keySet().iterator();
                    while (iterator.hasNext()) {
                        final String topic = iterator.next();
                        subscribe(topic, topics.get(topic));
                    }
                }

                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    WritableMap params = Arguments.createMap();
                    params.putString("event", "error");
                    final String errorDescription = new StringBuilder("connection failure ").append(exception)
                            .toString();
                    params.putString("message", errorDescription);
                    sendEvent(reactContext, "mqtt_events", params);
                }
            });
        } catch (MqttException e) {
            WritableMap params = Arguments.createMap();
            params.putString("event", "error");
            params.putString("message", "Can't create connection");
            sendEvent(reactContext, "mqtt_events", params);
        }
    }

    public void disconnect() {
        IMqttActionListener discListener = new IMqttActionListener() {
            public void onSuccess(IMqttToken asyncActionToken) {
                log("Disconnect Completed");
                WritableMap params = Arguments.createMap();
                params.putString("event", "closed");
                params.putString("message", "Disconnect");
                sendEvent(reactContext, "mqtt_events", params);
            }

            public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                log(new StringBuilder("Disconnect failed").append(exception).toString());
            }
        };

        try {
            client.disconnect(reactContext, discListener);
        } catch (MqttException e) {
            log("Disconnect failed ...");
        }
    }

    public void subscribe(@NonNull final String topic, final int qos) {
        try {
            topics.put(topic, qos);
            IMqttToken subToken = client.subscribe(topic, qos);
            subToken.setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    // The message was published
                    log("Subscribe success");
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    // The subscription could not be performed, maybe the user was not
                    // authorized to subscribe on the specified topic e.g. using wildcards
                    log("Subscribe failed");
                }
            });
        } catch (MqttException e) {
            log("Cann't subscribe");
            e.printStackTrace();
        }
    }

    public void unsubscribe(@NonNull final String topic) {
        try {
            if (topics.containsKey(topic)) {
                topics.remove(topic);
            }
            client.unsubscribe(topic).setActionCallback(new IMqttActionListener() {
                @Override
                public void onSuccess(IMqttToken asyncActionToken) {
                    log(new StringBuilder("Subscribed on ").append(topic).toString());
                }

                @Override
                public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
                    log(new StringBuilder("Failed to subscribe on ").append(topic).toString());
                }
            });
        } catch (MqttException e) {
            log(new StringBuilder("Can't unsubscribe").append(" ").append(topic).toString());
            e.printStackTrace();
        }
    }

    /**
     * @param topic
     * @param payload
     * @param qos
     * @param retain
     */
    public void publish(@NonNull final String topic, @NonNull final String payload, final int qos,
            final boolean retain) {
        try {
            byte[] encodedPayload = payload.getBytes("UTF-8");
            MqttMessage message = new MqttMessage(encodedPayload);
            message.setQos(qos);
            message.setRetained(retain);
            client.publish(topic, message);
        } catch (UnsupportedEncodingException | MqttException e) {
            e.printStackTrace();
        }
    }

    /****************************************************************/
    /* Methods to implement the MqttCallback interface */
    /****************************************************************/

    /**
     * @see MqttCallback#connectionLost(Throwable)
     */
    public void connectionLost(Throwable cause) {
        // Called when the connection to the server has been lost.
        // An application may choose to implement reconnection
        // logic at this point. This sample simply exits.
        log(new StringBuilder("Connection lost! ").append(cause).toString());
        WritableMap params = Arguments.createMap();
        params.putString("event", "error");
        final String errorDescription = new StringBuilder("ConnectionLost! ").append(cause).toString();
        params.putString("message", errorDescription);

        if (!(cause instanceof MqttException)) {
            final String notMqttExceptionError = new StringBuilder("Not MqttException ").append(cause).toString();
            log(notMqttExceptionError);
            return;
        }

        final MqttException mqttError = (MqttException) cause;
        if (isInsideWantedCodes(mqttError)) {
            sendEvent(reactContext, "mqtt_events", params);
        }
    }

    /**
     * @see MqttCallback#deliveryComplete(IMqttDeliveryToken)
     */
    public void deliveryComplete(IMqttDeliveryToken token) {
        // Called when a message has been delivered to the
        // server. The token passed in here is the same one
        // that was returned from the original call to publish.
        // This allows applications to perform asynchronous
        // delivery without blocking until delivery completes.
        //
        // This sample demonstrates asynchronous deliver, registering
        // a callback to be notified on each call to publish.
        //
        // The deliveryComplete method will also be called if
        // the callback is set on the client
        //
        // note that token.getTopics() returns an array so we convert to a string
        // before printing it on the console
        log("Delivery complete callback: Publish Completed ");
        WritableMap params = Arguments.createMap();
        params.putString("event", "msgSent");
        params.putString("message", "OK");
        sendEvent(reactContext, "mqtt_events", params);
    }

    /**
     * @see MqttCallback#messageArrived(String, MqttMessage)
     */
    public void messageArrived(@NonNull final String topic, @NonNull final MqttMessage message) throws MqttException {
        // Called when a message arrives from the server that matches any
        // subscription made by the client

        log(new StringBuilder("  Topic:\t").append(topic).append("  Message:\t")
                .append(new String(message.getPayload())).append("  QoS:\t").append(message.getQos()).toString());

        WritableMap data = Arguments.createMap();
        data.putString("topic", topic);
        data.putString("data", new String(message.getPayload()));
        data.putInt("qos", message.getQos());
        data.putBoolean("retain", message.isRetained());

        WritableMap params = Arguments.createMap();
        params.putString("event", "message");
        params.putMap("message", data);
        sendEvent(reactContext, "mqtt_events", params);
    }

    private void sendEvent(final ReactContext reactContext, final String eventName, @Nullable WritableMap params) {
        params.putString("clientRef", this.clientRef);
        reactContext.getJSModule(RCTNativeAppEventEmitter.class).emit(eventName, params);
    }

    private boolean isInsideWantedCodes(@NonNull final MqttException exception) {

        int reasonCode = exception.getReasonCode();
        return reasonCode == MqttException.REASON_CODE_SERVER_CONNECT_ERROR
                || reasonCode == MqttException.REASON_CODE_CLIENT_EXCEPTION
                || reasonCode == MqttException.REASON_CODE_CONNECTION_LOST
                || reasonCode == MqttException.REASON_CODE_CLIENT_TIMEOUT
                || reasonCode == MqttException.REASON_CODE_WRITE_TIMEOUT;
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        Log.d(TAG, String.format("connectComplete. reconnect:%1$b", reconnect));
        WritableMap data = Arguments.createMap();
        data.putBoolean("reconnect", reconnect);
        WritableMap params = Arguments.createMap();
        params.putString("event", "connect");
        params.putMap("message", data);
        sendEvent(reactContext, "mqtt_events", params);
    }

    /**
     * Utility method to handle logging. If 'quietMode' is set, this method does
     * nothing
     *
     * @param message the message to log
     */
    private void log(@NonNull final String message) {
        if (!BuildConfig.DEBUG) {
            return;
        }
        final String tag = new StringBuilder(TAG).append(" ").append(clientRef).toString();
        Log.d(tag, message);
    }
}
