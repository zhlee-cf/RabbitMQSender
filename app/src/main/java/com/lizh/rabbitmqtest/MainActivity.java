package com.lizh.rabbitmqtest;

import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class MainActivity extends AppCompatActivity {

    private Button button;
    private EditText et;
    private static final String queue_name = "my_queue_3";
    private static final boolean durable = true; //消息队列持久化
    private ConnectionFactory factory;
    private static final String EXCHANGE_NAME = "durable_3";
    private TextView tv;
    private SimpleDateFormat ft;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        button = (Button) findViewById(R.id.publish);
        et = (EditText) findViewById(R.id.text);
        tv = (TextView) findViewById(R.id.textView);
        ft = new SimpleDateFormat("HH:mm:ss");
        setupConnectionFactory();
        subscribe();
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                publish();
            }
        });
    }

    private void subscribe() {
        ThreadUtil.runOnBackThread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = factory.newConnection();
                    final Channel channel = connection.createChannel();
                    channel.exchangeDeclare(EXCHANGE_NAME, "fanout", durable);
                    channel.queueDeclare(queue_name, durable, false, false, null);
//                    channel.basicQos(1);
                    channel.queueBind(queue_name, EXCHANGE_NAME, "");
                    Consumer consumer = new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag, Envelope envelope,
                                                   AMQP.BasicProperties properties, byte[] body) throws IOException {
                            String message = new String(body, "UTF-8");
                            MyLog.showLog("收到RabbitMQ推送===::" + message);
                            MyLog.showLog("consumerTag::" + consumerTag);
                            Message msg = handler.obtainMessage();
                            msg.obj = message;
                            handler.sendMessage(msg);
//                            channel.basicAck(envelope.getDeliveryTag(), true);
                        }
                    };
                    channel.basicConsume(queue_name, true, consumer);
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void publish() {
        ThreadUtil.runOnBackThread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = factory.newConnection(); //创建连接
                    Channel channel = connection.createChannel();
                    channel.exchangeDeclare(EXCHANGE_NAME, "fanout", durable);
//                   channel.queueDeclare(queue_name, durable, false, false, null); //声明消息队列，且为可持久化的
                    String message = et.getText().toString().trim();
                    message = "发送时间==" + ft.format(new Date()) + "**" + message;
                    //将队列设置为持久化之后，还需要将消息也设为可持久化的，MessageProperties.PERSISTENT_TEXT_PLAIN
                    channel.basicPublish(EXCHANGE_NAME, "", MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                    System.out.println("Send message:" + message);
                    ThreadUtil.runOnUIThread(new Runnable() {
                        @Override
                        public void run() {
                            et.setText("");
                        }
                    });
                    channel.close();
                    connection.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private void setupConnectionFactory() {
        String uri = "amqp://push.openim.top";
        try {
            factory = new ConnectionFactory();
            factory.setAutomaticRecoveryEnabled(true);
            factory.setUri(uri);
        } catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException e1) {
            e1.printStackTrace();
        }
    }

    private Handler handler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
            super.handleMessage(msg);
            tv.append("" + msg.obj + '\n');
        }
    };
}
