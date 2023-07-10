import com.rabbitmq.client.*;
import ngs.ivc.addon.irisl.ws.RspBLSave;
import ngs.ivc.addon.irisl.ws.Service;
import ngs.ivc.addon.irisl.ws.ServiceService;
import ngs.ivc.addon.irisl.ws.SvcBLsData;
import org.xml.sax.InputSource;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class BulkInvoiceReceiver {
    Service servicePort;
    private static String IvcLoginName = "ngs";
    private static String IvcPassword = "arngs";

    private static String requestQueueName= "ServiceIvc";
    public static void main(String[] argv) throws Exception {

        BulkInvoiceReceiver receiver = new BulkInvoiceReceiver();
        receiver.connectIvc();
        receiver.start();

    }

    private void connectIvc(){
        try {
//            ServiceService service = new ServiceService(new URL("http://10.9.80.231:8000/ivc/16/serviceBL?wsdl"), QName.valueOf("{http://services.irisl.addon.ivc.ngs/}ServiceService"));
            ServiceService service = new ServiceService(new URL("http://localhost:8000/ivc/16/serviceBL?wsdl"), QName.valueOf("{http://services.irisl.addon.ivc.ngs/}ServiceService"));
            servicePort = service.getServicePort();

        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void start(){
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.queueDeclare(requestQueueName, false, false, false, null);
            channel.queuePurge(requestQueueName);

            channel.basicQos(1);

            System.out.println(" [x] Awaiting RPC requests");

            Object monitor = new Object();
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                        .Builder()
                        .correlationId(delivery.getProperties().getCorrelationId())
                        .build();

                String response = "";

                try {
                    String message = new String(delivery.getBody(), "UTF-8");
//                    int n = Integer.parseInt(message);

//                    System.out.println(" [.] (" + message + ")");

                    response= checkData(message);

                } catch (RuntimeException e) {
                    System.out.println(" [.] " + e.toString());
                } finally {
                    channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                    // RabbitMq consumer worker thread notifies the RPC server owner thread
                    synchronized (monitor) {
                        monitor.notify();
                    }
                }
            };

            channel.basicConsume(requestQueueName, false, deliverCallback, (consumerTag -> { }));
            // Wait and be prepared to consume the message from RPC client.
            while (true) {
                synchronized (monitor) {
                    try {
                        monitor.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

    }


    private String checkData(String n) {
        StringReader reader = new StringReader(n);
        try {
            JAXBContext jc = JAXBContext.newInstance("ngs.ivc.addon.irisl.ws");
            final Unmarshaller u = jc.createUnmarshaller();
            JAXBElement<SvcBLsData> jax = (JAXBElement<SvcBLsData>) u.unmarshal(reader);
            SvcBLsData svcBLsData = jax.getValue();
            saveInvoiceIntoRunnable(svcBLsData);

        } catch (JAXBException e) {
            e.printStackTrace();
        }

        return n;

    }

    private void saveInvoiceIntoRunnable(SvcBLsData svcBLsData){
        try {
            String sessionKey = servicePort.login(IvcLoginName, IvcPassword);

            List<RspBLSave> reps = servicePort.saveBL(svcBLsData, sessionKey);
            servicePort.logout(sessionKey);
            System.out.println(reps.get(0).getInvoiceNo());

        } catch (Exception e) {
            if(e.getMessage().startsWith("Voyage Not Found "))
                System.out.println(e.getMessage().substring(19));
            else {
//        e.printStackTrace();
            }
        }
    }

}
