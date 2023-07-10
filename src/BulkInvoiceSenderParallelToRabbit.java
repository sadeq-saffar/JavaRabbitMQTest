import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeoutException;


public class BulkInvoiceSenderParallelToRabbit {
    private static String startFolder = "D:\\HDAactXMLLog2";
    private static String fromFolderName = "14010726";
    private boolean start;
    private int threadCount = 50;
    static Timer savingTimer = new Timer();

    public static void main(String[] args) {

        final BulkInvoiceSenderParallelToRabbit bs = new BulkInvoiceSenderParallelToRabbit();


        Runnable runnable = new Runnable() {
            public void run() {
                bs.connectIvc();
                try {
                    bs.analyse(startFolder);
                } catch (ParserConfigurationException e) {
                } catch (IOException e) {
                }
            }
        };
        Thread t= new Thread(runnable, "Analyze Thread");
        t.start();

        TimerTask tt = new TimerTask() {
            public void run() {
                t.stop();
                System.exit(0);
            }
        };
        savingTimer.schedule(tt, 120000, 10);
    }

    Connection connection;
    Channel channel;
    AMQP.BasicProperties props;
        private String requestQueueName = "ServiceIvc";
    String replyQueueName;
    final String corrId = UUID.randomUUID().toString();
    private void connectIvc(){

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            replyQueueName = channel.queueDeclare().getQueue();

            HashMap<String, Object> headers = new HashMap<>();
            headers.put("app", "irisl.addon.ivc");
            headers.put("Method", "svsBL");
            headers.put("userName", "ngs");
            headers.put("password", "arngs");

            props = new AMQP.BasicProperties.Builder()
                    .correlationId(corrId)
                    .replyTo(replyQueueName).headers(headers)
                    .build();


        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void analyse(String rootPath) throws ParserConfigurationException, IOException{

        File[] list = (new File(rootPath)).listFiles();
        for (final File item:list){
            if(!start && fromFolderName.compareTo(item.getName())==0 && item.isDirectory())
                start = true;
            if(!start) continue;
            if(item.isDirectory()){
                for (int i = 0; i < threadCount; i++) {
                    int finalI = i;
                    Runnable runnable = new Runnable() {
                        public void run() {
                            workDir(item.getPath(), finalI+1);
                        }
                    };
                    Thread t = new Thread(runnable, "Thread -" + i);
                    t.start();
                }

            }
        }

    }


    private void workDir(String pathname, int filter){
        long start;
        long startIvc;
        long endIvc;
        long end;
        long scale = 1000000L;

        try {
            String sessionKey;

            start = System.nanoTime();
//      System.out.println("Start at : " + (start / scale));

//            JAXBContext jc = JAXBContext.newInstance("ngs.ivc.addon.irisl.ws");
//            final Unmarshaller u = jc.createUnmarshaller();
            File[] list = (new File(pathname)).listFiles();
            for (int i = 0; i < list.length; i++) {
                if(i%filter!=0)
                    continue;
                File file = list[i];
                if(!file.isDirectory()){



                    if(file.length()==0) continue;
                    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                    DocumentBuilder db = dbf.newDocumentBuilder();
                    Document doc = db.parse(file);
                    doc.getDocumentElement().normalize();
                    String type = doc.getDocumentElement().getNodeName();
                    if(type.compareTo("SvcBLsData")!=0) continue;


                    if(file.getName().indexOf(".xml", file.getName().length()-5)==-1){
                        continue;
                    }
                    if(file.getName().endsWith("null.xml")){
                        continue;
                    }
                    if(file.length()==0) {
                        continue;
                    }
                    {
                        StringBuilder sb = getFileData(file);

                        channel.basicPublish("", requestQueueName, props, sb.toString().getBytes(StandardCharsets.UTF_8));

                        System.out.println(" [x] Sent '" + doc.getDocumentElement().getNodeName() + "'");

                        final BlockingQueue<String> response = new ArrayBlockingQueue<>(1);

            String ctag = channel.basicConsume(replyQueueName, true, (consumerTag, delivery) -> {
              if (delivery.getProperties().getCorrelationId().equals(corrId)) {
                response.offer(new String(delivery.getBody(), "UTF-8"));
              }
            }, consumerTag -> {
            });
//
            String result = response.take();
            channel.basicCancel(ctag);

                    }


                }
            }

            end = System.nanoTime();

        } catch (ParserConfigurationException e) {
//      e.printStackTrace();
        } catch (IOException e) {
//      e.printStackTrace();
        } catch (SAXException e) {
//      e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private StringBuilder getFileData(File file) throws IOException {
        StringBuilder sb = new StringBuilder();
        char cbuf[] = new char[1024];
        int n;

        FileInputStream is = new FileInputStream(file);
        Reader fr = new InputStreamReader(is,"UTF-8");
        while ((n=fr.read(cbuf))>0){
            sb.append(cbuf, 0, n);
        }
        fr.close();
        return sb;
    }
}
