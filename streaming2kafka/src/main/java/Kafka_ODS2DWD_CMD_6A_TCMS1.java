import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.tk.entity.Txd;
import com.tk.utils.getProperties;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

public class Kafka_ODS2DWD_CMD_6A_TCMS1 {
    /**
     * 涉及表：
     *      LKJ\北斗，    kafka传过来
     *      b_car_group_list ，b_train_list 本地建表
     * 	涉及kafkatopic
     *
     * 	关联关系
     * 	    b_car_group_list.s_train_id ，b_train_list.s_train_id    得到car_train
     * 	    LKJ和北斗、car_train关联
     * 	目标表
     * @param args
     */
    public static void main(String[] args) throws IOException, AnalysisException, InterruptedException {
//        SparkSession ss = SparkSession.builder().getOrCreate();

        SparkConf sparkConf = new SparkConf().setAppName("test").setMaster("local[4]");
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        // SparkContext sparkContext = new SparkContext(sparkConf);
        //JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        sparkConf.set("spark.driver.allowMultipleContexts","true");
       // SparkSession ss = new SparkSession(sparkContext);
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,new Duration(5000));

        // kafka配置
        HashMap<String, String> properties = getProperties.getProperties("D:\\GXWorkspace\\tk_project\\streaming2kafka\\src\\main\\resources\\guoxin.properties");
        System.err.println(properties);

        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", properties.get("kafka_bootstrap"));
        //kafkaParams.put("auto.offset.reset","latest");
        kafkaParams.put("startingOffsets",properties.get("kafka_offset"));
        kafkaParams.put("group.id", properties.get("kafka_group"));
        String[] kafka_topics = properties.get("kafka_topics").split(",");
        Set<String> topicSet = new HashSet<String>(Arrays.asList(kafka_topics));//Arrays.asList(kafka_topics)
        //topicSet.add("CMD_LKJ_ITF_TAX");

        /**
         * 消费者   ETL
         */
        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicSet);

        //directStream.print();

        JavaDStream<String> map = directStream.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                String key = tuple2._1;
                String topic = key.split("_2")[0];
                String value = tuple2._2;
                //System.out.println("topic: " + topic + "  key: " + key + "  value: " + value);

                // JSONObject jsonObject = new JSONObject();
               // System.out.println(tuple2._2);
                //Txd txd1 = JSONObject.parseObject(tuple2._2, Txd.class);
               // System.out.println(txd1);

                return tuple2._1 + "`" + tuple2._2;
            }
        });
//        JavaDStream<Txd> map = directStream.map(new Function<Tuple2<String, String>, Txd>() {
//            @Override
//            public Txd call(Tuple2<String, String> tuple2) throws Exception {
//                String key = tuple2._1;
//                String topic = key.split("_2")[0];
//                String value = tuple2._2;
//                System.out.println("topic: " + topic + "  key: " + key + "  value: " + value);
//
//               // JSONObject jsonObject = new JSONObject();
//                System.out.println(tuple2._2);
//                Txd txd1 = JSONObject.parseObject(tuple2._2, Txd.class);
//                System.out.println(txd1);
//
//                return txd1;
//            }
//        });
        System.err.println("------------------------------------------     value start    ------------------------------------");
        map.print();
        System.err.println("------------------------------------------     value end    ------------------------------------");

        HashMap<String,String> topic1 = new HashMap<>();

        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        while (stringIterator.hasNext()){
                            String[] strArr = stringIterator.next().split("`");
                            topic1.put(strArr[0],strArr[1]);
                        }
                    }
                });
            }
        });


        Set<Map.Entry<String, String>> entries = topic1.entrySet();
        for (Map.Entry<String, String> entry : entries) {
            System.out.println("topicKey: " + entry.getKey() + "   topicValue: " + entry.getValue());
            System.out.println("-------------------------------------");
        }

        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            @Override
            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                Dataset<Row> dataset = sparkSession.read().json(stringJavaRDD);
                dataset.toDF();
            }

        });
//        map.foreachRDD(new VoidFunction<JavaRDD<Txd>>() {
//            @Override
//            public void call(JavaRDD<Txd> txdJavaRDD) throws Exception {
//                Dataset<Row> dataFrame = sparkSession.createDataFrame(txdJavaRDD, Txd.class);
//                //dataFrame.select("*");
//
////                dataFrame.select("*").coalesce(1).write()
////                        .format("kafka")
////                        .option("bootstrap.servers",properties.get("kafka_bootstrap"))
////                        .option("startingOffsets",properties.get("kafka_offset"))
////                        .option("group.id", properties.get("kafka_group"))
////                        .option("topic","5T_TCDS_B_FACTORY_DICT").save();
//                Dataset<Row> select = dataFrame.select("*");
//                select.show();
//                select.write()
//                        .format("kafka").mode(SaveMode.Append)
//                        .option("kafka.bootstrap.servers","hdp1:6667")
//                        .option("topic","5T_TCDS_B_MON_OBJ_DICT")
//                        .option("checkpointLocation","./checkpoint").save();
//
//            }
//        });



        /**
         * ETL
         */
//        JavaDStream<Txd> map1 = map.map(data -> json2class(data.split("`")[1]));
//        System.out.println(map1);
//        map1.foreachRDD(new VoidFunction<JavaRDD<Txd>>() {
//            @Override
//            public void call(JavaRDD<Txd> txdJavaRDD) throws Exception {
//
////                Gson gson = new Gson();
////                Txd txd = gson.fromJson(txdJavaRDD.toString(), Txd.class);
////                System.out.println(txd);
//
//                JSONObject jsonObject = new JSONObject();
//                System.out.println(txdJavaRDD.toString());
//                JSONArray jsonArray = jsonObject.getJSONArray(txdJavaRDD.toString());
//                for (int i = 0;i < jsonArray.size();i++){
//                    Object obj = jsonArray.get(i);
//                    JSON json = JSON.parseObject(obj.toString());
//                    Txd txd = JSON.parseObject(String.valueOf(json), Txd.class);
//                    System.out.println(txd);
//                }
//            }
//        });
//        SQLContext sqlContext = new SQLContext(javaSparkContext);
//        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                JSONObject jsonObject = new JSONObject();
//                System.out.println(stringJavaRDD);
//                JSONArray jsonArray = jsonObject.getJSONArray(stringJavaRDD.toString());
////                for (int i = 0;i < jsonArray.size();i++){
////                    Object obj = jsonArray.get(i);
////                    JSON json = JSON.parseObject(obj.toString());
////                    Txd txd = JSON.parseObject(String.valueOf(json), Txd.class);
////                    System.out.println(txd);
////
////
////                }
//                Dataset<Row> json1 = sqlContext.read().json(stringJavaRDD);
//
//
//            }
//        });


        /**
         * 生产者
         */
//        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                Dataset<Row> json = sparkSession.read().json(stringJavaRDD);
//                //json.toDF().createGlobalTempView("test111");
//                for (String column : json.toDF().columns()) {
//                    System.out.println(column);
//                }
//                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
//                    @Override
//                    public void call(Iterator<String> stringIterator) throws Exception {
//
//                        Properties properties1 = new Properties();
//                        properties1.setProperty("bootstrap.servers", properties.get("kafka_bootstrap"));
//                        properties1.setProperty("startingOffsets",properties.get("kafka_offset"));
//                        properties1.setProperty("group.id", properties.get("kafka_group"));
//                        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties1,new StringSerializer(),new StringSerializer());
//                        while (stringIterator.hasNext()) {
//                            String[] dataArr = stringIterator.next().split("`");
//                            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("JMIS_JCBM_T_ZBGL_JT6", dataArr[0],dataArr[1]);
//                            kafkaProducer.send(producerRecord);
//                            System.out.println("写入数据成功！key：" + dataArr[0]);
//                        }
//                    }
//                });
//            }
//        });
//        SQLContext sqlContext = sparkSession.sqlContext();
//        sqlContext.sql("select * from test111").toDF().show();
//        map.foreachRDD(new VoidFunction<JavaRDD<String>>() {
//            @Override
//            public void call(JavaRDD<String> stringJavaRDD) throws Exception {
//                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
//                    @Override
//                    public void call(Iterator<String> stringIterator) throws Exception {
//
//                        KafkaProducer kafkaProducer = KafkaProducer.getInstance(properties.get("kafka_bootstrap"));
//
//                        ArrayList<KeyedMessage<String,String>> arrayList = Lists.newArrayList();
//                        while (stringIterator.hasNext()){
//                            arrayList.add(new KeyedMessage<String,String>("JMIS_JCBM_T_ZBGL_JT6",stringIterator.next()));
//                        }
//                        kafkaProducer.send(arrayList);
//
//                    }
//                });
//            }
//        });

//
//        map.foreachRDD(new VoidFunction<JavaRDD<Txd>>() {
//            @Override
//            public void call(JavaRDD<Txd> stringJavaRDD) throws Exception {
//                Dataset<Row> json = sparkSession.read().json(stringJavaRDD.toString());
//             //   System.err.println("*************************   json start  ***********************");
//                System.out.println(json);
//               // System.err.println("*************************   json end  ***********************");
//                json.toDF().createGlobalTempView("m_rt_param_data");
//                sparkSession.sql("select * from  m_rt_param_data ").coalesce(1).show();
//            }
//        });

        ssc.start();
        ssc.awaitTermination();

    }

    public static Txd json2class(String jsonStr){
        Gson gson = new Gson();
        return gson.fromJson(jsonStr,Txd.class);
    }


    public static String toJsonString(Dataset<Row> rowDataset) {
        if (rowDataset == null) {
            return "";
        }
        JSONArray jsonArray = new JSONArray();
        Dataset<String> stringDataset = rowDataset.toJSON();
        if (stringDataset == null){
            return "";
        }
        stringDataset.show();
        List<String> stringList = stringDataset.collectAsList();
        for (String jsonStr : stringList) {
            JSONObject jsonObject = JSONObject.parseObject(jsonStr);
            jsonArray.add(jsonObject);
        }
        return jsonArray.toString();
    }

}
