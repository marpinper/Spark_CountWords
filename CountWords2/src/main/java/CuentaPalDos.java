import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
/**
 * Created by pilarpineiro on 20/4/16.
 */
public class CuentaPalDos {

    public static void main(String[] args) {
        if(args.length!=1){
            System.out.println("Error:expected one argument");
            throw new RuntimeException();
        }

        SparkConf sparkconf= new SparkConf().setAppName("CuentaPalDos");

        JavaSparkContext sparkContext= new JavaSparkContext(sparkconf);
        JavaRDD<String> lines= sparkContext.textFile(args[0]);
//variante de map que se llama flatMap, pero cada elemento se puede asociar a 0 o mas elemntos de salida.

        JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String,String>());//separamos por palabras

        // JavaRDD<String> words= lines.flatMap((s)->{return Arrays.asList(s.split(" "));}); otra manera d poner lo siguient
        public Iterable<String> call (String s) throws Exception{// m genera array con elementtos separados por espacio
            return Arrays.asList(s.split(" "));//lista con lo que quiera añadirle
        }
        //en words tendre todas las palabras , en lugar de lineas.


     // quiero crear par clave valor:<palabra,1>
        JavaPairRDD<String,Integer> pairs= words.mapToPair {//(s)->return Tuple2<String,Integer>(s,1);});
            new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return Tuple2 < String,Integer >(s,1);
                }


            }
        }

JavaPairRDD<String,Integer> counts = pairs.reduceByKey(// (integer,integer2)->{return integer + integer2}
        new Function2<Integer,Integer,Integer>(){
            public Integer call (Integer integer, Integer integer2) throws Exception{
                return integer+integer2;
            }
        }
);

// queremos ordenar:
        JavaPairRDD<String,Integer> orderedPairs= counts.sortByKey();

        List<Tuple2<String,Integer>> output= orderedPairs.collect(); //cuidado con esto, podria petar la memoria. mejor .take(100);


        //MPRIMIR:

        for(Tuple2<?,?> tuple: output){
            System.outprintln(tuple._1()+ " "+ tuple._2());
        }




    }

}
