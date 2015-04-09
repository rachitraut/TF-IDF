package mapdemo;


import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 

public class Reducer3 extends Reducer<Text, Text, Text, Text> {
 
	private static final DecimalFormat DF = new DecimalFormat("###.########");

    private Text wordAtDocument = new Text();

    private Text tfidfCounts = new Text();

    public Reducer3() {
    }

    
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
            InterruptedException {

        
        int numDocs = context.getConfiguration().getInt("numberOfDocsInCorpus", 0);
        
        int keyAppears = 0;
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        for (Text val : values) {
            String[] temp = val.toString().split("=");
            
            if (Integer.parseInt(temp[1].split("/")[0]) > 0) {
                keyAppears++;
            }
            tempFrequencies.put(temp[0], temp[1]);
        }
        for (String document : tempFrequencies.keySet()) {
            String[] temp1 = tempFrequencies.get(document).split("/");

            
            double tf = Double.valueOf(Double.valueOf(temp1[0])
                    / Double.valueOf(temp1[1]));

            
            double idf = Math.log10((double) numDocs / 
               (double) ((keyAppears == 0 ? 1 : 0) + 
                     keyAppears));

            double tfIdf = tf * idf;

            this.wordAtDocument.set(key + "@" + document);
            this.tfidfCounts.set("[" + keyAppears + "/"
                    + numDocs + " , " + temp1[0] + "/"
                    + temp1[1] + " , " + DF.format(tfIdf) + "]");

            context.write(this.wordAtDocument, this.tfidfCounts);
        }
    }
}
