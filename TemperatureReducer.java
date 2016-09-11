

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TemperatureReducer extends Reducer<Text, Text, Text, Text> {
	
	@Override public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
        String minlat = "";
        String minlon = "";
        String maxlat = "";
        String maxlon = "";
     	int maxtemp = Integer.MIN_VALUE;
		int mintemp = Integer.MAX_VALUE;
		Text output= new Text();
		

		for (Text value : values) {
			String[] splitValue = value.toString().split("_");
			if (Integer.parseInt(splitValue[2])>maxtemp) {	
				maxtemp = Integer.parseInt(splitValue[2]);
				maxlat = splitValue[0];
				maxlon = splitValue[1];
			}

			if (Integer.parseInt(splitValue[2])<mintemp) {	
				mintemp = Integer.parseInt(splitValue[2]);
				minlat = splitValue[0];
				minlon = splitValue[1];
			}
			
			output.set(minlat+"_"+minlon+"_"+mintemp+"_"+maxlat+"_"+maxlon+"_"+maxtemp);
			
		}
		
		
		

		context.write(key, output); 
		
	}
}


