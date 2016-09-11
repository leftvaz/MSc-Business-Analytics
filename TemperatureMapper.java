import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final int MISSING = 9999;
@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		String year = line.substring(15, 19);
		
		int temp;
			if (line.charAt(87) == '+') { // parseInt doesn't like leading plus signs
			temp = Integer.parseInt(line.substring(88, 92));
		} 
		else {
			temp = Integer.parseInt(line.substring(87, 92));
			}
		String temperature = String.valueOf(temp);

		
		
		String quality = line.substring(92, 93);
		
		
		String lat = line.substring(29, 34);
		String lon = line.substring(35, 41);
		Text outputKey = new Text();
		outputKey.set(year);
		Text outputValue = new Text();
		outputValue.set(lat+"_"+lon+"_"+temperature);

		if (temp != MISSING && quality.matches("[01459]")) {
		context.write(outputKey, outputValue); //CHANGE TO PROPER OUTPUT
		}
	}
}




