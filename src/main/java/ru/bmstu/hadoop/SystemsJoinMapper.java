public class SystemsJoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        SystemInfo system = new SystemInfo(value);
        context.write(new TextPair(system.getSystemCode().toString(),"0"), new Text(system.toString()));
    }
}