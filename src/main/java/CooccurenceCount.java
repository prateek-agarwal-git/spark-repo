
//Set appropriate package name
import scala.Tuple2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import  static org.apache.spark.sql.functions.col;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * This class uses Dataset APIs of spark to count number of articles per month
 * The year-month is obtained as a dataset of String
 * */

public class CooccurenceCount{

	public static void main(String[] args) {
		
		//Input dir - should contain all input json files
		String inputPath="/home/prateek/cs631/Assignment2/CooccurenceCount/newsdata"; //Use absolute paths 
	    String KeyWords[] 
      = new String[]{"sonia","lalu","nitish","farooq","sushma","tharoor","smriti","mamata","karunanidhi","kejriwal","sidhu","yogi","mayawati","akhilesh","chandrababu" ,"chidambaram","fadnavis","uddhav","pawar"}; 

		
		//Ouput dir - this directory will be created by spark. Delete this directory between each run
		String outputPath1="/home/prateek/cs631/Assignment2/CooccurenceCount/output1";   //Use absolute paths
		String outputPath2="/home/prateek/cs631/Assignment2/CooccurenceCount/output2";   //Use absolute paths
		String outputPath3="/home/prateek/cs631/Assignment2/CooccurenceCount/output3";   //Use absolute paths
		String outputPath4="/home/prateek/cs631/Assignment2/CooccurenceCount/output4";   //Use absolute paths
	StructType structType = new StructType();
	  structType = structType.add("entity", DataTypes.StringType, false); // false => not nullable
	  structType = structType.add("article_id", DataTypes.StringType, false); // false => not nullable
	  ExpressionEncoder<Row> entityArticleIdEncoder= RowEncoder.apply(structType);

		SparkSession sparkSession = SparkSession.builder()
				.appName("Cooccurence Count")		//Name of application
				.master("local")								//Run the application on local node
				.config("spark.sql.shuffle.partitions","2")		//Number of partitions
				.getOrCreate();
		////Read multi-line JSON from input files to dataset
		Dataset<Row> inputDataset=sparkSession.read().option("multiLine", true).json(inputPath);   
		//// Apply the map function to extract the year-month
		Dataset<Row> entityArticleIdDataset=inputDataset.flatMap(new FlatMapFunction<Row, Row>(){
	    				public Iterator<Row> call(Row  row) throws Exception {
				        String articleBody=(String)row.getAs("article_body");
                String articleID = (String)row.getAs("article_id");
	    					articleBody = articleBody.toLowerCase().replaceAll("[^A-Za-z]", " ");  //Remove all punctuation and convert to lower case
	    					articleBody = articleBody.replaceAll("( )+", " ");   //Remove all double spaces
	    					articleBody= articleBody.trim(); 
	    					List<String> wordList = Arrays.asList(articleBody.split(" ")); //Get words
                List <Row> RowList = new ArrayList<Row>();
                for (String word: wordList){
                   Row temp = RowFactory.create(word,articleID);
                   RowList.add(temp);
                } 
	    					return RowList.iterator();
	    				}
              },entityArticleIdEncoder);
//	 DataSet<Row> df = sparkSession.createDataFrame(entityArticleIdDataset,structType );   			
  Dataset<Row> FilteredRows = entityArticleIdDataset.filter(entityArticleIdDataset.col("entity").isin(KeyWords));

		Dataset<Row> count=FilteredRows.groupBy("entity").count();  
		Dataset<Row> joinedRows=FilteredRows.join(FilteredRows, FilteredRows.col("article_id"));  
  //FilteredRows.collect().show()  ; 
//			public String call(Row row) throws Exception {
		//Dataset<Row> count=entityArticleIdDataset.groupBy("entity").count().as("entity_count");  
//	org.apache.spark.api.java.function.FilterFunction;

  //FilteredRows.select("entity", "article_id").write().text(outputPath1);		

		FilteredRows.dropDuplicates("entity","article_id").toJavaRDD().saveAsTextFile(outputPath1);	
		count.toJavaRDD().saveAsTextFile(outputPath2);	
		joinedRows.toJavaRDD().saveAsTextFile(outputPath3);	
	
		//Outputs the dataset to the standard output
//	FilteredRows.show();
		
		//Ouputs the result to a file
	// FilteredRows.saveAsTextFile(outputPath);	
		
	}
}
	


