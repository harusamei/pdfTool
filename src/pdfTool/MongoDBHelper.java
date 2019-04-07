package pdfTool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;

//被调用的工具类：
public class MongoDBHelper {
/**
* 
* @return: 返回需要操作的集合
*/
	public static void main(String [] args){
		
	}
	// 从一个库导到别一个库
	public static void transCollection(){
		//source
		MongoClient clientA =getMongoDbClient("localhost", 27017, "root", "root", "admin");
		MongoCollection<Document> aColl = getMongoDbCollection(clientA, "chemistry","Patents");
		//target
		MongoClient clientB = getMongoDbClient("10.167.174.169", 27017, "frdc_rw", "frdc_rw_123", "admin");
		MongoCollection<Document> bColl = getMongoDbCollection(clientB, "chemical_data","patents_pdf");
		
		Document tObj=new Document();
		Document sObj=null;
		FindIterable<Document> findIterable = aColl.find();	//直接find() 则全部遍历
		FindIterable<Document> tIter=null;
		MongoCursor<Document> mongoCursor = findIterable.noCursorTimeout(true).iterator();
		
		int count=0;
		List<String> temList=null;

		while (mongoCursor.hasNext()) {
			sObj=mongoCursor.next();
			tObj.clear();
			
			/*tObj.put("_id", sObj.get("_id"));				//paper_pdf
			if(sObj.get("fulltxt")!=null)
				tObj.put("pdf2Txt", sObj.get("fulltxt"));
			if(sObj.getString("title")!=null)
				tObj.put("title", sObj.get("title"));
			if(sObj.getString("abstract")!=null)
				tObj.put("abstract", sObj.get("abstract"));
			if(sObj.getString("content")!=null && sObj.getString("content").length()>0)
				tObj.put("body", sObj.get("content"));*/
			
			tObj.put("_id", sObj.get("_id"));				//paper_pdf
			if(sObj.get("fulltxt")!=null)
				tObj.put("pdf2Txt", sObj.get("fulltxt"));
			if(sObj.get("title")!=null){
				temList=(List<String>)sObj.get("title");
				tObj.put("title", String.join("\r\n",temList));		//数组转字符串
			}
			if(sObj.get("abstract")!=null){
				temList=(List<String>)sObj.get("abstract");
				tObj.put("abstract", String.join("\r\n",temList));
			}
			if(sObj.get("claim")!=null)
				tObj.put("claim", String.join("\r\n", (List<String>)sObj.get("claim")));
			if(sObj.get("specification")!=null)
				tObj.put("specification", String.join("\r\n",(List<String>)sObj.get("specification")));
			
			tIter = bColl.find(Filters.eq("_id", sObj.get("_id")));	
			if(tIter.iterator().hasNext()){
				continue;
			}
			bColl.insertOne(tObj);

			count++;
		}
		System.out.println(count);
		clientA.close();
		clientB.close();
		
		
	}
	public static MongoClient getMongoDbClient(String ipAddr, int portNum, String userName, String passWord, String dbName) {
		MongoCredential credential = MongoCredential.createScramSha1Credential(userName, dbName, passWord.toCharArray());
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(credential);
		ServerAddress serverAddress = new ServerAddress(ipAddr, portNum);
		ArrayList<ServerAddress> addrs = new ArrayList<ServerAddress>();
		addrs.add(serverAddress);
		MongoClient mongoClient = new MongoClient(addrs, credentials);
		return mongoClient; // 返回collection集合
	}
	
	public static MongoCollection<Document> getMongoDbCollection(MongoClient mongoClient, String dbName, String collectionName) {
	
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCollection<Document> collection = db.getCollection(collectionName);
		return collection;
	}
	
	public static void closeMongoClient(MongoClient mongoClient) {
	
		if (mongoClient != null) {
		mongoClient.close();
		}
		mongoClient = null;
	}
	//发现并遍历
	public static void iterateDB(){
		
		MongoClient clientA =getMongoDbClient("localhost", 27017, "root", "root", "admin");
		MongoCollection<Document> aColl = getMongoDbCollection(clientA, "chemistry","Patents");
		
		Document tObj=new Document();
		tObj.append("_id", 1);
		FindIterable<Document> findIterable = aColl.find(tObj);	//直接find() 则全部遍历
		MongoCursor<Document> mongoCursor = findIterable.noCursorTimeout(true).iterator();
		
		int count=0;
		while (mongoCursor.hasNext()) {
			System.out.println(mongoCursor.next());
			count++;
			if(count>1){
				break;
			}
		}
		clientA.close();
	}
	//存取
	public static void accessDataFromMongo(MongoCollection<Document> mongoCollection) {
		// insert
		
		Document oneDoc=new Document();
		ArrayList<String> temList=new ArrayList<String>();
		List<String> list = Arrays.asList("o1","o2");
		temList.addAll(list);
		
		oneDoc.append("_id", 43).append("author", "MY").append("add",temList);
		mongoCollection.insertOne(oneDoc);
		
		// update 
		UpdateResult temResult=mongoCollection.updateMany(Filters.eq("author", "MY"), new Document("$set",new Document("author","Mengyao")));  
		System.out.println(temResult.getMatchedCount());
		
		//delete
		Document condition=new Document();
		condition.append("_id", 41);
		System.out.printf("deleteCount=%d",mongoCollection.deleteMany(condition).getDeletedCount());
		
		//replace
		oneDoc.clear();
		oneDoc.append("mail", "mengyao@cn.fuj");
		temResult=mongoCollection.replaceOne(Filters.eq("_id",40), oneDoc);
		System.out.printf("replace Count=%d",temResult.getMatchedCount());

	}	

}
