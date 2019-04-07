package pdfTool;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;


public class pdfParser {
	
	static PrintWriter fOut;
	static List<String> outSents=new LinkedList<>();
	static List<String> inSents=new LinkedList<>();
	
	static MongoClient mongoClient=null;
	static MongoCollection<Document> aCollection=null;
	static Document editDoc=new Document();

	public static void main(String[] args) {
	
//		String fullTxt=parsePdf("D:/backup until 201707/��Դ/coupus/paper/fullTxt_pdf/9b70a761-623d-4405-adad-2766d8dbc00a.pdf");
//		try{
//			fOut=new PrintWriter("fullTxt.out","utf-8");
//			fOut.println(fullTxt);
//			fOut.close();
//		}catch (IOException e) {
//            // TODO Auto-generated catch block
//			e.printStackTrace();
//		}	
		connectMongoDB();
		covPapers();
		closeMongoDB();
		System.out.println("finished");
		
	}
	public static void connectMongoDB(){
		mongoClient = MongoDBHelper.getMongoDbClient("localhost", 27017, "root", "root", "admin");
		aCollection = MongoDBHelper.getMongoDbCollection(mongoClient, "chemistry","Papers");
		
	}
	public static void closeMongoDB(){
		mongoClient.close();
	}
	public static int insertAll(String pathName){
		
		File file = new File(pathName);
		File[] files = file.listFiles();
		if(files ==null){
			System.out.println("can not open dir");
			return 0;
		}
		int count=0;
		for(File f:files){
			if(f.isFile()){
				if(f.getName().indexOf(".pdf")==-1){
					continue;
				}
				count++;
				insertOne(pathName+"/"+f.getName());
			}
			if(f.isDirectory()){
				count+=insertAll(f.getAbsolutePath());
			}
		}
		return count;
	}
	public static void insertOne(String fileName){
		
		System.out.println("save Pdf:"+fileName+" in db");
		
		FindIterable<Document> findIterable=null;
		String [] tList=fileName.split("/");
		String fId=tList[tList.length-1].split(".pdf")[0];
		findIterable = aCollection.find(Filters.eq("_id", fId));	//ֱ��find() ��ȫ������
		if(findIterable.iterator().hasNext()){
			return;
		}
		String fullTxt=parsePdf(fileName);
		Document oneDoc=new Document();
		ArrayList<String> temList=new ArrayList<String>();
		List<String> list = Arrays.asList(fullTxt.split("\r\n"));
		temList.addAll(list);
		oneDoc.append("_id",fId ).append("fulltxt",temList);
		aCollection.insertOne(oneDoc);
		
	}
	public static String parsePdf(String fileName){
		
		//System.out.println("this is parsePdf");
		PDDocument pdDoc =null;
		String outName="";
		String txtStr="";
		try {
			  pdDoc = PDDocument.load(new File(fileName));
			  //outName=fileName.split(".pdf")[0]+".txt";
			  PDFTextStripper textStripper = new PDFTextStripper();
			  txtStr=textStripper.getText(pdDoc);
			  //PrintWriter fResult=new PrintWriter(outName,"utf-8");
			  //fResult.println(textStripper.getText(pdDoc));
			  pdDoc.close();
			  //fResult.close();
			  
		}catch (IOException e) {
            // TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("parsing result is in xxx.txt");
		return txtStr;
	}
	public static boolean getPageHeader(int beg, int end){ 
		boolean isMatch=false;
		int i;
		i=beg>10?beg:10;
		if(end<0)	end=inSents.size();
		String temStr="";
		Pattern r1=Pattern.compile("Chem|Phys|Cata|Scien|Natu|RSC|Rev");
		Pattern r2=Pattern.compile("\\s2\\d{3}\\b");
		Pattern r3=Pattern.compile("^\\d+$");
		String pHeader="";
		for(;i<end; i++){
			temStr=inSents.get(i);
			if(!Pattern.compile("[\u4e00-\u9fa5]").matcher(temStr).find()){
				if(r1.matcher(temStr).find() && r2.matcher(temStr).find()){
					if(r3.matcher(inSents.get(i-1)).find())	pHeader+=(i-1)+";";
					pHeader+=i+";";
					isMatch=true;
					continue;
				}
			}else if(temStr.indexOf("��")>0 && temStr.indexOf("No")>0){
				if(r3.matcher(inSents.get(i-1)).find()){
					pHeader+=(i-1)+";"+i+";";
					isMatch=true;
					continue;
				}
			}
		}
		if(isMatch)		editDoc.put("headerIndx", pHeader);
		return isMatch;
	}
	public static boolean getPaperBody(int beg, int end, String filter){
		int i=beg<0?2:beg;
		end=end<0?inSents.size():end;
		filter=";"+filter;
	
		String body="";
		Pattern r=Pattern.compile("��$");
		Pattern r1=Pattern.compile("^\\d.*[\u4e00-\u9fa5]\\s*$");
		Pattern r2=Pattern.compile("[\u4e00-\u9fa5]");
		Pattern r3=Pattern.compile("��.*��.*��.*��");
		Pattern r4=Pattern.compile("ͨ\\s*Ѷ.*mail|^��\\s*��\\s*��|^��ͼ����");
		String temLine;
		for(;i<end;i++){
			if(filter.indexOf(";"+i+";")>=0) continue;
			temLine=inSents.get(i);
			if(temLine.length()>15 && !r2.matcher(temLine).find()){
				continue;
			}
			if(r4.matcher(temLine).find())	continue;
			if(body.length()==0 && !r2.matcher(temLine).find())	continue;
			if(r3.matcher(temLine).find())	break;
			temLine=temLine.replaceAll("\\s+", "");
			if(r1.matcher(temLine).find()){
				temLine=temLine.replace("ҡ"," ");
			}
			body+=temLine;
			if(r.matcher(temLine).find() || r1.matcher(temLine).find()){
				body+="\r\n";
			}
		}
		if(body.length()>100 && r2.matcher(body).find()){
			editDoc.put("content",body);
		}else{
			editDoc.put("content","");
		}
		
		return true;
	}
	public static boolean getTitle(int indx){
		boolean isMatch=false;
		if(indx<2) return false;
		String title=inSents.get(indx-1);
		if(Pattern.compile("[\u4E00-\u9FA5]\\s+[\u4E00-\u9FA5]").matcher(title).find()){ //����ƽ    ������    ��  ��    ��  ��    ��  ��    ��  �� 
			indx--;
			title=inSents.get(indx-1);
		}
		Matcher m=null;
		String temS=inSents.get(indx-2);
		m=Pattern.compile("[\u4E00-\u9FA5]{5,}").matcher(inSents.get(indx-2));
		if(title.length()<15&&m.find()&&inSents.get(indx-2).indexOf("����")<0){
			title=inSents.get(indx-2)+title;
		}
		editDoc.put("title", title);
		return true;
	}
	public static boolean getPaperAbs(int beg, int end){	//get abstract of paper
		boolean isMatch=false;
		int i=beg<0?5:beg;
		int oriEnd=end;
		end=end<0?inSents.size()-10:end;
		String temLine="";
		String abstr="";
		for(;i<end; i++){
			temLine=inSents.get(i);
			if(temLine.indexOf("ժ")>-1){
				isMatch=true;
				break;
			}
		}
		Pattern r=Pattern.compile("[\u4e00-\u9fa5]{2,}");
		Pattern r1=Pattern.compile("�ؼ���|��ͼ��|�ո�");
		for(;i<end; i++){
			temLine=inSents.get(i);
			if(r.matcher(temLine).find()){
				if(r1.matcher(temLine).find())	abstr+="\r\n";
				abstr+=temLine;
			}else{
				break;
			}
		}
		if(isMatch){
			editDoc.put("abstract",abstr);
			if(oriEnd<0) editDoc.put("beg",i);
		}
		return isMatch;
	}
	public static boolean getBody(){	//���ԣ�Reference	
		boolean isMatch=false;
		String oneLine="";
		Pattern r=Pattern.compile("��\\s*��");
		Matcher m=null;
		int i=0;
		for(; i<inSents.size()/2;i++){
			oneLine=inSents.get(i);
			m=r.matcher(oneLine);
			if(m.find()){
				editDoc.put("beg",i);
				isMatch=true;
				break;
			}
		}
		for(;i<inSents.size();i++){
			oneLine=inSents.get(i);
			m=Pattern.compile("eferences\\s*").matcher(oneLine);  //References 
			if(m.find()){
				editDoc.put("end",i);
				isMatch=true;
				break;
			}
		}
		
		return isMatch;
	}
	public static int getAAA(){			//author, address, abstract
		boolean isMatch=false;
		String oneLine="";
		int i=0;
		for(; i<inSents.size()-5;i++){
			oneLine=inSents.get(i);
			if(oneLine.indexOf("�6�5")>0 || oneLine.indexOf("*")>0){
				if(/*&& inSents.get(i+1).indexOf("(")==0 */inSents.get(i+2).indexOf("ժҪ")>=0){
					isMatch=true;
					break;
				}
				if(/*&& inSents.get(i+1).indexOf("(")==0*/ inSents.get(i+3).indexOf("ժҪ")>=0){
					isMatch=true;
					break;
				}
				if(/*&& inSents.get(i+1).indexOf("(")==0*/ inSents.get(i+4).indexOf("ժҪ")>=0){
					isMatch=true;
					break;
				}
				continue;
			}
			//��
			if((inSents.get(i+1).indexOf("��")>=0 ||inSents.get(i+1).indexOf("(")>=0) && inSents.get(i+2).indexOf("ժҪ")>=0){
				isMatch=true;
				break;
			}
			if((inSents.get(i+1).indexOf("��")>=0 ||inSents.get(i+1).indexOf("(")>=0) && inSents.get(i+3).indexOf("ժҪ")>=0){
				isMatch=true;
				break;
			}
			
		}
		return isMatch? i:-1;
	}
	public static int getEnArticle(){			// Ӣ��PAPER
		
		String oneLine="";
		boolean isMatch=false;		//Ĭ�Ϸ�
		int chNum=0;
		Pattern r = Pattern.compile("[\u4E00-\u9FA5]");
		Matcher m;
		
		for(int i=0; i<inSents.size()/2;i++){
			oneLine=inSents.get(i);
			if(oneLine.indexOf("ժ")>=0 ||oneLine.indexOf("��")>=0 || oneLine.indexOf("��")>=0){
				isMatch=true;		// ��ʽ����
				break;
			}
			m=r.matcher(oneLine);
			if(m.find())	chNum++;
		}
		if(isMatch){
			return 1;		//chinese
		}else if(chNum>10){
			return 0;		//non-standard
		}
		return -1;			//non-chinese
		
	}
	public static void covPapers(){
		
		inSents.clear();
		outSents.clear();
		
		Document tObj=new Document();
		FindIterable<Document> findIterable = aCollection.find();	//ֱ��find() ��ȫ������
		ArrayList<String> temList=new ArrayList<String>();

		MongoCursor<Document> mongoCursor = findIterable.noCursorTimeout(true).iterator();
		
		int count=0;
		List<String> errList=new LinkedList<>();
		int matedIndx=0;
		int isChArt=-1;
		while (mongoCursor.hasNext()) {
			editDoc.clear();
			outSents.clear();
			tObj=mongoCursor.next();
			
			inSents=(List<String>)tObj.get("fulltxt");
			//if(!tObj.getString("lang").equals("Chinese"))	continue;
			/*if(inSents.size()<2){
				editDoc.put("_id", tObj.getString("_id"));
				System.out.printf("deleteCount=%d",aCollection.deleteMany(editDoc).getDeletedCount());
				continue;
			}*/
			/*matedIndx=getAAA();
			isChArt=getEnArticle();
			if(isChArt==1){
				editDoc.put("lang", "Chinese");
			}else if(isChArt==0){
				editDoc.put("lang", "non-standard");
			}else{
				editDoc.put("lang","non-Chinese");
			}
			if(matedIndx>0){
				count++;
				editDoc.put("authorIndx", matedIndx);
			}*/
//			int beg=tObj.getInteger("authorIndx",-1);
//			int end=tObj.getInteger("beg", -1);
//			if(getPaperAbs(beg,end)){
//				count++;
//				aCollection.updateOne(Filters.eq("_id", tObj.getString("_id")), new Document("$set",editDoc));  
//			}
			int beg=tObj.getInteger("beg",-1);
			int end=tObj.getInteger("end", -1);
			String filt=tObj.getString("headerIndx");
			if(getPaperBody(beg,end,filt)){
				count++;
				aCollection.updateOne(Filters.eq("_id", tObj.getString("_id")), new Document("$set",editDoc));  
			}
		}
		System.out.println(count);
		try{
			fOut=new PrintWriter("errList.out","utf-8");
			errList.forEach((v)->{fOut.println(v);});
			fOut.close();
		}catch (IOException e) {
	        // TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void covPatents(){
		
		inSents.clear();
		outSents.clear();
		
		Document tObj=new Document();
		UpdateResult temResult=null;

		FindIterable<Document> findIterable = aCollection.find();	//ֱ��find() ��ȫ������
		ArrayList<String> temList=new ArrayList<String>();

		MongoCursor<Document> mongoCursor = findIterable.noCursorTimeout(true).iterator();
		
		int count=0;
		int lineCur=0;
		List<String> errList=new LinkedList<>();
		while (mongoCursor.hasNext()) {
			count++;
			editDoc.clear();
			outSents.clear();
			tObj=mongoCursor.next();
			
			if(count%1000==0){
				System.out.println(count);
			}
			/*if(!tObj.getString("_id").equals("CN201210004478[ZL]")){
				continue;
			}*/
			inSents=(List<String>)tObj.get("fulltxt");
			if(inSents.size()<2)	continue;
			
			lineCur=getSpecification(0);
			temResult=aCollection.updateMany(Filters.eq("_id", tObj.getString("_id")), new Document("$set",editDoc));  
			if(((List<String>)editDoc.get("specification")).size()<2){
				errList.add(tObj.getString("_id"));
				System.out.println(tObj.getString("_id"));
			}
		}
		
		try{
			fOut=new PrintWriter("errList.out","utf-8");
			errList.forEach((v)->{fOut.println(v);});
			fOut.close();
		}catch (IOException e) {
	        // TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static int getClaims(int lineBeg){
		
		int i=lineBeg;
		String temLine;
		Pattern r=Pattern.compile("Ȩ\\s*��\\s*Ҫ\\s*��\\s*��");
		Matcher m=null;

		//�ƶ���Ȩ��Ҫ���鿪ʼ
		for(;i<inSents.size();i++){
			temLine=inSents.get(i);
			m=r.matcher(temLine);
			if(m.find()){
				break;
			}
		}
		//û�ҵ�claim
		if(i+2>=inSents.size()){
			editDoc.append("claim",new ArrayList<String>());
			return inSents.size();
		}
		//Ȩ  ��  Ҫ  ��  ��CN 103193562 B
		String aPattern="\\d\\/(\\d+)\\s*ҳ";  //        1/2 ҳ
		r = Pattern.compile(aPattern);
		int pageCount=0;

		for(;i<inSents.size();i++){
			temLine=inSents.get(i);
			m=r.matcher(temLine);
			if(m.find()){
				i+=2;
				pageCount=Integer.parseInt(m.group(1));
				break;
			}
		}
		if(pageCount==0){
			//û�ҵ�
			editDoc.append("claim",new ArrayList<String>());
			return inSents.size();
		}
		for(;i<inSents.size();i++){
			if(pageCount==0){
				break;
			}
			temLine=inSents.get(i);
			m=r.matcher(temLine);
			if(m.find()){
				pageCount--;
				if(outSents.get(outSents.size()-1).indexOf("CN")>-1){
					outSents.remove(outSents.size()-1);
				}
				i+=1;
				continue;
			}
			outSents.add(temLine);
		}
		
		//����editDoc
		ArrayList<String> temList=new ArrayList<String>();
		temList.add("Ȩ��Ҫ����");
		int x=0;
		String temStr="";
		r = Pattern.compile("^\\d+\\.");
		Pattern r2=Pattern.compile("[������]$");
		for(;x<outSents.size(); x++){
			temLine=outSents.get(x);
			m=r2.matcher(temLine);
			if(m.find()){
				temList.add(temStr+temLine);
				temStr="";
				continue;
			}
			m=r.matcher(temLine);
			if(m.find() && temStr.length()>0){
				temList.add(temStr);
				temStr="";
			}
			temStr+=temLine;
		}
		if(temStr.length()>0) temList.add(temStr);
		editDoc.append("claim",temList);
		return i;
	}
	public static int getSpecification(int lineBeg){
		int i=lineBeg;
		String temLine;
		Pattern r=Pattern.compile("\\[0001\\]");
		Matcher m=null;

		//�ƶ���˵���鿪ʼ
		for(;i<inSents.size();i++){
			temLine=inSents.get(i);
			m=r.matcher(temLine);
			if(m.find()){
				break;
			}
		}
		//û�ҵ�claim
		if(i+2>=inSents.size()){
			editDoc.append("specification",new ArrayList<String>());
			return inSents.size();
		}
		
		r=Pattern.compile("\\d\\/(\\d+)\\s*ҳ");
		for(;i<inSents.size();i++){
			temLine=inSents.get(i);
			m=r.matcher(temLine);
			if(m.find()){
				if(outSents.get(outSents.size()-1).indexOf("CN")>-1) outSents.remove(outSents.size()-1);
				i++;
				continue;
			}
			outSents.add(temLine);
		}
		ArrayList<String> temList=new ArrayList<String>();
		temList.add("˵����");
		temList.add("��������");
		r=Pattern.compile("^\\[\\d+\\]");
		Pattern r2=Pattern.compile("[������]$");
		String temStr="";
		for(int x=0; x<outSents.size();x++){
			temLine=outSents.get(x);
			m=r.matcher(temLine);
			if(m.find() && temStr.length()>0){
				temList.add(temStr);
				temStr="";
			}
			m=r2.matcher(temLine);
			if(m.find()){
				temList.add(temStr+temLine);
				temStr="";
				continue;
			}
			temStr+=temLine;
		}
		if(temStr.length()>0 && temStr.indexOf("CN")<0) temList.add(temStr);
		editDoc.append("specification",temList);
		return i;
	}
	public static int getHead(int lineBeg){		//ȡ�ñ����ժҪ
		
		String temLine;
		String aPattern="^\\(\\d+\\)";
		Pattern r = Pattern.compile(aPattern);
		Matcher m;
		int headCount=2;
		int i=lineBeg;
		for(; i<inSents.size();i++){
			temLine=inSents.get(i);
			if(temLine.indexOf("��������")>0){
				headCount--;
				break;
			}
		}
		for(; i<inSents.size();i++){
			temLine=inSents.get(i);
			m = r.matcher(temLine);
			if(m.find()){
				if(headCount==0)	break;
				else if(temLine.indexOf("ժҪ")>0){
					headCount--;
				}
			}
			outSents.add(temLine);
		}
		
		ArrayList<String> temList=new ArrayList<String>();
		String temStr="";
		
		if(outSents.size()<1){
			editDoc.append("title", temList).append("abstract", temList);
			return i;
		}
		int x=1;
		temList.add(outSents.get(0));
		for(; x<outSents.size(); x++){
			temLine=outSents.get(x);
			if(temLine.indexOf("ժҪ")>0){
				temList.add(temStr);
				editDoc.append("title",temList.clone());
				temList.clear();
				temList.add(temLine);
				x++;
				break;
			}
			temStr+=temLine;
		}
		temStr="";
		String bPattern="��$";
		r=Pattern.compile(bPattern);
		for(;x<outSents.size();x++){
			temLine=outSents.get(x);
			temStr+=temLine;

			m=r.matcher(temLine);
			if(m.find()){
				temList.add(temStr);
				temStr="";
			}
		}
		if(temStr.length()>0)	temList.add(temStr);
		editDoc.append("abstract",temList.clone());
		return i;
	}
	public static int getFileList(String pathName){
		
		File file = new File(pathName);
		File[] files = file.listFiles();
		if(files ==null){
			System.out.println("can not open dir");
			return 0;
		}
		int count=0;
		for(File f:files){
			if(f.isFile()){
				count++;
				fOut.println(f.getName());
			}
			if(f.isDirectory()){
				count+=getFileList(f.getAbsolutePath());
			}
		}
		System.out.println(pathName+"\t"+count);
		return count;
		
	}

}
