/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.demo;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Administrator on 2018/8/29 0029.
 * Lucene 索引管理工具类
 */
public class IndexManagerUtils {
  /**
   *  解析xml标签
   */
  private static Pattern pattern =
      Pattern.compile("<doc id=\"([^\"]*)\" url=\"[^\"]*\" title=\"([^\"]*)\">");
  private static long fileNum=0;
  private static long finishNum=0;

  private static void getFileNum(File targetFileDir) {
    /** 如果传入的路径不是目录或者目录不存在，则放弃*/
    if (!targetFileDir.isDirectory() || !targetFileDir.exists()) {
      return;
    }
    for (File file : targetFileDir.listFiles()){
      if (file.isDirectory()) {
        /**如果当前是目录，则进行方法回调*/
        getFileNum(file);
      } else {
        fileNum++;
      }
    }
  }
  /**
   * 为指定目录下的文件创建索引,包括其下的所有子孙目录下的文件
   *
   * @param targetFileDir ：需要创建索引的文件目录
   * @param indexSaveDir  ：创建好的索引保存目录
   * @throws IOException
   */
  private static void indexCreate(File targetFileDir, File indexSaveDir) throws IOException {
    /** 如果传入的路径不是目录或者目录不存在，则放弃*/
    if (!targetFileDir.isDirectory() || !targetFileDir.exists()) {
      return;
    }

    /** 创建 Lucene 文档列表，用于保存多个 Docuemnt*/
    List<Document> docList = new ArrayList<>();

    /**循环目标文件夹，取出文件
     * 然后获取文件的需求内容，添加到 Lucene 文档(Document)中
     * 此例会获取 文件名称、文件内容、文件大小
     * */
    for (File file : targetFileDir.listFiles()) {
      if (file.isDirectory()) {
        /**如果当前是目录，则进行方法回调*/
        indexCreate(file, indexSaveDir);
      } else {
        List<String> fileContext = FileUtils.readLines(file, "UTF-8");
        Document luceneDocument = null;
        String content = "";
        for (String a : fileContext) {
          Matcher matcher = pattern.matcher(a);
          if (matcher.find()) {
            String title = matcher.group(2);
            /**Lucene 文档对象(Document)，文件系统中的一个文件就是一个 Docuemnt对象
             * 一个 Lucene Docuemnt 对象可以存放多个 Field（域）
             *  Lucene Docuemnt 相当于 Mysql 数据库表的一行记录
             *  Docuemnt 中 Field 相当于 Mysql 数据库表的字段*/
            luceneDocument = new Document();
            TextField titleFiled = new TextField("title", title, Store.NO);
            luceneDocument.add(titleFiled);
            continue;
          }
          if (a.equals("</doc>")) {
            TextField contentFiled = new TextField("content", content, Store.NO);
            luceneDocument.add(contentFiled);
            /**将文档存入文档集合中，之后再同统一进行存储*/
            docList.add(luceneDocument);
            content = "";
            continue;
          }
          content = content.concat(a);
          content = content.concat(" ");
        }
        System.out.println("finishFileNum: "+Long.toString(finishNum++));
      }
    }
    Analyzer analyzer = new StandardAnalyzer();

    /** 指定之后 创建好的 索引和 Lucene 文档存储的目录
     * 如果目录不存在，则会自动创建*/
    Path path = Paths.get(indexSaveDir.toURI());

    /** FSDirectory：表示文件系统目录，即会存储在计算机本地磁盘，继承于
     * org.apache.lucene.store.BaseDirectory
     * 同理还有：org.apache.lucene.store.RAMDirectory：存储在内存中
     */
    Directory directory = FSDirectory.open(path);

    /** 创建 索引写配置对象，传入分词器*/
    IndexWriterConfig config = new IndexWriterConfig(analyzer);
    config.setUseCompoundFile(false);
    /**创建 索引写对象，用于正式写入索引和文档数据*/
    IndexWriter indexWriter = new IndexWriter(directory, config);

    /**将 Lucene 文档加入到 写索引 对象中*/
    for (int i = 0; i < docList.size(); i++) {
      indexWriter.addDocument(docList.get(i));
      if ((i + 1) % 50 == 0) {
        indexWriter.flush();
      }
    }
    /**最后再 刷新流，然后提交、关闭流*/
    indexWriter.flush();
    indexWriter.commit();
    indexWriter.close();
    docList.clear();
  }

  public static void main(String[] args) throws IOException {
    File file1 = new File("D:/lucene_index/data/wikipedia");
    File file2 = new File("D:/lucene_index/output_index");
    getFileNum(file1);
    System.out.println(fileNum);
    indexCreate(file1, file2);
  }
}

