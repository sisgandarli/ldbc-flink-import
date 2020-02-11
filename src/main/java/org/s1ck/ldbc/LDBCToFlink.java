/*
 * This file is part of ldbc-flink-import.
 *
 * ldbc-flink-import is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * ldbc-flink-import is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *
 * You should have received a copy of the GNU General Public License
 * along with ldbc-flink-import. If not, see <http://www.gnu.org/licenses/>.
 */

package org.s1ck.ldbc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.s1ck.ldbc.functions.EdgeLineReader;
import org.s1ck.ldbc.tuples.LDBCEdge;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.s1ck.ldbc.LDBCConstants.*;

/**
 * Main class to read LDBC output into Apache Flink.
 */
public class LDBCToFlink {

  /** Logger */
  private static final Logger LOG = Logger.getLogger(LDBCToFlink.class);

  /** Flink execution environment */
  private final StreamExecutionEnvironment env;

  /** Hadoop Configuration */
  private final Configuration conf;

  /** Directory, where the LDBC output is stored. */
  private final String ldbcDirectory;

  /**
   * Defines how tokens are separated in a filename. For example in
   * "comment_0_0.csv" the tokens are separated by "_".
   */
  private final Pattern fileNameTokenDelimiter;

  /** List of vertex files */
  private final List<String> vertexFilePaths;

  /** List of edge files */
  private final List<String> edgeFilePaths;

  /** List of property files */
  private final List<String> propertyFilePaths;

  /**
   * Maps a vertex class (e.g. Person, Comment) to a unique identifier.
   */
  private final Map<String, Long> vertexClassToClassIDMap;

  /**
   * Used to create vertex class IDs.
   */
  private long nextVertexClassID = 0L;

  /**
   * Creates a new parser instance.
   *
   * @param ldbcDirectory path to LDBC output
   * @param env Flink execution environment
   */
  public LDBCToFlink(String ldbcDirectory, StreamExecutionEnvironment env) {
    this(ldbcDirectory, env, new Configuration());
  }

  /**
   * Creates a new parser instance.
   *
   * @param ldbcDirectory path to LDBC output
   * @param env Flink execution environment
   * @param conf Hadoop cluster configuration
   */
  public LDBCToFlink(String ldbcDirectory, StreamExecutionEnvironment env,
    Configuration conf) {
    if (ldbcDirectory == null || "".equals(ldbcDirectory)) {
      throw new IllegalArgumentException("LDBC directory must not be null or empty");
    }
    if (env == null) {
      throw new IllegalArgumentException("Flink Execution Environment must not be null");
    }
    if (conf == null) {
      throw new IllegalArgumentException("Hadoop Configuration must not  be null");
    }
    this.ldbcDirectory = ldbcDirectory;
    this.vertexFilePaths = Lists.newArrayList();
    this.edgeFilePaths = Lists.newArrayList();
    this.propertyFilePaths = Lists.newArrayList();
    this.env = env;
    this.conf = conf;
    this.vertexClassToClassIDMap = Maps.newHashMap();
    fileNameTokenDelimiter = Pattern.compile(FILENAME_TOKEN_DELIMITER);
    init();
  }

  /**
   * Parses and transforms the LDBC edge files to {@link LDBCEdge} tuples.
   *
   * @return DataSet containing all edges in the LDBC graph.
   */
  public DataStream<LDBCEdge> getEdges() throws Exception {
    LOG.info("Reading edges");
    List<DataStream<LDBCEdge>> edgeDataStreams =
      Lists.newArrayListWithCapacity(edgeFilePaths.size());
    for (String filePath : edgeFilePaths) {
      edgeDataStreams.add(readEdgeFile(filePath));
    }

    return unionDataStreams(edgeDataStreams)
            .map(new MapFunction<LDBCEdge, LDBCEdge>() {
              @Override
              public LDBCEdge map(LDBCEdge ldbcEdge) throws Exception {
                ldbcEdge.setEdgeId((long) (Math.random()) * 10000);
                return ldbcEdge;
              }
            });
  }

  private long getVertexClassCount() {
    return vertexFilePaths.size();
  }

  private <T> DataStream<T> unionDataStreams(List<DataStream<T>> dataStreams) {
    DataStream<T> finalDataStream = null;
    boolean first = true;
    for (DataStream<T> dataStream : dataStreams) {
      if (first) {
        finalDataStream = dataStream;
        first = false;
      } else {
        finalDataStream = finalDataStream.union(dataStream);
      }
    }
    return finalDataStream;
  }

  private DataStream<LDBCEdge> readEdgeFile(String filePath) throws Exception {
    LOG.info("Reading edges from " + filePath);
    String fileName = getFileName(filePath);
    String edgeClass = getEdgeClass(fileName);
    String sourceVertexClass = getSourceVertexClass(fileName);
    String targetVertexClass = getTargetVertexClass(fileName);
    Long sourceVertexClassId = getVertexClassId(sourceVertexClass);
    Long targetVertexClassId = getVertexClassId(targetVertexClass);
    Long vertexClassCount = getVertexClassCount();

    String[] edgeClassFields = null;
    FieldType[] edgeClassFieldTypes = null;
    switch (edgeClass) {
    case EDGE_CLASS_KNOWS:
      edgeClassFields = EDGE_CLASS_KNOWS_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_KNOWS_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_TYPE:
      edgeClassFields = EDGE_CLASS_HAS_TYPE_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_TYPE_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_LOCATED_IN:
      edgeClassFields = EDGE_CLASS_IS_LOCATED_IN_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_LOCATED_IN_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_INTEREST:
      edgeClassFields = EDGE_CLASS_HAS_INTEREST_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_INTEREST_FIELD_TYPES;
      break;
    case EDGE_CLASS_REPLY_OF:
      edgeClassFields = EDGE_CLASS_REPLY_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_REPLY_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_STUDY_AT:
      edgeClassFields = EDGE_CLASS_STUDY_AT_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_STUDY_AT_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_MODERATOR:
      edgeClassFields = EDGE_CLASS_HAS_MODERATOR_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_MODERATOR_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_MEMBER:
      edgeClassFields = EDGE_CLASS_HAS_MEMBER_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_MEMBER_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_TAG:
      edgeClassFields = EDGE_CLASS_HAS_TAG_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_TAG_FIELD_TYPES;
      break;
    case EDGE_CLASS_HAS_CREATOR:
      edgeClassFields = EDGE_CLASS_HAS_CREATOR_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_HAS_CREATOR_FIELD_TYPES;
      break;
    case EDGE_CLASS_WORK_AT:
      edgeClassFields = EDGE_CLASS_WORK_AT_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_WORK_AT_FIELD_TYPES;
      break;
    case EDGE_CLASS_CONTAINER_OF:
      edgeClassFields = EDGE_CLASS_CONTAINER_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_CONTAINER_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_PART_OF:
      edgeClassFields = EDGE_CLASS_IS_PART_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_PART_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_IS_SUBCLASS_OF:
      edgeClassFields = EDGE_CLASS_IS_SUBCLASS_OF_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_IS_SUBCLASS_OF_FIELD_TYPES;
      break;
    case EDGE_CLASS_LIKES:
      edgeClassFields = EDGE_CLASS_LIKES_FIELDS;
      edgeClassFieldTypes = EDGE_CLASS_LIKES_FIELD_TYPES;
      break;
    }
    DataStream<LDBCEdge> result = env.readTextFile(filePath, "UTF-8").map(
      new EdgeLineReader(edgeClass, edgeClassFields, edgeClassFieldTypes,
        sourceVertexClassId, sourceVertexClass, targetVertexClassId,
        targetVertexClass, vertexClassCount));
    return result;
  }

  private String getFileName(String filePath) {
    return filePath
      .substring(filePath.lastIndexOf(System.getProperty("file.separator")) + 1,
        filePath.length());
  }

  private String getVertexClass(String fileName) {
    return fileName.substring(0, fileName.indexOf(FILENAME_TOKEN_DELIMITER));
  }

  private String getEdgeClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[1];
  }

  private String getPropertyClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[1];
  }

  private String getSourceVertexClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[0];
  }

  private String getTargetVertexClass(String fileName) {
    return fileNameTokenDelimiter.split(fileName)[2];
  }

  private boolean isVertexFile(String fileName) {
    return isValidFile(fileName) &&
      fileName.split(FILENAME_TOKEN_DELIMITER).length == 3;
  }

  private boolean isEdgeFile(String fileName) {
    return isValidFile(fileName) &&
      fileName.split(FILENAME_TOKEN_DELIMITER).length == 5 &&
      !fileName.contains(PROPERTY_CLASS_EMAIL) &&
      !fileName.contains(PROPERTY_CLASS_SPEAKS);
  }

  private boolean isPropertyFile(String fileName) {
    return isValidFile(fileName) && (fileName.contains(PROPERTY_CLASS_EMAIL) ||
      fileName.contains(PROPERTY_CLASS_SPEAKS));
  }

  private boolean isValidFile(String fileName) {
    return !fileName.startsWith(".");
  }

  private Long getVertexClassId(String vertexClass) {
    Long vertexClassID;
    if (vertexClassToClassIDMap.containsKey(vertexClass)) {
      vertexClassID = vertexClassToClassIDMap.get(vertexClass);
    } else {
      vertexClassID = nextVertexClassID++;
      vertexClassToClassIDMap.put(vertexClass, vertexClassID);
    }
    return vertexClassID;
  }

  private void init() {
    if (ldbcDirectory.startsWith("hdfs://")) {
      initFromHDFS();
    } else {
      initFromLocalFS();
    }
  }

  private void initFromHDFS() {
    try {
      FileSystem fs = FileSystem.get(conf);
      Path p = new Path(ldbcDirectory);
      if (!fs.exists(p) || !fs.isDirectory(p)) {
        throw new IllegalArgumentException(
          String.format("%s does not exist or is not a directory", ldbcDirectory));
      }
      FileStatus[] fileStates = fs.listStatus(p);
      for (FileStatus fileStatus : fileStates) {
        String filePath = fileStatus.getPath().getName();
        if (isVertexFile(filePath)) {
          vertexFilePaths.add(ldbcDirectory + filePath);
        } else if (isEdgeFile(filePath)) {
          edgeFilePaths.add(ldbcDirectory + filePath);
        } else if (isPropertyFile(filePath)) {
          propertyFilePaths.add(ldbcDirectory + filePath);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void initFromLocalFS() {
    File folder = new File(ldbcDirectory);
    if (!folder.exists() || !folder.isDirectory()) {
      throw new IllegalArgumentException(
        String.format("%s does not exist or is not a directory", ldbcDirectory));
    }
    for (final File fileEntry : folder.listFiles()) {
      if (isVertexFile(fileEntry.getName())) {
        vertexFilePaths.add(fileEntry.getAbsolutePath());
      } else if (isEdgeFile(fileEntry.getName())) {
        edgeFilePaths.add(fileEntry.getAbsolutePath());
      } else if (isPropertyFile(fileEntry.getName())) {
        propertyFilePaths.add(fileEntry.getAbsolutePath());
      }
    }
  }
}
