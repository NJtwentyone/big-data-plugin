/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2024 by Hitachi Vantara : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.big.data.kettle.plugins.hdfs.job;

import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.di.trans.steps.named.cluster.NamedClusterEmbedManager;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.hadoop.HadoopSpoonPlugin;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles.DESTINATION_FILE_FOLDER;
import static org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles.SOURCE_FILE_FOLDER;

/**
 * Created by bryan on 11/23/15.
 */
public class JobEntryHadoopCopyFilesTest {
  private JobEntryHadoopCopyFiles jobEntryHadoopCopyFiles;
  private String testName;
  private NamedClusterService namedClusterManager;
  private String testUrl;
  private String testNcName;
  private IMetaStore metaStore;
  private Map mappings;
  private NamedCluster namedCluster;
  private NamedClusterEmbedManager mockNamedClusterEmbedManager;
  private final String EMPTY = "";

  @Before
  public void setup() {
    testName = "testName";
    namedClusterManager = mock( NamedClusterService.class );
    jobEntryHadoopCopyFiles =
        new JobEntryHadoopCopyFiles( namedClusterManager, mock( RuntimeTestActionService.class ), mock(
            RuntimeTester.class ) );
    jobEntryHadoopCopyFiles.setName( testName );
    testUrl = "testUrl";
    testNcName = "testNcName";
    metaStore = mock( IMetaStore.class );
    mappings = mock( Map.class );
    namedCluster = mock( NamedCluster.class );
    // TODO wire mock mockNamedClusterEmbedManager,
    Job parentJob = new Job();
    jobEntryHadoopCopyFiles.setParentJob( parentJob );
    JobMeta mockJobMeta = mock( JobMeta.class );
    mockNamedClusterEmbedManager = mock( NamedClusterEmbedManager.class );
    when( mockJobMeta.getNamedClusterEmbedManager() ).thenReturn(  mockNamedClusterEmbedManager );
    jobEntryHadoopCopyFiles.setParentJobMeta(  mockJobMeta );
  }

  @Test
  public void testLoadUrlNullNcName() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( testUrl, null, metaStore, mappings );
    assertNotNull( loadURL );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlNull() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( null, null, metaStore, mappings );
    assertNull( loadURL );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlNotNullForNotCluster() {
    testNcName = "LOCAL-SOURCE-FILE-1";
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( null );
    String loadURL = jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings );
    assertNotNull( loadURL );
    assertEquals( testUrl, loadURL );
    verify( mappings ).put( testUrl, testNcName );
  }

  @Test
  public void testLoadUrlMapRNull() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    assertNull( jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verifyNoMoreInteractions( mappings );
  }

  @Test
  public void testLoadUrlMapRNotNullNoPrefix() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    String testNewUrl = "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
  }

  @Test
  public void testLoadUrlMapRNotNullPrefix() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( true );
    String testNewUrl = HadoopSpoonPlugin.MAPRFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
  }

  @Test
  public void testLoadUrlNotMapR() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
        .thenReturn( testNewUrl );
    assertEquals( testNewUrl, jobEntryHadoopCopyFiles.loadURL( testUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( testNewUrl, testNcName );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( testNewUrl ) );
  }

  @Test
  public void testLoadUrlHdfsEMPTY_SOURCE_URL() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
      .thenReturn( testNewUrl );
    String prefixUrlSource = JobEntryCopyFiles.SOURCE_URL + 8 + "-";
    String testPrefixSourceUrl = prefixUrlSource + testUrl;
    String expectedPrefixSourceLoadUrl = prefixUrlSource + testNewUrl;
    assertEquals( expectedPrefixSourceLoadUrl, jobEntryHadoopCopyFiles.loadURL( testPrefixSourceUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( expectedPrefixSourceLoadUrl, testNcName );
    assertEquals( testPrefixSourceUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( expectedPrefixSourceLoadUrl ) );
  }

  @Test
  public void testLoadUrlHdfsEMPTY_DEST_URL() {
    when( namedClusterManager.getNamedClusterByName( testNcName, metaStore ) ).thenReturn( namedCluster );
    when( namedCluster.isMapr() ).thenReturn( false );
    String testNewUrl = HadoopSpoonPlugin.HDFS_SCHEME + "://" + "testNewUrl";
    when( namedCluster.processURLsubstitution( testUrl, metaStore, jobEntryHadoopCopyFiles.getVariables() ) )
      .thenReturn( testNewUrl );
    String prefixUrlDest = JobEntryCopyFiles.DEST_URL + 5 + "-";
    String testPrefixDestUrl = prefixUrlDest + testUrl;
    String expectedPrefixDestLoadUrl = prefixUrlDest + testNewUrl;
    assertEquals( expectedPrefixDestLoadUrl, jobEntryHadoopCopyFiles.loadURL( testPrefixDestUrl, testNcName, metaStore, mappings ) );
    verify( mappings ).put( expectedPrefixDestLoadUrl, testNcName );
    assertEquals( testPrefixDestUrl, jobEntryHadoopCopyFiles.fileFolderUrlMappings.get( expectedPrefixDestLoadUrl ) );
  }

  @Test
  public void testSaveUrlMappingsKeyMisses() {
    String testUrl = "/src/path/";
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.clear();
    // populating with other values
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyA", "ValueA" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyB", "ValueB" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src", "ValueC" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src/path/anotherPath", "ValueD" );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.saveURL( testUrl, testNcName, metaStore, mappings ) );

    assertNull( testUrl, jobEntryHadoopCopyFiles.saveURL( null, testNcName, metaStore, mappings ) );
  }

  @Test
  public void testSaveUrlMappingsKeyHits() {
    String testUrl = "/src/path/";
    String testUrlSubstituted = "hdfs://someHostname/src/path";
    // populating with other values
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyA", "ValueA" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "KeyB", "ValueB" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src", "ValueC" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( "/src/path/anotherPath", "ValueD" );
    jobEntryHadoopCopyFiles.fileFolderUrlMappings.put( testUrlSubstituted, testUrl );
    assertEquals( testUrl, jobEntryHadoopCopyFiles.saveURL( testUrl, testNcName, metaStore, mappings ) );
  }


  @Test
  public void saveLoadWithNamedClusters() throws Exception {
    String srcPath = "EMPTY_SOURCE_URL-0-hdfs://user321:321fake@foo.bar.com:8020/user/user321";
    String destPath = "EMPTY_DEST_URL-0-hdfs://user123:fake123@foo.bar.com:8020/user/user123";

    jobEntryHadoopCopyFiles.source_filefolder = new String[] { srcPath };
    jobEntryHadoopCopyFiles.destination_filefolder = new String[] { destPath };
    jobEntryHadoopCopyFiles.wildcard = new String[] { EMPTY };

    String xml = "<entry>" + jobEntryHadoopCopyFiles.getXML() + "</entry>"; // runs through all the loadURL and saveURL logic

    Document xmlDocument = getDocument( xml );

    // add new node
    String exampleSourceConfig0 = "<source_configuration_name>STATIC-SOURCE-FILE-0</source_configuration_name>";
    addSibling( xmlDocument, "EMPTY_SOURCE_URL-0", exampleSourceConfig0 );

    // save back changes
    xml = toString( xmlDocument );

    // TODO verify still correct test logic
    assertTrue( xml.contains( srcPath ) );
    assertTrue( xml.contains( destPath ) );
    JobEntryCopyFiles loadedentry = new JobEntryCopyFiles();
    InputStream is = new ByteArrayInputStream( xml.getBytes() );
    loadedentry.loadXML( XMLHandler.getSubNode(
        XMLHandler.loadXMLFile( is,
          null,
          false,
          false ),
        "entry" ),
      new ArrayList<DatabaseMeta>(),
      null,
      null,
      null );
    // NOTE: passwords should not be "scrubbed"
    assertTrue( loadedentry.source_filefolder[0].equals( srcPath ) );
    assertTrue( loadedentry.destination_filefolder[0].equals( destPath ) );
    verify( mockNamedClusterEmbedManager, times( 2 ) ).registerUrl( anyString() ); // might not be mocked correctly
  }

  protected void addNodeAfter( Document xmlDocument, String xmlNodeString ) {
    // TODO
  }

  protected static Document getDocument( String xmlSnippet ) throws ParserConfigurationException, IOException, SAXException {
    DocumentBuilderFactory builderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = builderFactory.newDocumentBuilder();
    String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
    String xmlString =/* XML_HEADER +*/ xmlSnippet;
    Document xmlDocument = builder.parse( new InputSource( new StringReader( xmlString ) ) );
    return xmlDocument; // TODO pentaho XMLHandler.loadXMLFile
  }

  public static String toString( Document document ) throws TransformerException {
    TransformerFactory transformerFactory = TransformerFactory.newInstance();
    Transformer transformer = transformerFactory.newTransformer();
    StringWriter stringWriter = new StringWriter();
    transformer.transform( new DOMSource( document ), new StreamResult( stringWriter ) );
    return stringWriter.toString();
  }

  protected static NodeList getFileFolderNodeList( Document document ) throws XPathExpressionException {
    String baesPath = "/entry/fields/field/";
    String sourceFF = baesPath + SOURCE_FILE_FOLDER;
    String destinationFF = baesPath + DESTINATION_FILE_FOLDER;
//    String expression = "/entry/fields/field/source_filefolder";
    String expression = sourceFF + " | " + destinationFF;
    XPath xPath = XPathFactory.newInstance().newXPath();
    NodeList nodeList = (NodeList) xPath.compile( expression ).evaluate( document, XPathConstants.NODESET );
    return nodeList;
  }

  protected static void addSibling( Document document, String fileFolderNodeSearchStartsWith, String xmlNewNode )
    throws XPathExpressionException, ParserConfigurationException, IOException, SAXException {
    NodeList fileFolders = getFileFolderNodeList( document );
    Node matchedNode = toStream( fileFolders )
        .filter( n -> n.getTextContent().startsWith( fileFolderNodeSearchStartsWith ) )
        .findFirst()
        .orElse( null );
    if ( matchedNode != null ) {
      appendChild( matchedNode.getParentNode(), createNode( xmlNewNode ) );
    }
  }

  protected static Stream<Node> toStream( NodeList nodeList ) {
    return IntStream.range( 0, nodeList.getLength() ) .mapToObj( nodeList::item );
  }

  protected static Element createNode( String xmlString ) throws ParserConfigurationException, IOException,
    SAXException {
    return DocumentBuilderFactory
      .newInstance()
      .newDocumentBuilder()
      .parse( new ByteArrayInputStream( xmlString.getBytes() ) )
      .getDocumentElement(); // TODO look at XMLHandler.loadXMLString(
  }

  protected static Node appendChild( Node node, Node child ) {
    /**
     * have to do this import since we don't have access to original Document builder
     * otherwise you'll get:
     * org.w3c.dom.DOMException: WRONG_DOCUMENT_ERR: A node is used in a different document than the one that created it
     */
    Document ownerDocument = node.getOwnerDocument();
    Node importedNode = ownerDocument.importNode( child, true );
    node.appendChild( importedNode );

    return importedNode;
  }
}
