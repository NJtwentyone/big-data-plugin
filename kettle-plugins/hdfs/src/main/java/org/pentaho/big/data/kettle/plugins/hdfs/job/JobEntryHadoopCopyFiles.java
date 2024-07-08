/*******************************************************************************
 *
 * Pentaho Big Data
 *
 * Copyright (C) 2002-2019 by Hitachi Vantara : http://www.pentaho.com
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

import com.google.common.annotations.VisibleForTesting;
import org.pentaho.di.ui.repository.repositoryexplorer.model.UIRepositoryContent;
import org.pentaho.hadoop.shim.api.cluster.NamedClusterService;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.JobEntry;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.job.entries.copyfiles.JobEntryCopyFiles;
import org.pentaho.hadoop.shim.api.cluster.NamedCluster;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.runtime.test.RuntimeTester;
import org.pentaho.runtime.test.action.RuntimeTestActionService;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

@JobEntry( id = "HadoopCopyFilesPlugin", image = "HDM.svg", name = "HadoopCopyFilesPlugin.Name",
  description = "HadoopCopyFilesPlugin.Description",
  categoryDescription = "i18n:org.pentaho.di.job:JobCategory.Category.BigData",
  i18nPackageName = "org.pentaho.di.job.entries.hadoopcopyfiles",
  documentationUrl = "mk-95pdia003/pdi-job-entries/hadoop-copy-files" )
public class JobEntryHadoopCopyFiles extends JobEntryCopyFiles {

  public static final String S3_SOURCE_FILE = "S3-SOURCE-FILE-";
  public static final String S3_DEST_FILE = "S3-DEST-FILE-";
  private final NamedClusterService namedClusterService;
  private final RuntimeTestActionService runtimeTestActionService;
  private final RuntimeTester runtimeTester;

  protected Map<String, String> reverseUrlLookup = new HashMap<>();

  public JobEntryHadoopCopyFiles( NamedClusterService namedClusterService,
                                  RuntimeTestActionService runtimeTestActionService, RuntimeTester runtimeTester ) {
    this.namedClusterService = namedClusterService;
    this.runtimeTestActionService = runtimeTestActionService;
    this.runtimeTester = runtimeTester;
  }

  @Override
  public String loadURL( String url, String ncName, IMetaStore metastore, Map mappings ) {
    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    String origUrl;
    String pref = null;
    String urlPreProcessURLsubstitution = null;
    if ( url != null && url.indexOf( SOURCE_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
    } else if ( url != null && url.indexOf( DEST_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
    }
    if ( c != null ) {
      urlPreProcessURLsubstitution = url;
      url = c.processURLsubstitution( url, metastore, getVariables() );
    }
    if ( pref != null ) {
      url = pref + url; // after #processURLsubstitution
      reverseUrlLookup.put( url, urlPreProcessURLsubstitution );
    }
    if ( !Const.isEmpty( ncName ) && !Const.isEmpty( url ) ) {
      mappings.put( url, ncName );
    }
    return url;
  }

  @Override
  public String saveURL( String url, String ncName, IMetaStore metastore, Map<String, String> mappings ) {
//    NamedCluster c = namedClusterService.getNamedClusterByName( ncName, metastore );
    String origUrl = null;
    String pref = null;
    if ( url != null && url.indexOf( SOURCE_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( SOURCE_URL ) + SOURCE_URL.length() ) + 1 );
    } else if ( url != null && url.indexOf( DEST_URL ) > -1 ) {
      origUrl = url;
      url = origUrl.substring( origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
      pref = origUrl.substring( 0, origUrl.indexOf( "-", origUrl.indexOf( DEST_URL ) + DEST_URL.length() ) + 1 );
    }
    /**
     * what is "prependCluster" logic in #processURLsubstitution
     * <code>
     *   url = c.processURLsubstitution( url, metastore, getVariables() );
     * </code>
     *
     * source class: NamedClusterImpl.java#processURLsubstitution(
     *
     * can we assume the url part is fully resolved!? and valid java.net.URL or parseable by Apache VFS's UrlFileNameParser
     */

    // TODO only execute #getPath if NamedCluster is not null
    String path = null;
    try {
      path = new URI( url ).getPath();
    } catch ( URISyntaxException use ) {
      // do nothing
    }

    return ( path != null ) ? pref + path : origUrl; // TODO other edge cases ie origUrl is null fails to parse

  }

  @VisibleForTesting
  @Override protected VariableSpace getVariables() {
    return super.getVariables();
  }

  public NamedClusterService getNamedClusterService() {
    return namedClusterService;
  }

  public RuntimeTestActionService getRuntimeTestActionService() {
    return runtimeTestActionService;
  }

  public RuntimeTester getRuntimeTester() {
    return runtimeTester;
  }
}
