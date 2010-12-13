/**
 * Copyright (c) 2008 Sonatype, Inc. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package org.sonatype.timeline;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriter.MaxFieldLength;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;
import org.codehaus.plexus.component.annotations.Component;
import org.codehaus.plexus.component.annotations.Requirement;
import org.codehaus.plexus.logging.Logger;

@Component( role = TimelineIndexer.class )
public class DefaultTimelineIndexer
    implements TimelineIndexer
{
    private static final String TIMESTAMP = "_t";

    private static final String TYPE = "_1";

    private static final String SUBTYPE = "_2";

    private static final Resolution TIMELINE_RESOLUTION = Resolution.SECOND;

    @Requirement
    private Logger logger;

    private volatile boolean started = false;

    private Directory directory;

    private IndexWriter indexWriter;

    private final ReentrantReadWriteLock indexLock = new ReentrantReadWriteLock();

    protected Logger getLogger()
    {
        return logger;
    }

    // ==
    // Public API

    public void configure( TimelineConfiguration configuration )
        throws TimelineException
    {
        boolean newIndex = true;

        indexLock.writeLock().lock();

        try
        {
            started = false;

            closeIndexWriter();

            if ( directory != null )
            {
                directory.close();
            }

            directory = FSDirectory.open( configuration.getIndexDirectory() );

            if ( IndexReader.indexExists( directory ) )
            {
                if ( IndexWriter.isLocked( directory ) )
                {
                    IndexWriter.unlock( directory );
                }

                newIndex = false;
            }

            indexWriter =
                new IndexWriter( directory, new StandardAnalyzer( Version.LUCENE_30 ), newIndex, MaxFieldLength.LIMITED );

            indexWriter.setMergeScheduler( new SerialMergeScheduler() );

            closeIndexWriter();

            indexWriter =
                new IndexWriter( directory, new StandardAnalyzer( Version.LUCENE_30 ), false, MaxFieldLength.LIMITED );

            indexWriter.setRAMBufferSizeMB( 2 );

            indexWriter.setMergeScheduler( new SerialMergeScheduler() );

            started = true;
        }
        catch ( Exception e )
        {
            throw new TimelineException( "Failed to configure timeline index!", e );
        }
        finally
        {
            indexLock.writeLock().unlock();
        }
    }

    public void stop()
    {
        indexLock.writeLock().lock();

        try
        {
            closeIndexWriter();

            if ( directory != null )
            {
                directory.close();
            }
        }
        catch ( IOException e )
        {
            getLogger().warn( "Unable to cleanly stop TimelineIndexer", e );
        }
        finally
        {
            started = false;

            indexLock.writeLock().unlock();
        }
    }

    public boolean isStarted()
    {
        return started;
    }

    public void add( TimelineRecord record )
        throws TimelineException
    {
        IndexWriter writer = null;

        indexLock.readLock().lock();

        try
        {
            writer = getIndexWriter();

            writer.addDocument( createDocument( record ) );

            writer.commit();
        }
        catch ( Exception e )
        {
            throw new TimelineException( "Fail to add a record to the timeline index", e );
        }
        finally
        {
            indexLock.readLock().unlock();
        }
    }

    public void addAll( TimelineResult res )
        throws TimelineException
    {
        IndexWriter writer = null;

        indexLock.readLock().lock();

        try
        {
            writer = getIndexWriter();

            for ( TimelineRecord rec : res )
            {
                writer.addDocument( createDocument( rec ) );
            }

            writer.commit();
        }
        catch ( Exception e )
        {
            throw new TimelineException( "Fail to add a record to the timeline index", e );
        }
        finally
        {
            indexLock.readLock().unlock();
        }
    }

    public TimelineResult retrieve( long fromTime, long toTime, Set<String> types, Set<String> subTypes, int from,
                                    int count, TimelineFilter filter )
        throws TimelineException
    {
        IndexSearcher searcher = null;

        boolean cleanup = true;

        indexLock.readLock().lock();

        try
        {
            searcher = getIndexSearcher();

            if ( searcher.maxDoc() == 0 )
            {
                return TimelineResult.EMPTY_RESULT;
            }

            TopFieldDocs topDocs =
                searcher.search( buildQuery( fromTime, toTime, types, subTypes ), null, searcher.maxDoc(), new Sort(
                    new SortField( TIMESTAMP, SortField.LONG, true ) ) );

            if ( topDocs.scoreDocs.length == 0 )
            {
                return TimelineResult.EMPTY_RESULT;
            }

            cleanup = false;

            return new IndexerTimelineResult( searcher, topDocs, filter, from, count, indexLock.readLock() );
        }
        catch ( Exception e )
        {
            throw new TimelineException( "Failed to retrieve records from the timeline index!", e );
        }
        finally
        {
            if ( cleanup )
            {
                indexLock.readLock().unlock();

                if ( searcher != null )
                {
                    try
                    {
                        searcher.close();
                    }
                    catch ( IOException e )
                    {
                        // failure closing
                        getLogger().error( "Unable to close searcher", e );
                    }
                }
            }
        }
    }

    public int purge( long fromTime, long toTime, Set<String> types, Set<String> subTypes )
        throws TimelineException
    {
        IndexSearcher searcher = null;

        indexLock.readLock().lock();

        try
        {
            searcher = getIndexSearcher();

            if ( searcher.maxDoc() == 0 )
            {
                return 0;
            }

            Query q = buildQuery( fromTime, toTime, types, subTypes );

            // just to know how many will we delete, will not actually load 'em up
            TopFieldDocs topDocs =
                searcher.search( q, null, searcher.maxDoc(),
                    new Sort( new SortField( TIMESTAMP, SortField.LONG, true ) ) );

            if ( topDocs.scoreDocs.length == 0 )
            {
                return 0;
            }

            IndexWriter writer = getIndexWriter();

            writer.deleteDocuments( q );

            writer.commit();

            writer.optimize();

            return topDocs.scoreDocs.length;
        }
        catch ( IOException e )
        {
            throw new TimelineException( "Failed to purge records from the timeline index!", e );
        }
        finally
        {
            indexLock.readLock().unlock();

            if ( searcher != null )
            {
                try
                {
                    searcher.close();
                }
                catch ( IOException e )
                {
                    getLogger().error( "Unable to close searcher", e );
                }
            }
        }
    }

    // ==

    protected void closeIndexWriter()
        throws IOException
    {
        indexLock.writeLock().lock();

        try
        {
            if ( indexWriter != null )
            {
                indexWriter.commit();

                indexWriter.close();

                indexWriter = null;
            }
        }
        finally
        {
            indexLock.writeLock().unlock();
        }
    }

    protected IndexWriter getIndexWriter()
        throws IOException
    {
        return indexWriter;
    }

    protected IndexSearcher getIndexSearcher()
        throws IOException
    {
        return new IndexSearcher( directory );
    }

    protected Document createDocument( TimelineRecord record )
    {
        Document doc = new Document();

        doc.add( new Field( TIMESTAMP, DateTools.timeToString( record.getTimestamp(), TIMELINE_RESOLUTION ),
            Field.Store.YES, Field.Index.NOT_ANALYZED ) );

        doc.add( new Field( TYPE, record.getType(), Field.Store.YES, Field.Index.NOT_ANALYZED ) );

        doc.add( new Field( SUBTYPE, record.getSubType(), Field.Store.YES, Field.Index.NOT_ANALYZED ) );

        for ( String key : record.getData().keySet() )
        {
            doc.add( new Field( key, record.getData().get( key ), Field.Store.YES, Field.Index.ANALYZED ) );
        }

        return doc;
    }

    protected Query buildQuery( long from, long to, Set<String> types, Set<String> subTypes )
    {
        if ( isEmptySet( types ) && isEmptySet( subTypes ) )
        {
            return new TermRangeQuery( TIMESTAMP, DateTools.timeToString( from, TIMELINE_RESOLUTION ),
                DateTools.timeToString( to, TIMELINE_RESOLUTION ), true, true );
        }
        else
        {
            BooleanQuery result = new BooleanQuery();

            result.add(
                new TermRangeQuery( TIMESTAMP, DateTools.timeToString( from, TIMELINE_RESOLUTION ),
                    DateTools.timeToString( to, TIMELINE_RESOLUTION ), true, true ), Occur.MUST );

            if ( !isEmptySet( types ) )
            {
                BooleanQuery typeQ = new BooleanQuery();

                for ( String type : types )
                {
                    typeQ.add( new TermQuery( new Term( TYPE, type ) ), Occur.SHOULD );
                }

                result.add( typeQ, Occur.MUST );
            }
            if ( !isEmptySet( subTypes ) )
            {
                BooleanQuery subTypeQ = new BooleanQuery();

                for ( String subType : subTypes )
                {
                    subTypeQ.add( new TermQuery( new Term( SUBTYPE, subType ) ), Occur.SHOULD );
                }

                result.add( subTypeQ, Occur.MUST );
            }

            return result;
        }
    }

    protected boolean isEmptySet( Set<String> set )
    {
        return set == null || set.size() == 0;
    }

    public static class IndexerTimelineResult
        extends TimelineResult
    {
        private IndexSearcher searcher;

        private final TopFieldDocs hits;

        private final TimelineFilter filter;

        private final int docNumberToReturn;

        private final Lock readLock;

        private int docNumberToSkip;

        private int i;

        private int returned;

        public IndexerTimelineResult( final IndexSearcher searcher, final TopFieldDocs hits,
                                      final TimelineFilter filter, final int docNumberToSkip,
                                      final int docNumberToReturn, final Lock readLock )
        {
            this.searcher = searcher;

            this.hits = hits;

            this.filter = filter;

            this.docNumberToSkip = docNumberToSkip;

            this.docNumberToReturn = docNumberToReturn;

            this.readLock = readLock;

            this.i = 0;

            this.returned = 0;
        }

        // for testing
        protected int getLength()
        {
            return hits.scoreDocs.length;
        }

        @Override
        protected TimelineRecord fetchNextRecord()
        {
            Document doc;

            TimelineRecord data;

            while ( true )
            {
                if ( i >= hits.scoreDocs.length || returned >= docNumberToReturn )
                {
                    return null;
                }

                try
                {
                    doc = searcher.doc( hits.scoreDocs[i++].doc );

                    data = buildData( doc );

                    if ( filter != null && !filter.accept( data ) )
                    {
                        data = null;

                        continue;
                    }

                    // skip the unneeded stuff
                    // Warning: this means we skip the needed FILTERED stuff out!
                    if ( docNumberToSkip > 0 )
                    {
                        docNumberToSkip--;

                        continue;
                    }

                    returned++;

                    return data;
                }
                catch ( IOException e )
                {
                    return null;
                }
            }
        }

        protected TimelineRecord buildData( Document doc )
        {
            long timestamp = -1;

            String tsString = doc.get( TIMESTAMP );

            if ( tsString != null )
            {
                // legacy indexes will have nulls here
                try
                {
                    timestamp = DateTools.stringToTime( tsString );
                }
                catch ( ParseException e )
                {
                    // leave it -1
                }
            }

            // legacy indexes will have nulls here
            String type = doc.get( TYPE );

            // legacy indexes will have nulls here
            String subType = doc.get( SUBTYPE );

            Map<String, String> data = new HashMap<String, String>();

            TimelineRecord result = new TimelineRecord( timestamp, type, subType, data );

            for ( Fieldable field : doc.getFields() )
            {
                if ( !field.name().startsWith( "_" ) )
                {
                    data.put( field.name(), field.stringValue() );
                }
            }

            return result;
        }

        @Override
        public void release()
        {
            super.release();

            // do this only once
            if ( searcher != null )
            {
                try
                {
                    searcher.close();
                }
                catch ( IOException e )
                {
                    // muted
                }
                finally
                {
                    readLock.unlock();
                }

                searcher = null;
            }
        }
    }

}
