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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.codehaus.plexus.component.annotations.Component;
import org.codehaus.plexus.component.annotations.Requirement;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.util.FileUtils;

@Component( role = Timeline.class )
public class DefaultTimeline
    implements Timeline
{
    @Requirement
    private Logger logger;

    @Requirement
    private TimelinePersistor persistor;

    @Requirement
    private TimelineIndexer indexer;

    private TimelineConfiguration configuration;

    private ReentrantLock repairLock = new ReentrantLock();

    private volatile boolean started = false;

    protected Logger getLogger()
    {
        return logger;
    }

    protected boolean isIndexerHealthy()
    {
        // we have no ongoing repair and indexer is started
        return ( !repairLock.isLocked() ) && indexer.isStarted();
    }

    public synchronized void configure( TimelineConfiguration config )
        throws TimelineException
    {
        this.configuration = config;

        persistor.configure( config );

        try
        {
            indexer.configure( config );
        }
        catch ( TimelineException e )
        {
            repairTimelineIndexer( e );
        }

        this.started = true;
    }

    public void stop()
    {
        indexer.stop();

        this.started = false;
    }

    private void repairTimelineIndexer( Exception e )
        throws TimelineException
    {
        if ( repairLock.tryLock() )
        {
            try
            {
                getLogger().info( "Timeline index got corrupted, trying to repair it.", e );

                indexer.stop();

                try
                {
                    FileUtils.cleanDirectory( configuration.getIndexDirectory() );
                }
                catch ( IOException ex )
                {
                    throw new TimelineException( "Failed to clean index and repair timeline, directory: "
                        + configuration.getIndexDirectory().getAbsolutePath(), ex );
                }

                try
                {
                    indexer.configure( configuration );

                    indexer.addAll( persistor.readAllSinceDays( configuration.getRepairDaysCountRestored() ) );

                    if ( indexer.isStarted() )
                    {
                        getLogger().info(
                            "Timeline index is succesfully repaired, the last "
                                + configuration.getRepairDaysCountRestored() + " days were restored." );
                    }
                    else
                    {
                        getLogger().warn( "Timeline index was corrupted and repair of it failed!" );
                    }
                }
                catch ( Exception ex )
                {
                    // we need to stop it
                    indexer.stop();

                    if ( ex instanceof TimelineException )
                    {
                        throw (TimelineException) ex;
                    }
                    else
                    {
                        throw new TimelineException( "Unable to repair the Timeline indexer!", ex );
                    }
                }
            }
            finally
            {
                repairLock.unlock();
            }
        }
    }

    public void add( long timestamp, String type, String subType, Map<String, String> data )
    {
        if ( !started )
        {
            return;
        }

        TimelineRecord record = new TimelineRecord( timestamp, type, subType, data );

        try
        {
            persistor.persist( record );

            if ( isIndexerHealthy() )
            {
                try
                {
                    indexer.add( record );
                }
                catch ( TimelineException e )
                {
                    repairTimelineIndexer( e );

                    // now try add again
                    indexer.add( record );
                }
            }
        }
        catch ( TimelineException e )
        {
            getLogger().warn( "Failed to add a timeline record", e );
        }
    }

    public void add( String type, String subType, Map<String, String> data )
    {
        add( System.currentTimeMillis(), type, subType, data );
    }

    public void addAll( String type, String subType, Collection<Map<String, String>> datas )
    {
        addAll( System.currentTimeMillis(), type, subType, datas );
    }

    public void addAll( long timestamp, String type, String subType, Collection<Map<String, String>> datas )
    {
        for ( Map<String, String> data : datas )
        {
            add( timestamp, type, subType, data );
        }
    }

    private int purge( long fromTime, long toTime, Set<String> types, Set<String> subTypes )
    {
        try
        {
            if ( isIndexerHealthy() )
            {
                try
                {
                    return indexer.purge( fromTime, toTime, types, subTypes );
                }
                catch ( TimelineException e )
                {
                    repairTimelineIndexer( e );

                    // now try purge again
                    return indexer.purge( fromTime, toTime, types, subTypes );
                }
            }
            else
            {
                return 0;
            }
        }
        catch ( TimelineException e )
        {
            getLogger().warn( "Failed to purge timeline!", e );

            return 0;
        }
    }

    public int purgeAll()
    {
        return purgeAll( null );
    }

    public int purgeAll( Set<String> types )
    {
        return purgeAll( types, null, null );
    }

    /**
     * Note that currently the filter is not used
     */
    public int purgeAll( Set<String> types, Set<String> subTypes, TimelineFilter filter )
    {
        return purge( 0L, System.currentTimeMillis(), types, subTypes );
    }

    public int purgeOlderThan( long timestamp )
    {
        return purgeOlderThan( timestamp, null );
    }

    public int purgeOlderThan( long timestamp, Set<String> types )
    {
        return purgeOlderThan( timestamp, types, null, null );
    }

    /**
     * Note that currently the filter is not used
     */
    public int purgeOlderThan( long timestamp, Set<String> types, Set<String> subTypes, TimelineFilter filter )
    {
        return purge( 0L, timestamp, types, subTypes );
    }

    public TimelineResult retrieve( long fromTs, int count, Set<String> types )
    {
        return retrieve( fromTs, count, types, null, null );
    }

    public TimelineResult retrieve( long fromTs, int count, Set<String> types, Set<String> subTypes,
                                    TimelineFilter filter )
    {
        return retrieve( fromTs, System.currentTimeMillis(), types, subTypes, 0, count, filter );
    }

    public TimelineResult retrieve( int fromItem, int count, Set<String> types )
    {
        return retrieve( fromItem, count, types, null, null );
    }

    public TimelineResult retrieve( int fromItem, int count, Set<String> types, Set<String> subTypes,
                                    TimelineFilter filter )
    {
        return retrieve( 0L, System.currentTimeMillis(), types, subTypes, fromItem, count, filter );
    }

    private TimelineResult retrieve( long fromTime, long toTime, Set<String> types, Set<String> subTypes, int from,
                                     int count, TimelineFilter filter )
    {
        if ( !started )
        {
            return TimelineResult.EMPTY_RESULT;
        }

        try
        {
            if ( isIndexerHealthy() )
            {
                try
                {
                    return indexer.retrieve( fromTime, toTime, types, subTypes, from, count, filter );
                }
                catch ( TimelineException e )
                {
                    repairTimelineIndexer( e );

                    // now try retrieve again
                    return indexer.retrieve( fromTime, toTime, types, subTypes, from, count, filter );
                }
            }
            else
            {
                return TimelineResult.EMPTY_RESULT;
            }
        }
        catch ( TimelineException e )
        {
            getLogger().warn( "Unable to retrieve data from timeline!", e );

            return TimelineResult.EMPTY_RESULT;
        }
    }

    public TimelineResult retrieveNewest( int count, Set<String> types )
    {
        return retrieveNewest( count, types, null, null );
    }

    public TimelineResult retrieveNewest( int count, Set<String> types, Set<String> subTypes, TimelineFilter filter )
    {
        return retrieve( 0L, System.currentTimeMillis(), types, subTypes, 0, count, filter );
    }

}
