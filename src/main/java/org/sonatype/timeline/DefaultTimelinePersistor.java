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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.codehaus.plexus.component.annotations.Component;
import org.codehaus.plexus.component.annotations.Requirement;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.util.IOUtil;
import org.sonatype.timeline.proto.TimeLineRecordProtos;

/**
 * The class doing persitence of timeline records using Protobuf.
 * 
 * @author juven
 * @author cstamas
 */
@Component( role = TimelinePersistor.class )
public class DefaultTimelinePersistor
    implements TimelinePersistor
{
    @Deprecated
    private static final String V1_DATA_FILE_NAME_DATE_FORMAT = "yyyy-MM-dd.HH-mm-ss";

    @Deprecated
    private static final Pattern V1_DATA_FILE_NAME_PATTERN =
        Pattern.compile( "^timeline\\.(\\d{4}-\\d{2}-\\d{2}\\.\\d{2}-\\d{2}-\\d{2})\\.dat$" );

    // ==

    private static final String V2_DATA_FILE_NAME_PREFIX = "timeline.";

    private static final String V2_DATA_FILE_NAME_SUFFIX = ".dat";

    private static final String V2_DATA_FILE_NAME_DATE_FORMAT = "yyyy-MM-dd.HH-mm-ssZ";

    private static final Pattern V2_DATA_FILE_NAME_PATTERN = Pattern.compile( "^"
        + V2_DATA_FILE_NAME_PREFIX.replace( ".", "\\." ) + "(\\d{4}-\\d{2}-\\d{2}\\.\\d{2}-\\d{2}-\\d{2}[+-]\\d{4})"
        + V2_DATA_FILE_NAME_SUFFIX.replace( ".", "\\." ) + "$" );

    @Requirement
    private Logger logger;

    private int rollingInterval;

    private File persistDirectory;

    private long lastRolledTimestamp = 0L;

    private File lastRolledFile;

    private final ReentrantReadWriteLock dataLock = new ReentrantReadWriteLock();

    protected Logger getLogger()
    {
        return logger;
    }

    // ==
    // Public API

    public void configure( TimelineConfiguration config )
    {
        dataLock.writeLock().lock();

        try
        {
            this.persistDirectory = config.getPersistDirectory();

            if ( !this.persistDirectory.exists() )
            {
                this.persistDirectory.mkdirs();
            }

            this.rollingInterval = config.getPersistRollingInterval();
        }
        finally
        {
            dataLock.writeLock().unlock();
        }
    }

    public void persist( TimelineRecord record )
        throws TimelineException
    {
        verify( record );

        OutputStream out = null;

        dataLock.writeLock().lock();

        try
        {
            out = new FileOutputStream( getDataFile(), true );

            byte[] bytes = toProto( record ).toByteArray();

            out.write( bytes.length );

            out.write( bytes );

            out.flush();
        }
        catch ( IOException e )
        {
            throw new TimelineException( "Failed to persist timeline record to data file!", e );
        }
        finally
        {
            IOUtil.close( out );

            dataLock.writeLock().unlock();
        }
    }

    public TimelineResult readAll()
        throws TimelineException
    {
        return readAllSinceDays( Integer.MAX_VALUE );
    }

    public TimelineResult readAllSinceDays( int days )
        throws TimelineException
    {
        // read data files
        File[] files = persistDirectory.listFiles( new FilenameFilter()
        {
            public boolean accept( File dir, String fname )
            {
                return V2_DATA_FILE_NAME_PATTERN.matcher( fname ).matches()
                    || V1_DATA_FILE_NAME_PATTERN.matcher( fname ).matches();
            }
        } );

        // do we have any?
        if ( files == null || files.length == 0 )
        {
            return TimelineResult.EMPTY_RESULT;
        }

        // sort it, youngest goes 1st
        Arrays.sort( files, new Comparator<File>()
        {
            public int compare( File f1, File f2 )
            {
                final long f1ts = getTimestampedFileNameTimestamp( f1 );
                final long f2ts = getTimestampedFileNameTimestamp( f2 );

                // "reverse" the sort, we need newest-first
                final long result = -( f1ts - f2ts );

                if ( result < 0 )
                {
                    return -1;
                }
                else if ( result > 0 )
                {
                    return 1;
                }
                else
                {
                    return 0;
                }
            }
        } );

        // get the "last applicable" file time stamp if needed: it is the youngest (1st) minus (going into past) as many
        // days as wanted.
        final long oldestFileTimestampThreshold =
            Integer.MAX_VALUE == days ? 0
                : ( getTimestampedFileNameTimestamp( files[0] ) - ( days * 24L * 60L * 60L * 1000L ) );

        // "cut"/filter the files
        ArrayList<File> result = new ArrayList<File>();

        for ( File file : files )
        {
            if ( oldestFileTimestampThreshold <= getTimestampedFileNameTimestamp( file ) )
            {
                result.add( file );
            }
            else
            {
                // we have sorted array, so we can bail out, we know that older files will come only
                break;
            }

        }

        return new PersistorTimelineResult( result.toArray( new File[result.size()] ), dataLock.readLock() );
    }

    // ==

    protected static class PersistorTimelineResult
        extends TimelineResult
    {
        private final File[] fromFiles;

        private final Lock readLock;

        private int filePtr;

        private Iterator<TimelineRecord> currentIterator;

        public PersistorTimelineResult( final File[] files, final Lock readLock )
        {
            this.fromFiles = files;

            this.readLock = readLock;

            this.filePtr = 0;

            this.currentIterator = null;
        }

        @Override
        protected TimelineRecord fetchNextRecord()
        {
            if ( currentIterator != null && currentIterator.hasNext() )
            {
                return currentIterator.next();
            }
            else if ( filePtr >= fromFiles.length )
            {
                // no more
                return null;
            }
            else
            {
                currentIterator = readFile( fromFiles[filePtr] );

                filePtr++;

                return fetchNextRecord();
            }
        }

        /**
         * Reads a whole file into memory, and in case of any problem, it returns an empty collection, making this file
         * to be skipped.
         * 
         * @param file
         * @return
         */
        protected Iterator<TimelineRecord> readFile( File file )
        {
            ArrayList<TimelineRecord> result = new ArrayList<TimelineRecord>();

            InputStream in = null;

            readLock.lock();

            try
            {
                in = new FileInputStream( file );

                while ( in.available() > 0 )
                {
                    int length = in.read();

                    byte[] bytes = new byte[length];

                    in.read( bytes, 0, length );

                    result.add( fromProto( TimeLineRecordProtos.TimeLineRecord.parseFrom( bytes ) ) );
                }
            }
            catch ( Exception e )
            {
                // just ignore it
            }
            finally
            {
                if ( in != null )
                {
                    try
                    {
                        in.close();
                    }
                    catch ( IOException e )
                    {
                    }
                }

                readLock.unlock();
            }

            return result.iterator();
        }

        protected TimelineRecord fromProto( TimeLineRecordProtos.TimeLineRecord rec )
        {
            Map<String, String> dataMap = new HashMap<String, String>();

            for ( TimeLineRecordProtos.TimeLineRecord.Data data : rec.getDataList() )
            {
                dataMap.put( data.getKey(), data.getValue() );
            }

            return new TimelineRecord( rec.getTimestamp(), rec.getType(), rec.getSubType(), dataMap );
        }
    }

    // ==

    protected File getDataFile()
        throws IOException
    {
        long now = System.currentTimeMillis();

        if ( lastRolledTimestamp == 0L || ( now - lastRolledTimestamp ) > ( rollingInterval * 1000 ) )
        {
            lastRolledTimestamp = now;

            lastRolledFile = new File( persistDirectory, buildTimestampedFileName() );

            lastRolledFile.createNewFile();
        }

        return lastRolledFile;
    }

    protected String buildTimestampedFileName()
    {
        final SimpleDateFormat dateFormat = new SimpleDateFormat( V2_DATA_FILE_NAME_DATE_FORMAT );

        StringBuilder fileName = new StringBuilder();

        fileName.append( V2_DATA_FILE_NAME_PREFIX ).append( dateFormat.format( new Date( System.currentTimeMillis() ) ) ).append(
            V2_DATA_FILE_NAME_SUFFIX );

        return fileName.toString();
    }

    protected long getTimestampedFileNameTimestamp( File file )
    {
        final Matcher fnMatcher = V2_DATA_FILE_NAME_PATTERN.matcher( file.getName() );

        if ( fnMatcher.matches() )
        {
            final String datePattern = fnMatcher.group( 1 );

            try
            {
                return new SimpleDateFormat( V2_DATA_FILE_NAME_DATE_FORMAT ).parse( datePattern ).getTime();
            }
            catch ( ParseException e )
            {
                // silently go to next try
            }
        }

        final Matcher oldFnMatcher = V1_DATA_FILE_NAME_PATTERN.matcher( file.getName() );

        if ( oldFnMatcher.matches() )
        {
            final String datePattern = oldFnMatcher.group( 1 );

            try
            {
                return new SimpleDateFormat( V1_DATA_FILE_NAME_DATE_FORMAT ).parse( datePattern ).getTime();
            }
            catch ( ParseException e )
            {
                // silently go to next try
            }
        }

        // fallback to lastModified
        return file.lastModified();
    }

    protected TimeLineRecordProtos.TimeLineRecord toProto( TimelineRecord record )
    {
        TimeLineRecordProtos.TimeLineRecord.Builder builder = TimeLineRecordProtos.TimeLineRecord.newBuilder();

        builder.setTimestamp( record.getTimestamp() );

        builder.setType( record.getType() );

        builder.setSubType( record.getSubType() );

        for ( Map.Entry<String, String> entry : record.getData().entrySet() )
        {
            builder.addData( TimeLineRecordProtos.TimeLineRecord.Data.newBuilder().setKey( entry.getKey() ).setValue(
                entry.getValue() ).build() );
        }

        return builder.build();
    }

    protected void verify( TimelineRecord record )
        throws TimelineException
    {
        Map<String, String> data = record.getData();

        if ( data == null )
        {
            return;
        }

        for ( Map.Entry<String, String> entry : data.entrySet() )
        {
            if ( entry.getKey() == null )
            {
                throw new TimelineException( "Timeline record contains invalid data: key is null." );
            }
            if ( entry.getValue() == null )
            {
                throw new TimelineException( "Timeline record contains invalid data: value is null." );
            }
        }
    }
}
