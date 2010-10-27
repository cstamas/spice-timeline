package org.sonatype.timeline;

import java.util.Iterator;

public abstract class TimelineResult
    implements Iterable<TimelineRecord>, Iterator<TimelineRecord>
{
    public static final TimelineResult EMPTY_RESULT = new TimelineResult()
    {
        @Override
        public TimelineRecord fetchNextRecord()
        {
            return null;
        }
    };

    // ==

    private TimelineRecord nextRecord;

    private boolean firstCall;

    public TimelineResult()
    {
        this.firstCall = true;
    }

    public Iterator<TimelineRecord> iterator()
    {
        return this;
    }

    protected void init()
    {
        nextRecord = fetchNextRecord();

        firstCall = false;
    }

    public boolean hasNext()
    {
        if ( firstCall )
        {
            init();
        }

        final boolean hasNext = nextRecord != null;

        if ( !hasNext )
        {
            // release as early as possible
            release();
        }

        return hasNext;
    }

    public TimelineRecord next()
    {
        if ( firstCall )
        {
            init();
        }

        TimelineRecord result = nextRecord;

        nextRecord = fetchNextRecord();

        // to triger release if needed as fast possible
        hasNext();

        return result;
    }

    public void remove()
    {
        throw new UnsupportedOperationException( "This operation is not supported on TimelineResult!" );
    }

    public void release()
    {
        // override and free something if needed
        // the implementation must allow multiple call!
    }

    @Override
    public void finalize()
    {
        release();
    }

    // ==

    protected abstract TimelineRecord fetchNextRecord();
}
