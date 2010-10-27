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

/**
 * Persist the TimelineRecord
 * 
 * @author juven
 * @author cstamas
 */
public interface TimelinePersistor
{
    /**
     * Default rolling interval is one day.
     */
    public static final int DEFAULT_ROLLING_INTERVAL = 60 * 60 * 24;

    void configure( TimelineConfiguration config )
        throws TimelineException;

    /**
     * Writes a record to persistent storage.
     * 
     * @param record
     * @throws TimelineException
     */
    void persist( TimelineRecord record )
        throws TimelineException;

    /**
     * Reads all records. Worning: this may be huuge on long running instances.
     * 
     * @return
     * @throws TimelineException
     */
    TimelineResult readAll()
        throws TimelineException;

    /**
     * Reads last records corresponding to last days. The days calculation is relative, counted since most recent
     * output.
     * 
     * @param days
     * @return
     * @throws TimelineException
     */
    TimelineResult readAllSinceDays( int days )
        throws TimelineException;
}
