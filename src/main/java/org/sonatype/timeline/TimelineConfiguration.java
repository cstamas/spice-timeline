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

public class TimelineConfiguration
{
    private File persistDirectory;

    private File indexDirectory;

    private int persistRollingInterval;

    private int repairDaysCountRestored;

    public TimelineConfiguration( File persistDirectory, File indexDirectory, int persistRollingInterval,
                                  int repairDaysCountRestored )
    {
        this.persistDirectory = persistDirectory;

        this.indexDirectory = indexDirectory;

        this.persistRollingInterval = persistRollingInterval;

        this.repairDaysCountRestored = repairDaysCountRestored;
    }

    public TimelineConfiguration( File persistDirectory, File indexDirectory, int persistRollingInterval )
    {
        this( persistDirectory, indexDirectory, persistRollingInterval, Timeline.DEFAULT_REPAIR_DAYS_TO_RESTORE );
    }

    public TimelineConfiguration( File persistDirectory, File indexDirectory )
    {
        this( persistDirectory, indexDirectory, TimelinePersistor.DEFAULT_ROLLING_INTERVAL );
    }

    public TimelineConfiguration( File baseDir )
    {
        this( new File( baseDir, "persist" ), new File( baseDir, "index" ), TimelinePersistor.DEFAULT_ROLLING_INTERVAL );
    }

    public File getPersistDirectory()
    {
        return persistDirectory;
    }

    public void setPersistDirectory( File persistDirectory )
    {
        this.persistDirectory = persistDirectory;
    }

    public File getIndexDirectory()
    {
        return indexDirectory;
    }

    public void setIndexDirectory( File indexDirectory )
    {
        this.indexDirectory = indexDirectory;
    }

    public int getPersistRollingInterval()
    {
        return persistRollingInterval;
    }

    public void setPersistRollingInterval( int persistRollingInterval )
    {
        this.persistRollingInterval = persistRollingInterval;
    }

    public int getRepairDaysCountRestored()
    {
        return repairDaysCountRestored;
    }

    public void setRepairDaysCountRestored( int repairDaysCountRestored )
    {
        this.repairDaysCountRestored = repairDaysCountRestored;
    }
}
