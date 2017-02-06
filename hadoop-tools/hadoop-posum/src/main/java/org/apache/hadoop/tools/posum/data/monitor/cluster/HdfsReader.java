package org.apache.hadoop.tools.posum.data.monitor.cluster;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.hadoop.mapreduce.split.JobSplit;
import org.apache.hadoop.mapreduce.split.SplitMetaInfoReader;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobConfProxy;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

class HdfsReader {
    private static Log logger = LogFactory.getLog(JobInfoCollector.class);

    private Configuration conf;
    private FileSystem fileSystem;

    HdfsReader(Configuration conf) throws IOException {
        this.conf = conf;
        this.fileSystem = FileSystem.get(conf);
    }

    JobConfProxy getSubmittedConf(JobId jobId, String user) throws IOException {
        Path confPath = MRApps.getStagingAreaDir(conf, user != null ? user : UserGroupInformation.getCurrentUser().getUserName());
        confPath = fileSystem.makeQualified(confPath);

        //DANGER We assume there can only be one job / application
        Path jobConfDir = new Path(confPath, jobId.toString());
        logger.debug("Checking file path for conf: " + jobConfDir);
        Path jobConfPath = new Path(jobConfDir, "job.xml");
        Configuration jobConf = new JobConf(false);
        jobConf.addResource(fileSystem.open(jobConfPath), jobConfPath.toString());
        JobConfProxy proxy = Records.newRecord(JobConfProxy.class);
        proxy.setId(jobId.toString());
        proxy.setConfPath(jobConfDir.toUri().toString());
        proxy.setConf(jobConf);
        return proxy;
    }

    JobSplit.TaskSplitMetaInfo[] getSplitMetaInfo(JobId jobId,
                                                  JobConfProxy jobConfProxy) throws IOException {
        try {
            Path jobSubmitDir = new Path(new URI(jobConfProxy.getConfPath()));
            return SplitMetaInfoReader.readSplitMetaInfo(
                    TypeConverter.fromYarn(jobId),
                    fileSystem,
                    jobConfProxy.getConf(),
                    jobSubmitDir
            );
        } catch (URISyntaxException e) {
            throw new PosumException("Invalid jobConfDir path " + jobConfProxy.getConfPath(), e);
        }
    }

}
