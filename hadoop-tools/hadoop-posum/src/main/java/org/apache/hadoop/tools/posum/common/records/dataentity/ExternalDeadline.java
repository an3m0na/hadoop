package org.apache.hadoop.tools.posum.common.records.dataentity;

public interface ExternalDeadline extends GeneralDataEntity<ExternalDeadline> {

  void setDeadline(Long deadline);

  Long getDeadline();
}
