package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

public interface ExternalDeadline extends GeneralDataEntity<ExternalDeadline> {

  void setDeadline(Long deadline);

  Long getDeadline();
}
